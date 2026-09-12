package ferricstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"reflect"
	"testing"
)

func TestCompactPubSubPublishPipelineWireShape(t *testing.T) {
	commands := [][]any{
		{"PUBLISH", "channel-a", "one"},
		{"publish", []byte("channel-b"), []byte("two")},
	}
	payload, ok, err := compactPipelinePayloadWithLimit(commands, nativeMaxFrameBytes)
	if err != nil || !ok {
		t.Fatalf("compact PUBLISH pipeline = ok %t, error %v", ok, err)
	}

	want := []byte{nativeCompactPipelineRequest, 0x80 | 35, 0, 0, 0, 2}
	want = appendCompactBinary(want, []byte("channel-a"))
	want = appendCompactBinary(want, []byte("one"))
	want = appendCompactBinary(want, []byte("channel-b"))
	want = appendCompactBinary(want, []byte("two"))
	if !bytes.Equal(payload, want) {
		t.Fatalf("compact PUBLISH payload = %x; want %x", payload, want)
	}
	if got := binary.BigEndian.Uint32(payload[2:6]); got != uint32(len(commands)) {
		t.Fatalf("compact PUBLISH count = %d; want %d", got, len(commands))
	}
}

func TestPubSubOptionsPreflightUsesServerOpcode(t *testing.T) {
	if nativeOpOptions != 0x000B {
		t.Fatalf("OPTIONS opcode = %#04x; want 0x000B", nativeOpOptions)
	}
}

func TestCompactPubSubPublishPipelineDeclinesNonFastPathGrammar(t *testing.T) {
	for _, command := range [][]any{
		{"PUBLISH", "channel"},
		{"PUBLISH", "channel", "message", "extra"},
		{"PUBLISH", struct{}{}, "message"},
		{"PUBLISH", "channel", struct{}{}},
	} {
		if _, ok, err := compactPipelinePayloadWithLimit([][]any{command}, nativeMaxFrameBytes); err != nil {
			t.Fatalf("compact PUBLISH fallback for %#v returned error: %v", command, err)
		} else if ok {
			t.Fatalf("compact PUBLISH accepted non-fast-path grammar %#v", command)
		}
	}
}

func TestCompactPubSubPublishPipelineHonorsExactWireLimit(t *testing.T) {
	commands := [][]any{{"PUBLISH", "channel", "value"}}
	payload, ok, err := compactPipelinePayloadWithLimit(commands, nativeMaxFrameBytes)
	if err != nil || !ok {
		t.Fatalf("compact PUBLISH pipeline = ok %t, error %v", ok, err)
	}
	if exact, exactOK, exactErr := compactPipelinePayloadWithLimit(commands, len(payload)); exactErr != nil || !exactOK || !bytes.Equal(exact, payload) {
		t.Fatalf("exact-limit compact PUBLISH = %x, ok %t, error %v", exact, exactOK, exactErr)
	}
	_, overOK, overErr := compactPipelinePayloadWithLimit(commands, len(payload)-1)
	var limitErr nativeEncodeLimitError
	if !overOK || !errors.As(overErr, &limitErr) {
		t.Fatalf("over-limit compact PUBLISH = ok %t, error %v; want encoding limit", overOK, overErr)
	}
}

func TestCompactPubSubPublishPipelineRequiresAdvertisedMode(t *testing.T) {
	commands := [][]any{{"PUBLISH", "channel", "message"}}

	payload, flags, _, err := nativePipelinePayloadWithCapabilities(
		commands, 1, nativeDefaultRequestFrameBytes, true, false,
	)
	if err != nil {
		t.Fatal(err)
	}
	if flags&nativeFlagCustomPayload != 0 {
		t.Fatalf("legacy server received compact flags %#x and payload %#v", flags, payload)
	}

	_, flags, _, err = nativePipelinePayloadWithCapabilities(
		commands, 1, nativeDefaultRequestFrameBytes, true, true,
	)
	if err != nil {
		t.Fatal(err)
	}
	if flags&nativeFlagCustomPayload == 0 {
		t.Fatalf("advertised Pub/Sub mode did not use compact flags %#x", flags)
	}
}

func TestHelloNegotiatesCompactPubSubPublishMode(t *testing.T) {
	legacy, err := parseNativeHelloContract(nativeHelloForTest(), nativeDefaultResponseBytes)
	if err != nil {
		t.Fatal(err)
	}
	if legacy.compactPubSubPublish {
		t.Fatal("legacy HELLO enabled compact Pub/Sub PUBLISH")
	}

	hello := nativeHelloForTest()
	capabilities := hello["capabilities"].(map[string]any)
	capabilities["pipeline"] = map[string]any{
		"compact_request":      "pipeline_v1",
		"values_only_mode_bit": 0x80,
		"modes":                map[string]any{"pubsub_publish": 35},
	}
	negotiated, err := parseNativeHelloContract(hello, nativeDefaultResponseBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !negotiated.compactPubSubPublish {
		t.Fatal("advertised compact Pub/Sub PUBLISH mode was not negotiated")
	}
}

func TestHelloRequestsPubSubBatchOnlyWhenOptionsAdvertiseEventOpcode(t *testing.T) {
	legacy := nativeHelloPayload("test-client", map[string]any{})
	if got := legacy["compact_response_codecs"]; !reflect.DeepEqual(got, []any{"flow_query_result_v1"}) {
		t.Fatalf("legacy compact_response_codecs = %#v", got)
	}

	options := map[string]any{
		"response_codecs": map[string]any{
			"compact_response_opcodes": map[string]any{
				"pubsub_batch_v1": []any{int64(nativeOpEvent)},
			},
		},
	}
	payload := nativeHelloPayload("test-client", options)
	want := []any{"flow_query_result_v1", "pubsub_batch_v1"}
	if got := payload["compact_response_codecs"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("advertised compact_response_codecs = %#v; want %#v", got, want)
	}

	wrongOpcode := map[string]any{
		"response_codecs": map[string]any{
			"compact_response_opcodes": map[string]any{
				"pubsub_batch_v1": []any{int64(nativeOpEvent + 1)},
			},
		},
	}
	if got := nativeHelloPayload("test-client", wrongOpcode)["compact_response_codecs"]; !reflect.DeepEqual(got, []any{"flow_query_result_v1"}) {
		t.Fatalf("wrong-opcode compact_response_codecs = %#v", got)
	}
}

func TestPubSubBatchPreflightIsLimitedToNewPubSubConnections(t *testing.T) {
	legacy := newNativeExecutor(defaultNativeOptions("unused", false))
	legacy.enableEventDelivery()
	if legacy.opts.pubSubBatchCodec {
		t.Fatal("generic event delivery enabled Pub/Sub batch negotiation")
	}

	pubsub := newNativeExecutor(defaultNativeOptions("unused", false))
	pubsub.enablePubSubBatchDelivery()
	if !pubsub.opts.pubSubBatchCodec {
		t.Fatal("new Pub/Sub connection did not enable batch negotiation")
	}

	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()
	existing := newNativeExecutor(defaultNativeOptions("unused", false))
	existing.conn = client
	existing.enablePubSubBatchDelivery()
	if existing.opts.pubSubBatchCodec {
		t.Fatal("existing shared connection changed its negotiated codecs")
	}
}

func TestNativePubSubBatchExpandsInOrderBeforeQueueing(t *testing.T) {
	exec := newNativeExecutor(defaultNativeOptions("unused", false))
	exec.enableEventDelivery()
	t.Cleanup(func() { _ = exec.Close() })

	exec.deliverEvent(nativeServerEvent{
		opcode: nativeOpEvent,
		value: map[string]any{
			"event": "PUBSUB_MESSAGE",
			"at_ms": int64(1234),
			"payload": map[string]any{
				"kind":     "message_batch",
				"channel":  []byte("jobs"),
				"messages": []any{[]byte("one"), []byte("two")},
			},
		},
		wireBytes: 128,
	})

	pubsub := newPubSub(exec, false)
	for index, want := range []string{"one", "two"} {
		message, err := pubsub.Next(context.Background())
		if err != nil {
			t.Fatalf("message %d: %v", index, err)
		}
		if message.Kind != "message" || message.Channel != "jobs" || asString(message.Payload) != want {
			t.Fatalf("message %d = %#v; want jobs/%s", index, message, want)
		}
	}
	if exec.eventBufferedBytes != 0 {
		t.Fatalf("expanded batch left %d buffered bytes", exec.eventBufferedBytes)
	}
}
