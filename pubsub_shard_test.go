package ferricstore

import (
	"bufio"
	"context"
	"net"
	"reflect"
	"testing"
	"time"
)

func TestNativePubSubDeliversShardMessages(t *testing.T) {
	exec := newNativeExecutor(defaultNativeOptions("unused", false))
	exec.enableEventDelivery()
	t.Cleanup(func() { _ = exec.Close() })

	exec.deliverEvent(nativeServerEvent{
		opcode: nativeOpEvent,
		value: map[string]any{
			"event": "PUBSUB_MESSAGE",
			"at_ms": int64(1234),
			"payload": map[string]any{
				"kind":    "smessage",
				"channel": []byte("orders"),
				"message": []byte("created"),
			},
		},
		wireBytes: 128,
	})

	message, err := newPubSub(exec, false).Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if message.Kind != "smessage" || message.Channel != "orders" || asString(message.Payload) != "created" {
		t.Fatalf("unexpected shard message: %#v", message)
	}
}

func TestPubSubParsesShardShapes(t *testing.T) {
	message, err := parsePubSubMessage([]any{[]byte("smessage"), []byte("orders"), []byte("body")})
	if err != nil {
		t.Fatal(err)
	}
	if message.Kind != "smessage" || message.Channel != "orders" || asString(message.Payload) != "body" {
		t.Fatalf("unexpected flat shard message: %#v", message)
	}

	ack, err := pubSubAcknowledgementFromNative([]any{"sunsubscribe", nil, int64(0)}, "SUNSUBSCRIBE")
	if err != nil {
		t.Fatal(err)
	}
	if ack.Kind != "sunsubscribe" || ack.Channel != "" || ack.Count != 0 {
		t.Fatalf("unexpected unsubscribe-all ack: %#v", ack)
	}

	if _, err := pubSubAcknowledgementFromNative([]any{"ssubscribe", nil, int64(1)}, "SSUBSCRIBE"); err == nil {
		t.Fatal("an SSUBSCRIBE acknowledgement without a channel was accepted")
	}

	native := pubSubMessageFromNative([]any{"ssubscribe", "orders", int64(2)})
	if native.Kind != "ssubscribe" || native.Channel != "orders" || native.Count != 2 {
		t.Fatalf("unexpected shard ack from native value: %#v", native)
	}
}

func TestPubSubTracksShardChannelsApart(t *testing.T) {
	p := newPubSub(&NativeExecutor{}, false)
	p.trackPubSubCommand([]any{"SUBSCRIBE", "jobs"})
	p.trackPubSubCommand([]any{"SSUBSCRIBE", "orders", "billing"})

	if got := p.trackedPubSubCount("SSUBSCRIBE"); got != 2 {
		t.Fatalf("shard count = %d, want 2", got)
	}
	if got := p.trackedPubSubCount("SUBSCRIBE"); got != 1 {
		t.Fatalf("channel count = %d, want 1 (shard channels counted apart)", got)
	}

	p.trackPubSubCommand([]any{"SUNSUBSCRIBE", "orders"})
	if !reflect.DeepEqual(mapKeys(p.shardChannels), []string{"billing"}) {
		t.Fatalf("shard channels after SUNSUBSCRIBE orders = %#v", p.shardChannels)
	}
	p.trackPubSubCommand([]any{"SUNSUBSCRIBE"})
	if p.shardChannels != nil {
		t.Fatal("sunsubscribe-all retained an empty shard channel map")
	}
	if !reflect.DeepEqual(mapKeys(p.channels), []string{"jobs"}) {
		t.Fatalf("SUNSUBSCRIBE touched ordinary channels: %#v", p.channels)
	}
}

func TestPubSubShardSubscribeRequiresAChannel(t *testing.T) {
	p := newPubSub(&NativeExecutor{}, false)
	if _, err := p.SSubscribe(context.Background()); err == nil {
		t.Fatal("SSUBSCRIBE without a channel was sent")
	}
}

func TestPubSubReplaysShardSubscriptionsAfterReconnect(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	errCh := make(chan error, 1)
	go func() {
		first, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		reader, writer := bufio.NewReader(first), bufio.NewWriter(first)
		if err := serveNativeStartup(reader, writer); err != nil {
			errCh <- err
			return
		}
		subscribe, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, subscribe, nativeStatusOK, []any{"ssubscribe", "orders", int64(1)}); err != nil {
			errCh <- err
			return
		}
		_ = first.Close()

		second, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = second.Close() }()
		reader, writer = bufio.NewReader(second), bufio.NewWriter(second)
		if err := serveNativeStartup(reader, writer); err != nil {
			errCh <- err
			return
		}
		ping, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, ping, nativeStatusOK, []byte("PONG")); err != nil {
			errCh <- err
			return
		}
		replay, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		value, _, err := decodeNativeValue(replay.body)
		if err != nil {
			errCh <- err
			return
		}
		payload := value.(map[string]any)
		if replay.opcode != nativeOpCommandExec || asString(payload["command"]) != "SSUBSCRIBE" {
			errCh <- errUnexpectedFrame(replay)
			return
		}
		if err := writeNativeTestResponse(writer, replay, nativeStatusOK, []any{"ssubscribe", "orders", int64(1)}); err != nil {
			errCh <- err
			return
		}
		errCh <- writeNativeTestResponse(writer, nativeFrame{opcode: nativeOpEvent, requestID: 0}, nativeStatusOK, []any{"smessage", "orders", "after-reconnect"})
	}()

	pubsub := NewPubSub(listener.Addr().String(), WithNativeHeartbeat(0, 0))
	defer func() { _ = pubsub.Close() }()
	subscribeCtx, cancelSubscribe := context.WithTimeout(context.Background(), time.Second)
	ack, err := pubsub.SSubscribe(subscribeCtx, "orders")
	cancelSubscribe()
	if err != nil {
		t.Fatal(err)
	}
	if ack.Kind != "ssubscribe" || ack.Channel != "orders" || ack.Count != 1 {
		t.Fatalf("unexpected ssubscribe ack: %#v", ack)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		pubsub.exec.mu.Lock()
		disconnected := pubsub.exec.conn == nil
		pubsub.exec.mu.Unlock()
		if disconnected {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("native reader did not observe the closed subscription connection")
		}
		time.Sleep(time.Millisecond)
	}
	reconnectCtx, cancelReconnect := context.WithTimeout(context.Background(), 2*time.Second)
	got, err := pubsub.exec.command(reconnectCtx, "PING")
	cancelReconnect()
	if err != nil || asString(got) != "PONG" {
		t.Fatalf("ordinary reconnecting command = %#v, %v", got, err)
	}
	nextCtx, cancelNext := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelNext()
	message, err := pubsub.Next(nextCtx)
	if err != nil {
		t.Fatal(err)
	}
	if message.Kind != "smessage" || message.Channel != "orders" || asString(message.Payload) != "after-reconnect" {
		t.Fatalf("unexpected replayed shard message: %#v", message)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestClientShardPubSubHelpersBuildCommands(t *testing.T) {
	exec := &fakeExecutor{values: []any{
		int64(1),
		[]any{[]byte("orders")},
		[]any{[]byte("orders"), int64(1)},
	}}
	client := NewClientWithExecutor(exec)
	ctx := context.Background()

	if got, err := client.SPublish(ctx, "orders", "created"); err != nil || got != 1 {
		t.Fatalf("SPublish = %d, %v", got, err)
	}
	if got, err := client.PubSubShardChannels(ctx, "ord*"); err != nil || !reflect.DeepEqual(got, []string{"orders"}) {
		t.Fatalf("PubSubShardChannels = %#v, %v", got, err)
	}
	if got, err := client.PubSubShardNumSub(ctx, "orders"); err != nil || got["orders"] != 1 {
		t.Fatalf("PubSubShardNumSub = %#v, %v", got, err)
	}

	want := [][]any{
		{"SPUBLISH", "orders", "created"},
		{"PUBSUB", "SHARDCHANNELS", "ord*"},
		{"PUBSUB", "SHARDNUMSUB", "orders"},
	}
	if !reflect.DeepEqual(exec.calls, want) {
		t.Fatalf("calls\n got: %#v\nwant: %#v", exec.calls, want)
	}
}
