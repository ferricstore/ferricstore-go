package ferricstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestCompactStreamXAddPipelineWireShape(t *testing.T) {
	commands := [][]any{
		{"XADD", "stream-a", "*", "field", "one"},
		{"xadd", []byte("stream-b"), []byte("*"), "first", "two", "second", "three"},
	}
	payload, ok, err := compactPipelinePayloadWithLimit(commands, nativeMaxFrameBytes)
	if err != nil || !ok {
		t.Fatalf("compact XADD pipeline = ok %t, error %v", ok, err)
	}

	want := []byte{nativeCompactPipelineRequest, 0x80 | 34, 0, 0, 0, 2}
	want = appendCompactBinary(want, []byte("stream-a"))
	want = appendUint16(want, 1)
	want = appendCompactBinary(want, []byte("field"))
	want = appendCompactBinary(want, []byte("one"))
	want = appendCompactBinary(want, []byte("stream-b"))
	want = appendUint16(want, 2)
	for _, value := range []string{"first", "two", "second", "three"} {
		want = appendCompactBinary(want, []byte(value))
	}
	if !bytes.Equal(payload, want) {
		t.Fatalf("compact XADD payload = %x; want %x", payload, want)
	}
	if got := binary.BigEndian.Uint32(payload[2:6]); got != uint32(len(commands)) {
		t.Fatalf("compact XADD count = %d; want %d", got, len(commands))
	}
}

func TestCompactStreamXAddPipelineDeclinesNonFastPathGrammar(t *testing.T) {
	for _, command := range [][]any{
		{"XADD", "stream", "1-0", "field", "value"},
		{"XADD", "stream", "NOMKSTREAM", "*", "field", "value"},
		{"XADD", "stream", "MAXLEN", "~", 100, "*", "field", "value"},
		{"XADD", "stream", "*"},
		{"XADD", "stream", "*", "field"},
		{"XADD", "stream", "*", "field", struct{}{}},
	} {
		if _, ok, err := compactPipelinePayloadWithLimit([][]any{command}, nativeMaxFrameBytes); err != nil {
			t.Fatalf("compact XADD fallback for %#v returned error: %v", command, err)
		} else if ok {
			t.Fatalf("compact XADD accepted non-fast-path grammar %#v", command)
		}
	}
}

func TestCompactStreamXAddPipelineHonorsExactWireLimit(t *testing.T) {
	commands := [][]any{{"XADD", "stream", "*", "field", "value"}}
	payload, ok, err := compactPipelinePayloadWithLimit(commands, nativeMaxFrameBytes)
	if err != nil || !ok {
		t.Fatalf("compact XADD pipeline = ok %t, error %v", ok, err)
	}
	if exact, exactOK, exactErr := compactPipelinePayloadWithLimit(commands, len(payload)); exactErr != nil || !exactOK || !bytes.Equal(exact, payload) {
		t.Fatalf("exact-limit compact XADD = %x, ok %t, error %v", exact, exactOK, exactErr)
	}
	_, overOK, overErr := compactPipelinePayloadWithLimit(commands, len(payload)-1)
	var limitErr nativeEncodeLimitError
	if !overOK || !errors.As(overErr, &limitErr) {
		t.Fatalf("over-limit compact XADD = ok %t, error %v; want encoding limit", overOK, overErr)
	}
}

func TestCompactPipelineHeaderBudgetIsExact(t *testing.T) {
	commands := [][]any{{"GET", "key"}}
	payload, ok, err := compactPipelinePayloadWithLimit(commands, nativeMaxFrameBytes)
	if err != nil || !ok {
		t.Fatalf("compact GET pipeline = ok %t, error %v", ok, err)
	}
	if _, exactOK, exactErr := compactPipelinePayloadWithLimit(commands, len(payload)); exactErr != nil || !exactOK {
		t.Fatalf("exact-limit compact GET = ok %t, error %v", exactOK, exactErr)
	}
	_, overOK, overErr := compactPipelinePayloadWithLimit(commands, len(payload)-1)
	var limitErr nativeEncodeLimitError
	if !overOK || !errors.As(overErr, &limitErr) {
		t.Fatalf("over-limit compact GET = ok %t, error %v; want encoding limit", overOK, overErr)
	}
}

func TestCompactStreamXAddPipelinePlanningDoesNotAllocate(t *testing.T) {
	commands := [][]any{{"XADD", []byte("stream"), "*", []byte("field"), []byte("value")}}
	allocs := testing.AllocsPerRun(1_000, func() {
		plan, ok, err := compactPipelinePlanWithLimit(commands, nativeDefaultRequestFrameBytes)
		if err != nil || !ok || plan.size == 0 {
			panic("compact XADD pipeline was not planned")
		}
	})
	if allocs != 0 {
		t.Fatalf("compact XADD pipeline planning allocations = %.0f; want 0", allocs)
	}
}

func TestCompactStreamXAddPipelineRequiresAdvertisedMode(t *testing.T) {
	commands := [][]any{{"XADD", "stream", "*", "field", "value"}}

	payload, flags, _, err := nativePipelinePayloadWithCapabilities(
		commands, 1, nativeDefaultRequestFrameBytes, false,
	)
	if err != nil {
		t.Fatal(err)
	}
	if flags&nativeFlagCustomPayload != 0 {
		t.Fatalf("legacy server received compact flags %#x and payload %#v", flags, payload)
	}

	_, flags, _, err = nativePipelinePayloadWithCapabilities(
		commands, 1, nativeDefaultRequestFrameBytes, true,
	)
	if err != nil {
		t.Fatal(err)
	}
	if flags&nativeFlagCustomPayload == 0 {
		t.Fatalf("advertised Stream mode did not use compact flags %#x", flags)
	}
}

func TestHelloNegotiatesCompactStreamXAddMode(t *testing.T) {
	legacy, err := parseNativeHelloContract(nativeHelloForTest(), nativeDefaultResponseBytes)
	if err != nil {
		t.Fatal(err)
	}
	if legacy.compactStreamXAdd {
		t.Fatal("legacy HELLO enabled compact Stream XADD")
	}

	hello := nativeHelloForTest()
	capabilities := hello["capabilities"].(map[string]any)
	capabilities["pipeline"] = map[string]any{
		"compact_request":      "pipeline_v1",
		"values_only_mode_bit": 0x80,
		"modes":                map[string]any{"stream_xadd_auto": 34},
	}
	negotiated, err := parseNativeHelloContract(hello, nativeDefaultResponseBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !negotiated.compactStreamXAdd {
		t.Fatal("advertised compact Stream XADD mode was not negotiated")
	}
}
