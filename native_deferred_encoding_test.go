package ferricstore

import (
	"errors"
	"testing"
)

type recordingCodec struct {
	calls int
	err   error
}

func (c *recordingCodec) Encode(value any) (any, error) {
	c.calls++
	if c.err != nil {
		return nil, c.err
	}
	return []byte(asString(value)), nil
}

func (*recordingCodec) Decode(value any) (any, error) { return value, nil }

func TestNativeStoreCodecEncodingIsDeferredUntilWireEncoding(t *testing.T) {
	codec := &recordingCodec{}
	exec := NewNativeExecutor("unused")
	defer func() { _ = exec.Close() }()
	client := NewClientWithExecutor(exec, WithCodec(codec))
	encoded, err := client.encode("value")
	if err != nil {
		t.Fatal(err)
	}
	if codec.calls != 0 {
		t.Fatalf("native store codec ran %d time(s) before native write admission", codec.calls)
	}
	body, err := encodeNativeValueWithLimit(encoded, 64)
	if err != nil {
		t.Fatal(err)
	}
	if codec.calls != 1 {
		t.Fatalf("native wire encoding called codec %d times; want 1", codec.calls)
	}
	value, rest, err := decodeNativeValue(body)
	if err != nil || len(rest) != 0 || asString(value) != "value" {
		t.Fatalf("deferred codec round trip = %#v, rest %x, err %v", value, rest, err)
	}
}

func TestCustomExecutorCodecErrorsRemainSynchronous(t *testing.T) {
	want := errors.New("encode failed")
	codec := &recordingCodec{err: want}
	client := NewClientWithExecutor(&fakeExecutor{}, WithCodec(codec))
	if _, err := client.encode("value"); !errors.Is(err, want) {
		t.Fatalf("custom executor codec error = %v; want %v", err, want)
	}
}

func TestNativeCommandJSONArgumentsAreDeferredUntilWireEncoding(t *testing.T) {
	command, err := buildNativeCommand([]any{"CUSTOM", map[string]any{"large": "value"}})
	if err != nil {
		t.Fatal(err)
	}
	payload := command.payload.(map[string]any)
	args := payload["args"].([]any)
	if _, eager := args[0].([]byte); eager {
		t.Fatal("custom command JSON argument was materialized before native write admission")
	}
	if _, err := encodeNativeValueWithLimit(command.payload, 8); err == nil {
		t.Fatal("deferred JSON argument ignored native frame encoding limit")
	}
}

func TestDeferredStoreCodecSurvivesCommandExecWrapping(t *testing.T) {
	codec := &recordingCodec{}
	exec := NewNativeExecutor("unused")
	defer func() { _ = exec.Close() }()
	client := NewClientWithExecutor(exec, WithCodec(codec))
	encoded, err := client.encode("value")
	if err != nil {
		t.Fatal(err)
	}
	command, err := commandExecNativeCommand("HSET", []any{"key", "field", encoded})
	if err != nil {
		t.Fatal(err)
	}
	body, err := encodeNativeValue(command.payload)
	if err != nil {
		t.Fatal(err)
	}
	decoded, rest, err := decodeNativeValue(body)
	if err != nil || len(rest) != 0 {
		t.Fatalf("decode command payload: rest %x, err %v", rest, err)
	}
	args := decoded.(map[string]any)["args"].([]any)
	if got := asString(args[2]); got != "value" {
		t.Fatalf("deferred COMMAND_EXEC value = %q; want value", got)
	}
}
