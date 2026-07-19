package ferricstore

import (
	"context"
	"math"
	"reflect"
	"testing"
)

func TestV080RawSETOptionsAreStrictlyValidatedBeforeTransport(t *testing.T) {
	tests := []struct {
		name string
		args []any
	}{
		{name: "NX XX", args: []any{"SET", "key", "value", "NX", "XX"}},
		{name: "EX PX", args: []any{"SET", "key", "value", "EX", 1, "PX", 1}},
		{name: "EXAT PXAT", args: []any{"SET", "key", "value", "EXAT", 1, "PXAT", 1}},
		{name: "KEEPTTL expiry", args: []any{"SET", "key", "value", "KEEPTTL", "PX", 1}},
		{name: "zero expiry", args: []any{"SET", "key", "value", "EXAT", 0}},
		{name: "duplicate GET", args: []any{"SET", "key", "value", "GET", "GET"}},
		{name: "unknown option", args: []any{"SET", "key", "value", "LEGACY"}},
		{name: "missing expiry", args: []any{"SET", "key", "value", "PXAT"}},
		{name: "whitespace expiry", args: []any{"SET", "key", "value", "PXAT", " 1 "}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			client := NewClientWithExecutor(exec)
			if _, err := client.Command(context.Background(), tc.args...); err == nil {
				t.Fatalf("accepted SET command %#v", tc.args)
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid SET reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestV080NativeSETOptionsUseDedicatedProtocolV1Schema(t *testing.T) {
	tests := []struct {
		name string
		args []any
		want map[string]any
	}{
		{
			name: "EX NX GET",
			args: []any{"SET", "key", []byte("value"), "EX", int64(2), "NX", "GET"},
			want: map[string]any{"key": "key", "value": []byte("value"), "ttl": int64(2_000), "nx": true, "get": true},
		},
		{
			name: "EXAT XX",
			args: []any{"SET", "key", []byte("value"), "EXAT", int64(123), "XX"},
			want: map[string]any{"key": "key", "value": []byte("value"), "exat": int64(123), "xx": true},
		},
		{
			name: "PXAT",
			args: []any{"SET", "key", []byte("value"), "PXAT", int64(456)},
			want: map[string]any{"key": "key", "value": []byte("value"), "pxat": int64(456)},
		},
		{
			name: "KEEPTTL",
			args: []any{"SET", "key", []byte("value"), "KEEPTTL"},
			want: map[string]any{"key": "key", "value": []byte("value"), "keepttl": true},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			command, err := buildNativeCommand(tc.args)
			if err != nil {
				t.Fatal(err)
			}
			if command.opcode != nativeOpSet || command.flags != 0 || command.laneID != 1 {
				t.Fatalf("native SET command = %#v", command)
			}
			payload, ok := command.payload.(map[string]any)
			if !ok {
				t.Fatalf("native SET payload = %T", command.payload)
			}
			assertNativeMapEqual(t, payload, tc.want)
		})
	}
}

func TestV080SETEXConversionRejectsOverflow(t *testing.T) {
	if _, err := buildNativeCommand([]any{"SET", "key", "value", "EX", int64(math.MaxInt64)}); err == nil {
		t.Fatal("SET EX accepted a seconds value that overflows ttl milliseconds")
	}
}

func TestV080SetWithOptionsAcceptsDedicatedConditionalResponse(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response any
		want     any
	}{
		{name: "condition applied", response: true, want: true},
		{name: "condition not applied", response: false, want: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := NewClientWithExecutor(&fakeExecutor{value: tc.response}).KV()
			value, err := store.SetWithOptions(
				context.Background(), "key", "value", SetOptions{NX: true},
			)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(value, tc.want) {
				t.Fatalf("conditional SET response = %#v; want %#v", value, tc.want)
			}
		})
	}
}

func TestV080SETOptionParsingDoesNotAllocate(t *testing.T) {
	args := []any{"EXAT", int64(123), "NX", "GET"}
	allocations := testing.AllocsPerRun(1000, func() {
		if _, err := parseSETCommandOptions(args); err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("SET option parsing allocations = %.0f, want 0", allocations)
	}
}

func assertNativeMapEqual(t *testing.T, got, want map[string]any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("native map = %#v; want %#v", got, want)
	}
	for key, expected := range want {
		actual, exists := got[key]
		if !exists || !nativeFuzzValuesEqual(actual, expected) {
			t.Fatalf("native map = %#v; want %#v", got, want)
		}
	}
}
