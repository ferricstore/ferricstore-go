package ferricstore

import (
	"bytes"
	"testing"
)

func TestNativeCommandPreservesNamedRawMutationValues(t *testing.T) {
	type namedString string
	type namedBytes []byte

	tests := []struct {
		name  string
		value any
	}{
		{name: "named string", value: namedString("tail")},
		{name: "named bytes", value: namedBytes("tail")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			command, err := buildNativeCommand([]any{"APPEND", "key", tc.value})
			if err != nil {
				t.Fatal(err)
			}
			wire, err := encodeNativeValue(command.payload)
			if err != nil {
				t.Fatal(err)
			}
			decoded, rest, err := decodeNativeValue(wire)
			if err != nil || len(rest) != 0 {
				t.Fatalf("decode native command: rest=%d err=%v", len(rest), err)
			}
			payload, err := nativeMap(decoded)
			if err != nil {
				t.Fatal(err)
			}
			args, ok := payload["args"].([]any)
			if !ok || len(args) != 2 {
				t.Fatalf("native command args = %#v", payload["args"])
			}
			if got, ok := args[1].([]byte); !ok || !bytes.Equal(got, []byte("tail")) {
				t.Fatalf("wire mutation value = %#v; want raw bytes %q", args[1], "tail")
			}
		})
	}
}
