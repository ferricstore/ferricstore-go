package ferricstore

import (
	"encoding/json"
	"fmt"
	"testing"
)

func BenchmarkHTTPEncodePipeline100(b *testing.B) {
	commands := make([][]any, 100)
	for index := range commands {
		commands[index] = []any{"SET", fmt.Sprintf("key:%d", index), []byte("value")}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		encoded := make([]any, len(commands))
		state := newHTTPEncodeState(defaultHTTPMaxRequestBytes)
		for index, command := range commands {
			var err error
			encoded[index], err = encodeHTTPCommandWithState(command, state)
			if err != nil {
				b.Fatal(err)
			}
		}
		if _, err := json.Marshal(map[string]any{
			"encoding": httpBinaryEncoding,
			"commands": encoded,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHTTPDecodeResults100(b *testing.B) {
	results := make([]any, 100)
	for index := range results {
		results[index] = map[string]any{
			"status": "ok",
			"value":  map[string]any{httpBytesTag: "dmFsdWU="},
		}
	}
	envelope := map[string]any{"encoding": httpBinaryEncoding, "results": results}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := decodeHTTPResults(envelope, len(results)); err != nil {
			b.Fatal(err)
		}
	}
}
