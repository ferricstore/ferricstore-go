package ferricstore

import (
	"context"
	"errors"
	"testing"
)

type nativeCustomPayloadTestEncoder interface {
	encodeNativeCustomPayload(int) ([]byte, error)
}

type mapValueCodec struct{}

func (mapValueCodec) Encode(value any) (any, error) { return map[string]any{"value": value}, nil }
func (mapValueCodec) Decode(value any) (any, error) { return value, nil }

func encodeNativeCustomPayloadForTest(t *testing.T, payload any) []byte {
	t.Helper()
	encoder, ok := payload.(nativeCustomPayloadTestEncoder)
	if !ok {
		t.Fatalf("native custom payload %T is not an encoder", payload)
	}
	body, err := encoder.encodeNativeCustomPayload(nativeMaxFrameBytes)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestCompactFlowPayloadsAreMaterializedLazilyAndBounded(t *testing.T) {
	tests := []struct {
		name string
		args []any
	}{
		{
			name: "create many",
			args: []any{
				"FLOW.CREATE_MANY", "MIXED", "TYPE", "email", "STATE", "queued",
				"NOW", int64(1), "RUN_AT", int64(1), "INDEPENDENT", true,
				"ITEMS", "flow-1", "p1", []byte("payload"),
			},
		},
		{
			name: "claim due",
			args: []any{
				"FLOW.CLAIM_DUE", "email", "WORKER", "worker-1", "LEASE_MS", int64(30_000),
				"LIMIT", int64(1), "RETURN", "JOBS_COMPACT",
			},
		},
		{
			name: "complete many",
			args: []any{
				"FLOW.COMPLETE_MANY", "MIXED", "NOW", int64(1), "INDEPENDENT", true,
				"ITEMS", "flow-1", "p1", "lease-1", int64(7),
			},
		},
		{
			name: "value mget",
			args: []any{
				"FLOW.VALUE.MGET", "f:{fa:1}:v:first", "f:{fa:1}:v:second",
				"MAX_BYTES", int64(1024),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command, err := buildNativeCommand(test.args)
			if err != nil {
				t.Fatal(err)
			}
			if _, eager := command.payload.([]byte); eager {
				t.Fatal("compact Flow request was fully materialized before native write admission")
			}
			encoder, ok := command.payload.(nativeCustomPayloadTestEncoder)
			if !ok {
				t.Fatalf("compact Flow payload %T is not a deferred encoder", command.payload)
			}
			if _, err := encoder.encodeNativeCustomPayload(1); err == nil {
				t.Fatal("compact Flow payload ignored the negotiated frame limit")
			} else {
				var limitErr nativeEncodeLimitError
				if !errors.As(err, &limitErr) {
					t.Fatalf("bounded compact Flow encoding error = %v; want nativeEncodeLimitError", err)
				}
			}
		})
	}
}

func TestCompactFlowFallsBackForCodecWithNonCompactOutput(t *testing.T) {
	exec := NewNativeExecutor("unused")
	defer func() { _ = exec.Close() }()
	client := NewClientWithExecutor(exec, WithCodec(mapValueCodec{}))
	_, ok, err := client.tryCreateManyNativeCompact(
		context.Background(),
		CreateManyOptions{Type: "email", Items: []CreateItem{{ID: "one", Payload: "payload"}}},
		"queued", 1, 1, false, "AUTO",
	)
	if err != nil || ok {
		t.Fatalf("custom codec compact selection = %t, %v; want generic fallback", ok, err)
	}
}
