package ferricstore

import (
	"reflect"
	"testing"
)

func TestV080FlowLifecycleUsesDedicatedNativeSchemas(t *testing.T) {
	tests := []struct {
		name   string
		opcode uint16
		args   []any
		want   map[string]any
	}{
		{
			name: "create", opcode: 0x0201,
			args: []any{
				"FLOW.CREATE", "flow-1", "TYPE", "email", "STATE", "queued",
				"VALUE", "input", []byte("payload"), "VALUE_REF", "blob", "ref-1",
				"ATTRIBUTE", "tenant", "a", "STATE_META", "attempt", int64(1),
			},
			want: map[string]any{
				"id": "flow-1", "type": "email", "state": "queued",
				"values":     map[string]any{"input": []byte("payload")},
				"value_refs": map[string]any{"blob": "ref-1"},
				"attributes": map[string]any{"tenant": "a"},
				"state_meta": map[string]any{"attempt": int64(1)},
			},
		},
		{
			name: "complete", opcode: 0x0204,
			args: []any{
				"FLOW.COMPLETE", "flow-1", "lease-1", "FENCING", int64(7),
				"TTL", int64(1000), "VALUE", "output", []byte("done"),
			},
			want: map[string]any{
				"id": "flow-1", "lease_token": "lease-1", "fencing_token": int64(7),
				"ttl_ms": int64(1000), "values": map[string]any{"output": []byte("done")},
			},
		},
		{
			name: "transition", opcode: 0x0205,
			args: []any{
				"FLOW.TRANSITION", "flow-1", "queued", "ready", "LEASE_TOKEN", "lease-1",
				"FENCING", int64(8), "DROP_VALUE", "temporary",
			},
			want: map[string]any{
				"id": "flow-1", "from_state": "queued", "to_state": "ready",
				"lease_token": "lease-1", "fencing_token": int64(8),
				"drop_values": []string{"temporary"},
			},
		},
		{
			name: "retry", opcode: 0x0206,
			args: []any{
				"FLOW.RETRY", "flow-1", "lease-1", "FENCING", int64(9),
				"ERROR", []byte("again"), "RUN_AT", int64(200),
			},
			want: map[string]any{
				"id": "flow-1", "lease_token": "lease-1", "fencing_token": int64(9),
				"error": []byte("again"), "run_at_ms": int64(200),
			},
		},
		{
			name: "fail", opcode: 0x0207,
			args: []any{
				"FLOW.FAIL", "flow-1", "lease-1", "FENCING", int64(10),
				"OVERRIDE_VALUE", "failure",
			},
			want: map[string]any{
				"id": "flow-1", "lease_token": "lease-1", "fencing_token": int64(10),
				"override_values": []string{"failure"},
			},
		},
		{
			name: "cancel", opcode: 0x0208,
			args: []any{
				"FLOW.CANCEL", "flow-1", "FENCING", int64(11), "LEASE_TOKEN", "lease-1",
				"REASON", []byte("stop"),
			},
			want: map[string]any{
				"id": "flow-1", "fencing_token": int64(11), "lease_token": "lease-1",
				"reason": []byte("stop"),
			},
		},
		{
			name: "extend lease", opcode: 0x0209,
			args: []any{
				"FLOW.EXTEND_LEASE", "flow-1", "lease-1", "FENCING", int64(12),
				"LEASE_MS", int64(30000),
			},
			want: map[string]any{
				"id": "flow-1", "lease_token": "lease-1", "fencing_token": int64(12),
				"lease_ms": int64(30000),
			},
		},
		{
			name: "rewind", opcode: 0x0216,
			args: []any{
				"FLOW.REWIND", "flow-1", "TO_EVENT", "evt-4", "EXPECT_STATE", "failed",
			},
			want: map[string]any{"id": "flow-1", "to_event": "evt-4", "expect_state": "failed"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command, err := buildNativeCommand(test.args)
			if err != nil {
				t.Fatal(err)
			}
			if command.opcode != test.opcode || command.laneID != 1 || command.flags != 0 {
				t.Fatalf("command = opcode %#x lane %d flags %#x, want %#x/1/0", command.opcode, command.laneID, command.flags, test.opcode)
			}
			payload, ok := command.payload.(map[string]any)
			if !ok || !reflect.DeepEqual(payload, test.want) {
				t.Fatalf("payload = %#v, want %#v", command.payload, test.want)
			}
			if _, err := encodeNativeValue(payload); err != nil {
				t.Fatalf("encode payload: %v", err)
			}
		})
	}
}

func TestV080FlowMutationBatchesUseDedicatedNativeSchemas(t *testing.T) {
	tests := []struct {
		name   string
		opcode uint16
		args   []any
		want   map[string]any
	}{
		{
			name: "complete many", opcode: 0x0210,
			args: []any{
				"FLOW.COMPLETE_MANY", "tenant-1", "RESULT", []byte("done"),
				"VALUE", "output", []byte("value"), "ITEMS", "flow-1", "lease-1", int64(3),
			},
			want: map[string]any{
				"partition_key": "tenant-1", "result": []byte("done"),
				"values": map[string]any{"output": []byte("value")},
				"items": []any{map[string]any{
					"id": "flow-1", "lease_token": "lease-1", "fencing_token": int64(3),
				}},
			},
		},
		{
			name: "transition many", opcode: 0x0211,
			args: []any{
				"FLOW.TRANSITION_MANY", "MIXED", "queued", "ready", "NOW", int64(10),
				"ITEMS", "flow-1", "tenant-1", int64(4), "lease-1",
			},
			want: map[string]any{
				"from_state": "queued", "to_state": "ready", "now_ms": int64(10),
				"items": []any{map[string]any{
					"id": "flow-1", "partition_key": "tenant-1",
					"fencing_token": int64(4), "lease_token": "lease-1",
				}},
			},
		},
		{
			name: "retry many", opcode: 0x0212,
			args: []any{
				"FLOW.RETRY_MANY", "tenant-1", "ERROR", []byte("again"),
				"ITEMS", "flow-1", "lease-1", int64(5),
			},
			want: map[string]any{
				"partition_key": "tenant-1", "error": []byte("again"),
				"items": []any{map[string]any{
					"id": "flow-1", "lease_token": "lease-1", "fencing_token": int64(5),
				}},
			},
		},
		{
			name: "fail many", opcode: 0x0213,
			args: []any{
				"FLOW.FAIL_MANY", "tenant-1", "TTL", int64(1000),
				"ITEMS", "flow-1", "lease-1", int64(6),
			},
			want: map[string]any{
				"partition_key": "tenant-1", "ttl_ms": int64(1000),
				"items": []any{map[string]any{
					"id": "flow-1", "lease_token": "lease-1", "fencing_token": int64(6),
				}},
			},
		},
		{
			name: "cancel many", opcode: 0x0214,
			args: []any{
				"FLOW.CANCEL_MANY", "MIXED", "REASON", []byte("stop"),
				"ITEMS", "flow-1", "tenant-1", int64(7),
			},
			want: map[string]any{
				"reason": []byte("stop"),
				"items": []any{map[string]any{
					"id": "flow-1", "partition_key": "tenant-1", "fencing_token": int64(7),
				}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command, err := buildNativeCommand(test.args)
			if err != nil {
				t.Fatal(err)
			}
			if command.opcode != test.opcode || command.laneID != 1 || command.flags != 0 {
				t.Fatalf("command = opcode %#x lane %d flags %#x, want %#x/1/0", command.opcode, command.laneID, command.flags, test.opcode)
			}
			payload, ok := command.payload.(map[string]any)
			if !ok || !reflect.DeepEqual(payload, test.want) {
				t.Fatalf("payload = %#v, want %#v", command.payload, test.want)
			}
			if _, err := encodeNativeValue(payload); err != nil {
				t.Fatalf("encode payload: %v", err)
			}
		})
	}
}
