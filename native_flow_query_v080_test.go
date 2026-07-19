package ferricstore

import (
	"reflect"
	"testing"
)

func TestV080FlowQueriesUseDedicatedNativeSchemas(t *testing.T) {
	tests := []struct {
		name   string
		opcode uint16
		args   []any
		want   map[string]any
	}{
		{
			name: "get values", opcode: 0x0202,
			args: []any{"FLOW.GET", "flow-1", "PARTITION", "tenant-1", "VALUE", "result"},
			want: map[string]any{
				"id": "flow-1", "partition_key": "tenant-1", "values": []string{"result"},
			},
		},
		{
			name: "list", opcode: 0x020E,
			args: []any{
				"FLOW.LIST", "email", "STATE", "queued", "COUNT", 5,
				"ATTRIBUTE", "tenant", "a", "REV", "true",
			},
			want: map[string]any{
				"type": "email", "state": "queued", "count": 5,
				"attributes": map[string]any{"tenant": "a"}, "rev": true,
			},
		},
		{
			name: "history", opcode: 0x020A,
			args: []any{
				"FLOW.HISTORY", "flow-1", "COUNT", 10, "FROM_EVENT", "evt-1",
				"VALUES", "true", "PAYLOAD_MAX_BYTES", int64(4096),
			},
			want: map[string]any{
				"id": "flow-1", "count": 10, "from_event": "evt-1",
				"values": true, "payload_max_bytes": int64(4096),
			},
		},
		{
			name: "search nested state meta", opcode: 0x0230,
			args: []any{
				"FLOW.SEARCH", "TYPE", "email", "STATE_META", "queued", "attempt", int64(2),
			},
			want: map[string]any{
				"type":       "email",
				"state_meta": map[string]map[string]any{"queued": {"attempt": int64(2)}},
			},
		},
		{
			name: "stats", opcode: 0x022D,
			args: []any{"FLOW.STATS", "email", "STATE", "queued"},
			want: map[string]any{"type": "email", "state": "queued"},
		},
		{
			name: "attribute values", opcode: 0x022F,
			args: []any{"FLOW.ATTRIBUTE_VALUES", "email", "tenant", "COUNT", 5},
			want: map[string]any{"type": "email", "attribute": "tenant", "count": 5},
		},
		{
			name: "by parent", opcode: 0x0219,
			args: []any{"FLOW.BY_PARENT", "parent-1", "COUNT", 5},
			want: map[string]any{"parent_id": "parent-1", "count": 5},
		},
		{
			name: "stuck", opcode: 0x021D,
			args: []any{"FLOW.STUCK", "email", "OLDER_THAN", int64(1000), "NOW", int64(2000)},
			want: map[string]any{"type": "email", "older_than_ms": int64(1000), "now_ms": int64(2000)},
		},
		{
			name: "retention cleanup", opcode: 0x0221,
			args: []any{"FLOW.RETENTION_CLEANUP", "LIMIT", 50, "NOW", int64(2000)},
			want: map[string]any{"limit": 50, "now_ms": int64(2000)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertV080NativeFlowMap(t, test.args, test.opcode, test.want)
		})
	}
}

func TestV080FlowClaimFallbackUsesDedicatedNativeSchema(t *testing.T) {
	assertV080NativeFlowMap(t, []any{
		"FLOW.CLAIM_DUE", "email",
		"STATE", "queued", "STATE", "waiting", "WORKER", "worker-1",
		"LEASE_MS", int64(30000), "LIMIT", 2,
		"PARTITIONS", 2, "tenant-1", "tenant-2",
		"PAYLOAD", "MAXBYTES", int64(4096), "VALUE", "input",
		"RECLAIM_EXPIRED", "false", "RETURN", "JOBS_COMPACT",
	}, 0x0203, map[string]any{
		"type": "email", "states": []string{"queued", "waiting"}, "worker": "worker-1",
		"lease_ms": int64(30000), "limit": 2,
		"partition_keys": []string{"tenant-1", "tenant-2"},
		"payload":        true, "payload_max_bytes": int64(4096), "values": []string{"input"},
		"reclaim_expired": false, "return": "JOBS_COMPACT",
	})
}

func TestV080FlowReclaimUsesDedicatedNativeSchema(t *testing.T) {
	assertV080NativeFlowMap(t, []any{
		"FLOW.RECLAIM", "email", "WORKER", "worker-1", "LEASE_MS", int64(30000),
		"LIMIT", 2, "PARTITIONS", 2, "tenant-1", "tenant-2", "NOPAYLOAD", "VALUE", "input",
	}, 0x0215, map[string]any{
		"type": "email", "worker": "worker-1", "lease_ms": int64(30000), "limit": 2,
		"partition_keys": []string{"tenant-1", "tenant-2"},
		"payload":        false, "values": []string{"input"},
	})
}

func assertV080NativeFlowMap(t *testing.T, args []any, opcode uint16, want map[string]any) {
	t.Helper()
	command, err := buildNativeCommand(args)
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != opcode || command.laneID != 1 || command.flags != 0 {
		t.Fatalf("command = opcode %#x lane %d flags %#x, want %#x/1/0", command.opcode, command.laneID, command.flags, opcode)
	}
	payload, ok := command.payload.(map[string]any)
	if !ok || !reflect.DeepEqual(payload, want) {
		t.Fatalf("payload = %#v, want %#v", command.payload, want)
	}
	if _, err := encodeNativeValue(payload); err != nil {
		t.Fatalf("encode payload: %v", err)
	}
}
