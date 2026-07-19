package ferricstore

import (
	"reflect"
	"testing"
)

func TestV080AdministrationCommandsUseDedicatedNativeSchemas(t *testing.T) {
	tests := []struct {
		name   string
		opcode uint16
		args   []any
		want   map[string]any
	}{
		{name: "health", opcode: 0x0301, args: []any{"CLUSTER.HEALTH"}, want: map[string]any{"args": []any{}}},
		{name: "stats", opcode: 0x0302, args: []any{"CLUSTER.STATS"}, want: map[string]any{"args": []any{}}},
		{name: "keyslot", opcode: 0x0303, args: []any{"CLUSTER.KEYSLOT", "key"}, want: map[string]any{"key": "key", "args": []any{"key"}}},
		{name: "slots", opcode: 0x0304, args: []any{"CLUSTER.SLOTS"}, want: map[string]any{"args": []any{}}},
		{name: "status", opcode: 0x0305, args: []any{"CLUSTER.STATUS"}, want: map[string]any{"args": []any{}}},
		{name: "join", opcode: 0x0306, args: []any{"CLUSTER.JOIN", "node:7379", "REPLACE"}, want: map[string]any{"args": []any{"node:7379", "REPLACE"}}},
		{name: "leave", opcode: 0x0307, args: []any{"CLUSTER.LEAVE"}, want: map[string]any{"args": []any{}}},
		{name: "failover", opcode: 0x0308, args: []any{"CLUSTER.FAILOVER", 1, "node:7379"}, want: map[string]any{"args": []any{1, "node:7379"}}},
		{name: "promote", opcode: 0x0309, args: []any{"CLUSTER.PROMOTE", "node:7379"}, want: map[string]any{"args": []any{"node:7379"}}},
		{name: "demote", opcode: 0x030A, args: []any{"CLUSTER.DEMOTE", "node:7379"}, want: map[string]any{"args": []any{"node:7379"}}},
		{name: "role", opcode: 0x030B, args: []any{"CLUSTER.ROLE"}, want: map[string]any{"args": []any{}}},
		{name: "key info", opcode: 0x030C, args: []any{"FERRICSTORE.KEY_INFO", "key"}, want: map[string]any{"key": "key", "args": []any{"key"}}},
		{name: "config", opcode: 0x030D, args: []any{"FERRICSTORE.CONFIG", "GET", "*"}, want: map[string]any{"args": []any{"GET", "*"}}},
		{name: "hotness", opcode: 0x030E, args: []any{"FERRICSTORE.HOTNESS", "TOP", 5}, want: map[string]any{"args": []any{"TOP", 5}}},
		{name: "metrics", opcode: 0x030F, args: []any{"FERRICSTORE.METRICS"}, want: map[string]any{"args": []any{}}},
		{name: "blobgc", opcode: 0x0310, args: []any{"FERRICSTORE.BLOBGC", "STATUS"}, want: map[string]any{"args": []any{"STATUS"}}},
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

func TestV080AdministrationKeySchemasPreserveRawScalarCoercion(t *testing.T) {
	for _, args := range [][]any{
		{"CLUSTER.KEYSLOT", 42},
		{"FERRICSTORE.KEY_INFO", 42},
	} {
		command, err := buildNativeCommand(args)
		if err != nil {
			t.Fatal(err)
		}
		if command.opcode != nativeOpCommandExec {
			t.Fatalf("%s opcode = %#x, want COMMAND_EXEC", args[0], command.opcode)
		}
	}
}
