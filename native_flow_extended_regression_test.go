package ferricstore

import (
	"reflect"
	"testing"
)

func TestNativeCreateManyBuildsDedicatedPayloadForExtendedItems(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"FLOW.CREATE_MANY", "AUTO",
		"TYPE", "order", "STATE", "queued", "NOW", int64(100), "RUN_AT", int64(100),
		"INDEPENDENT", "true",
		"ITEMS_EXT", 1,
		"flow-1", "-", []byte("payload"),
		1, "result", []byte("value"),
		1, "source", "ref-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != nativeOpFlowCreateMany || command.flags != 0 || command.laneID != 1 {
		t.Fatalf("extended create command = opcode %#x flags %#x lane %d", command.opcode, command.flags, command.laneID)
	}
	want := map[string]any{
		"type":        "order",
		"state":       "queued",
		"now_ms":      int64(100),
		"run_at_ms":   int64(100),
		"independent": true,
		"items": []any{map[string]any{
			"id": "flow-1", "payload": []byte("payload"),
			"values":     map[string]any{"result": []byte("value")},
			"value_refs": map[string]any{"source": "ref-1"},
		}},
	}
	if !reflect.DeepEqual(command.payload, want) {
		t.Fatalf("extended create payload = %#v, want %#v", command.payload, want)
	}
}

func TestNativeSpawnChildrenBuildsDedicatedPayloadForExtendedItems(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"FLOW.SPAWN_CHILDREN", "parent",
		"GROUP", "fanout", "WAIT", "all", "NOW", int64(100),
		"PARTITION", "tenant", "FENCING", int64(7),
		"WAIT_STATE", "waiting", "SUCCESS", "done", "FAILURE", "failed",
		"ITEMS_EXT", 1,
		"child", "-", "order", []byte("payload"),
		1, "result", []byte("value"),
		1, "source", "ref-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != 0x0220 || command.flags != 0 || command.laneID != 1 {
		t.Fatalf("extended spawn command = opcode %#x flags %#x lane %d", command.opcode, command.flags, command.laneID)
	}
	want := map[string]any{
		"id": "parent", "group_id": "fanout", "wait": "all", "now_ms": int64(100),
		"partition_key": "tenant", "fencing_token": int64(7),
		"wait_state": "waiting", "success": "done", "failure": "failed",
		"children": []any{map[string]any{
			"id": "child", "type": "order", "payload": []byte("payload"),
			"values":     map[string]any{"result": []byte("value")},
			"value_refs": map[string]any{"source": "ref-1"},
		}},
	}
	if !reflect.DeepEqual(command.payload, want) {
		t.Fatalf("extended spawn payload = %#v, want %#v", command.payload, want)
	}
}

func TestNativeExtendedFlowBuildersRejectTruncatedCounts(t *testing.T) {
	for _, args := range [][]any{
		{"FLOW.CREATE_MANY", "AUTO", "TYPE", "order", "STATE", "queued", "NOW", int64(1), "ITEMS_EXT", 1, "id", "-", []byte("payload"), 1},
		{"FLOW.SPAWN_CHILDREN", "parent", "GROUP", "group", "WAIT", "all", "NOW", int64(1), "ITEMS_EXT", 1, "child", "-", "order", []byte("payload"), 0},
	} {
		command, err := buildNativeCommand(args)
		if err == nil {
			t.Fatalf("truncated extended payload was accepted: %#v", command)
		}
	}
}
