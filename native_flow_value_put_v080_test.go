package ferricstore

import "testing"

func TestV080FlowValuePutUsesDedicatedMapSchema(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"FLOW.VALUE.PUT", []byte("payload"),
		"PARTITION", "tenant-1",
		"OWNER_FLOW_ID", "flow-1",
		"NAME", "result",
		"OVERRIDE", "true",
		"TTL", int64(60_000),
		"NOW", int64(100),
	})
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != 0x020B {
		t.Fatalf("FLOW.VALUE.PUT opcode = %#x, want dedicated 0x020b", command.opcode)
	}
	payload, ok := command.payload.(map[string]any)
	if !ok {
		t.Fatalf("FLOW.VALUE.PUT payload = %T, want map", command.payload)
	}
	if string(payload["value"].([]byte)) != "payload" ||
		asString(payload["partition_key"]) != "tenant-1" ||
		asString(payload["owner_flow_id"]) != "flow-1" ||
		asString(payload["name"]) != "result" ||
		payload["override"] != true ||
		asInt64(payload["ttl_ms"]) != 60_000 ||
		asInt64(payload["now_ms"]) != 100 {
		t.Fatalf("FLOW.VALUE.PUT payload = %#v", payload)
	}
}
