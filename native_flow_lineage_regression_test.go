package ferricstore

import "testing"

func TestNativeStartAndClaimUsesCanonicalLineageFields(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"FLOW.START_AND_CLAIM", "flow-1",
		"TYPE", "order",
		"INITIAL_STATE", "created",
		"WORKER", "worker-1",
		"PARENT_FLOW_ID", "parent-1",
		"ROOT_FLOW_ID", "root-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != nativeOpFlowStartAndClaim {
		t.Fatalf("START_AND_CLAIM opcode = %#x, want %#x", command.opcode, nativeOpFlowStartAndClaim)
	}
	payload, ok := command.payload.(map[string]any)
	if !ok {
		t.Fatalf("START_AND_CLAIM payload = %T, want map", command.payload)
	}
	if got := payload["parent_flow_id"]; got != "parent-1" {
		t.Fatalf("parent_flow_id = %#v, want parent-1; payload %#v", got, payload)
	}
	if got := payload["root_flow_id"]; got != "root-1" {
		t.Fatalf("root_flow_id = %#v, want root-1; payload %#v", got, payload)
	}
	if _, exists := payload["parent_id"]; exists {
		t.Fatalf("obsolete parent_id emitted in payload %#v", payload)
	}
	if _, exists := payload["root_id"]; exists {
		t.Fatalf("obsolete root_id emitted in payload %#v", payload)
	}
}
