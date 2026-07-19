package ferricstore

import "testing"

func TestNativeClaimDuePreservesMultipleStateFilters(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"FLOW.CLAIM_DUE", "orders",
		"STATE", "ready",
		"STATE", "retry",
		"WORKER", "worker-1",
		"LEASE_MS", int64(30_000),
		"LIMIT", 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != nativeOpFlowClaimDue || command.flags != 0 {
		t.Fatalf("multiple-state CLAIM_DUE did not use typed opcode=%d flags=%#x", command.opcode, command.flags)
	}
	payload, ok := command.payload.(map[string]any)
	if !ok {
		t.Fatalf("typed payload = %T; want command map", command.payload)
	}
	states, ok := payload["states"].([]string)
	if !ok || len(states) != 2 || states[0] != "ready" || states[1] != "retry" {
		t.Fatalf("typed CLAIM_DUE states = %#v; want both state filters", payload["states"])
	}
}
