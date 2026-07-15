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
	if command.opcode != nativeOpCommandExec || command.flags != 0 {
		t.Fatalf("multiple-state CLAIM_DUE used lossy compact opcode=%d flags=%#x", command.opcode, command.flags)
	}
	payload, ok := command.payload.(map[string]any)
	if !ok {
		t.Fatalf("fallback payload = %T; want command map", command.payload)
	}
	args, ok := payload["args"].([]any)
	if !ok || !containsSubsequence(args, []any{"STATE", "ready", "STATE", "retry"}) {
		t.Fatalf("fallback CLAIM_DUE args = %#v; want both state filters", payload["args"])
	}
}
