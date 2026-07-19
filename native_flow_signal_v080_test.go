package ferricstore

import "testing"

func TestV080FlowSignalUsesDedicatedIDAndSignalSchema(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"FLOW.SIGNAL", "flow-1", "SIGNAL", "approved", "PARTITION", "tenant-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != nativeOpFlowSignal {
		t.Fatalf("FLOW.SIGNAL opcode = %#x, want %#x", command.opcode, nativeOpFlowSignal)
	}
	payload, ok := command.payload.(map[string]any)
	if !ok {
		t.Fatalf("FLOW.SIGNAL payload = %T", command.payload)
	}
	if asString(payload["id"]) != "flow-1" || asString(payload["signal"]) != "approved" ||
		asString(payload["partition_key"]) != "tenant-1" {
		t.Fatalf("FLOW.SIGNAL payload = %#v", payload)
	}
}

func TestV080FlowSignalFullSchemaStaysOnDedicatedOpcode(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"FLOW.SIGNAL", "flow-1",
		"SIGNAL", "approved",
		"IDEMPOTENCY", "signal-7",
		"IF_STATE", "queued",
		"IF_STATE", "waiting",
		"TRANSITION_TO", "completed",
		"RUN_AT", int64(200),
		"NOW", int64(100),
	})
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != nativeOpFlowSignal {
		t.Fatalf("FLOW.SIGNAL opcode = %#x, want dedicated %#x", command.opcode, nativeOpFlowSignal)
	}
	payload, ok := command.payload.(map[string]any)
	if !ok {
		t.Fatalf("FLOW.SIGNAL payload = %T", command.payload)
	}
	wantStates := []string{"queued", "waiting"}
	states, ok := payload["if_state"].([]string)
	if !ok || len(states) != len(wantStates) || states[0] != wantStates[0] || states[1] != wantStates[1] {
		t.Fatalf("FLOW.SIGNAL if_state = %#v, want %#v", payload["if_state"], wantStates)
	}
	if asString(payload["idempotency_key"]) != "signal-7" ||
		asString(payload["transition_to"]) != "completed" ||
		asInt64(payload["run_at_ms"]) != 200 || asInt64(payload["now_ms"]) != 100 {
		t.Fatalf("FLOW.SIGNAL payload = %#v", payload)
	}
}
