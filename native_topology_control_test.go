package ferricstore

import "testing"

func TestBuildNativeCommandUsesDedicatedShardsOpcode(t *testing.T) {
	command, err := buildNativeCommand([]any{"SHARDS"})
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != nativeOpShards || command.laneID != 0 {
		t.Fatalf("SHARDS command = opcode 0x%x lane %d; want opcode 0x%x lane 0", command.opcode, command.laneID, nativeOpShards)
	}
	payload, ok := command.payload.(map[string]any)
	if !ok || len(payload) != 0 {
		t.Fatalf("SHARDS payload = %#v; want empty map", command.payload)
	}
}

func TestBuildNativeCommandRejectsShardsArguments(t *testing.T) {
	if _, err := buildNativeCommand([]any{"SHARDS", "unexpected"}); err == nil {
		t.Fatal("SHARDS accepted unexpected arguments")
	}
}
