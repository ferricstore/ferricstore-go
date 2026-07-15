package ferricstore

import "testing"

func TestFlowValueMGetRoutingTreatsMiddleMaxBytesTokenAsReference(t *testing.T) {
	first, last := differentSlotKeys(t)
	args := []any{"FLOW.VALUE.MGET", first, "MAX_BYTES", last}

	if key, ok := routingKeyForCommand(args); ok || key != nil {
		t.Fatalf("ambiguous FLOW.VALUE.MGET routed with key %#v; want safe control path", key)
	}
}

func TestFlowValueMGetRoutingRecognizesTrailingMaxBytesOption(t *testing.T) {
	first := "value:{tenant}:first"
	second := "value:{tenant}:second"
	args := []any{"FLOW.VALUE.MGET", first, second, "MAX_BYTES", int64(1024)}

	key, ok := routingKeyForCommand(args)
	if !ok || asString(key) != first {
		t.Fatalf("FLOW.VALUE.MGET route = %#v, %t; want %q, true", key, ok, first)
	}
}
