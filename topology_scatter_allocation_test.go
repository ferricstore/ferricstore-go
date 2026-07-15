package ferricstore

import "testing"

var topologyScatterValuesSink []any

func TestTopologyCountScatterDoesNotAllocateMGetValueSlots(t *testing.T) {
	if allocations := testing.AllocsPerRun(1000, func() {
		topologyScatterValuesSink = topologyScatterValueSlots("DEL", 10_000)
	}); allocations != 0 {
		t.Fatalf("DEL scatter value-slot allocations = %v; want 0", allocations)
	}
	if values := topologyScatterValueSlots("MGET", 10_000); len(values) != 10_000 {
		t.Fatalf("MGET scatter value slots = %d; want 10000", len(values))
	}
}
