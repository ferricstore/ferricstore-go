package ferricstore

import "testing"

var (
	mergedFlowValuesSink map[string]any
	mergedFlowRefsSink   map[string]string
	mappedFlowValuesSink map[string]any
)

func TestV080EmptyPerItemNamedValueMergesDoNotAllocate(t *testing.T) {
	allocations := testing.AllocsPerRun(1_000, func() {
		mergedFlowValuesSink = mergeValues(nil, nil)
		mergedFlowRefsSink = mergeRefs(nil, nil)
	})
	if allocations != 0 {
		t.Fatalf("empty per-item named value merges allocate %.0f objects; want 0", allocations)
	}
}

func TestV080MappedBuiltInFlowValuesAvoidRedundantSortAllocation(t *testing.T) {
	client := &Client{codec: RawCodec{}}
	values := make(map[string]any, 128)
	for index := range 128 {
		values[string(rune(index+1))] = "value"
	}
	allocations := testing.AllocsPerRun(1_000, func() {
		out := make(map[string]any)
		if err := client.putEncodedFlowValues(out, values); err != nil {
			panic(err)
		}
		mappedFlowValuesSink = out
	})
	if allocations > 6 {
		t.Fatalf("mapped built-in Flow value encoding allocates %.0f objects; want at most 6", allocations)
	}
}
