package ferricstore

import "testing"

func TestNativeFlowBooleanGrammarKeepsDirectFastPaths(t *testing.T) {
	for _, test := range []struct {
		value any
		want  bool
	}{
		{value: "ON", want: true},
		{value: "yes", want: true},
		{value: []byte("off"), want: false},
		{value: "NO", want: false},
	} {
		schedule, err := buildNativeCommand([]any{
			"FLOW.SCHEDULE.CREATE", "schedule-1", "OVERWRITE", test.value,
		})
		if err != nil {
			t.Fatalf("schedule boolean %q: %v", test.value, err)
		}
		if schedule.opcode != nativeOpFlowScheduleCreate {
			t.Fatalf("schedule boolean %q fell back to opcode %#x", test.value, schedule.opcode)
		}
		payload := schedule.payload.(map[string]any)
		if got, ok := payload["overwrite"].(bool); !ok || got != test.want {
			t.Fatalf("schedule boolean %q = %#v, want %t", test.value, payload["overwrite"], test.want)
		}

		claim, err := buildNativeCommand([]any{
			"FLOW.CLAIM_DUE", "email", "WORKER", "worker-1", "LEASE_MS", int64(30_000),
			"LIMIT", int64(1), "RETURN", "JOBS_COMPACT", "RECLAIM_EXPIRED", test.value,
		})
		if err != nil {
			t.Fatalf("claim boolean %q: %v", test.value, err)
		}
		if claim.opcode != nativeOpFlowClaimDue || claim.flags != nativeFlagCustomPayload {
			t.Fatalf("claim boolean %q lost compact fast path: %#v", test.value, claim)
		}
	}
}

func TestNativeFlowBooleanParsingDoesNotAllocate(t *testing.T) {
	value := []byte("ON")
	if allocations := testing.AllocsPerRun(1_000, func() {
		parsed, err := nativeFlowBool(value)
		if err != nil || !parsed {
			panic("unexpected boolean result")
		}
	}); allocations != 0 {
		t.Fatalf("native flow boolean allocations = %v, want 0", allocations)
	}
}
