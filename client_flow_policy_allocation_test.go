package ferricstore

import "testing"

func TestValidateAppliedPolicyReusesNormalizedSnapshot(t *testing.T) {
	retry := RetryPolicy{
		MaxRetries:  3,
		Backoff:     "exponential",
		BaseMS:      10,
		MaxMS:       1_000,
		JitterPct:   25,
		ExhaustedTo: "failed",
	}
	policy := map[string]any{
		"retry": map[string]any{
			"max_retries":  int64(3),
			"exhausted_to": "failed",
			"backoff": map[string]any{
				"kind": "exponential", "base_ms": int64(10),
				"max_ms": int64(1_000), "jitter_pct": int64(25),
			},
		},
		"states": map[string]any{
			"queued": map[string]any{
				"mode": "fifo",
				"retry": map[string]any{
					"max_retries":  int64(3),
					"exhausted_to": "failed",
					"backoff": map[string]any{
						"kind": "exponential", "base_ms": int64(10),
						"max_ms": int64(1_000), "jitter_pct": int64(25),
					},
				},
			},
		},
	}
	options := PolicyOptions{
		Retry: &retry,
		StatePolicies: map[string]FlowStatePolicy{
			"queued": {Mode: FlowStateModeFIFO, Retry: &retry},
		},
	}

	allocations := testing.AllocsPerRun(1_000, func() {
		if err := validateAppliedPolicy(policy, options); err != nil {
			panic(err)
		}
	})
	if allocations > 1 {
		t.Fatalf(
			"validating an already-normalized policy allocated %.0f times; want at most 1",
			allocations,
		)
	}
}

func BenchmarkValidateAppliedPolicySnapshot(b *testing.B) {
	retry := RetryPolicy{MaxRetries: 3}
	policy := map[string]any{
		"retry": map[string]any{"max_retries": int64(3)},
	}
	options := PolicyOptions{Retry: &retry}
	b.ReportAllocs()
	for b.Loop() {
		if err := validateAppliedPolicy(policy, options); err != nil {
			b.Fatal(err)
		}
	}
}
