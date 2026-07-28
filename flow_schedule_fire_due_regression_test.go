package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestScheduleFireDueDecodesSchedulerSummary(t *testing.T) {
	t.Parallel()

	response := map[string]any{
		"claimed":          int64(3),
		"fired":            int64(1),
		"skipped":          int64(1),
		"coalesced":        int64(1_000_000),
		"errors":           []any{[]any{"schedule-bad", "target failed"}},
		"last_target_id":   "target-1",
		"last_skip_reason": "overlap",
	}
	exec := &fakeExecutor{value: response}
	client := NewClientWithExecutor(exec)
	result, err := client.ScheduleFireDue(context.Background(), ScheduleFireDueOptions{
		NowMS:      Int64(10),
		Worker:     "scheduler",
		LeaseMS:    Int64(20),
		BlockMS:    Int64(30),
		Limit:      Int(4),
		DeadlineMS: Int64(40),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 3 || result.Fired != 1 || result.Skipped != 1 || result.Coalesced != 1_000_000 ||
		result.LastTargetID != "target-1" || result.LastSkipReason != "overlap" ||
		!reflect.DeepEqual(result.Errors, []ScheduleFireDueError{{ID: "schedule-bad", Reason: "target failed"}}) {
		t.Fatalf("decoded fire-due summary = %#v", result)
	}
	want := []any{
		"FLOW.SCHEDULE.FIRE_DUE", "NOW", int64(10), "WORKER", "scheduler",
		"LEASE_MS", int64(20), "BLOCK", int64(30), "LIMIT", 4, "DEADLINE_MS", int64(40),
	}
	if !reflect.DeepEqual(exec.calls, [][]any{want}) {
		t.Fatalf("fire-due command = %#v, want %#v", exec.calls, want)
	}
}

func TestScheduleFireDueKeepsLaterClaimFailureSeparateFromScheduleOutcomes(t *testing.T) {
	t.Parallel()

	response := map[string]any{
		"claimed":        int64(1),
		"fired":          int64(1),
		"skipped":        int64(0),
		"coalesced":      int64(0),
		"errors":         []any{},
		"claim_error":    "ERR claim unavailable",
		"last_target_id": "daily:1000:1",
	}

	result, err := NewClientWithExecutor(&fakeExecutor{value: response}).ScheduleFireDue(
		context.Background(), ScheduleFireDueOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.ClaimError != "ERR claim unavailable" {
		t.Fatalf("claim error = %q", result.ClaimError)
	}
}

func TestScheduleFireDueRejectsMalformedSchedulerSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		response map[string]any
	}{
		{
			name: "negative count",
			response: map[string]any{
				"claimed": int64(-1), "fired": int64(0), "skipped": int64(0), "coalesced": int64(0), "errors": []any{},
			},
		},
		{
			name: "count exceeds requested maximum",
			response: map[string]any{
				"claimed": int64(101), "fired": int64(101),
				"skipped": int64(0), "coalesced": int64(0), "errors": []any{},
			},
		},
		{
			name: "inconsistent outcomes",
			response: map[string]any{
				"claimed": int64(2), "fired": int64(1), "skipped": int64(0), "coalesced": int64(0), "errors": []any{},
			},
		},
		{
			name: "malformed error",
			response: map[string]any{
				"claimed": int64(1), "fired": int64(0), "skipped": int64(0), "coalesced": int64(0), "errors": []any{[]any{"id"}},
			},
		},
		{
			name: "missing errors",
			response: map[string]any{
				"claimed": int64(0), "fired": int64(0), "skipped": int64(0), "coalesced": int64(0),
			},
		},
		{
			name: "missing coalesced",
			response: map[string]any{
				"claimed": int64(0), "fired": int64(0), "skipped": int64(0), "errors": []any{},
			},
		},
		{
			name: "invalid claim error",
			response: map[string]any{
				"claimed": int64(0), "fired": int64(0), "skipped": int64(0), "coalesced": int64(0), "errors": []any{}, "claim_error": int64(7),
			},
		},
		{
			name: "negative coalesced",
			response: map[string]any{
				"claimed": int64(0), "fired": int64(0), "skipped": int64(0), "coalesced": int64(-1), "errors": []any{},
			},
		},
		{
			name: "coalesced exceeds exact integer range",
			response: map[string]any{
				"claimed": int64(0), "fired": int64(0), "skipped": int64(0), "coalesced": maxFlowExactIntegerV080 + 1, "errors": []any{},
			},
		},
		{
			name: "fired outcome missing target id",
			response: map[string]any{
				"claimed": int64(1), "fired": int64(1), "skipped": int64(0), "coalesced": int64(0), "errors": []any{},
			},
		},
		{
			name: "skipped outcome missing reason",
			response: map[string]any{
				"claimed": int64(1), "fired": int64(0), "skipped": int64(1), "coalesced": int64(0), "errors": []any{},
			},
		},
		{
			name: "stale target id without fired outcome",
			response: map[string]any{
				"claimed": int64(0), "fired": int64(0), "skipped": int64(0), "coalesced": int64(0), "errors": []any{}, "last_target_id": "target-1",
			},
		},
		{
			name: "stale skip reason without skipped outcome",
			response: map[string]any{
				"claimed": int64(0), "fired": int64(0), "skipped": int64(0), "coalesced": int64(0), "errors": []any{}, "last_skip_reason": "overlap",
			},
		},
		{
			name: "coalesced without successful outcome",
			response: map[string]any{
				"claimed": int64(1), "fired": int64(0), "skipped": int64(0), "coalesced": int64(1), "errors": []any{[]any{"schedule", "failed"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := NewClientWithExecutor(&fakeExecutor{value: tt.response})
			if _, err := client.ScheduleFireDue(context.Background(), ScheduleFireDueOptions{}); err == nil {
				t.Fatal("malformed scheduler summary was accepted")
			}
		})
	}
}

func TestScheduleFireDueRejectsInvalidLeaseBeforeTransport(t *testing.T) {
	t.Parallel()

	exec := &fakeExecutor{}
	_, err := NewClientWithExecutor(exec).ScheduleFireDue(context.Background(), ScheduleFireDueOptions{LeaseMS: Int64(0)})
	if err == nil {
		t.Fatal("zero schedule lease succeeded")
	}
	if len(exec.calls) != 0 {
		t.Fatalf("invalid schedule lease reached transport: %#v", exec.calls)
	}
}
