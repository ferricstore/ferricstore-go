package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestScheduleFireDueWithOptionsDecodesSchedulerSummary(t *testing.T) {
	t.Parallel()

	response := map[string]any{
		"claimed":          int64(3),
		"fired":            int64(1),
		"skipped":          int64(1),
		"errors":           []any{[]any{"schedule-bad", "target failed"}},
		"last_target_id":   "target-1",
		"last_skip_reason": "overlap",
	}
	exec := &fakeExecutor{value: response}
	client := NewClientWithExecutor(exec)
	result, err := client.ScheduleFireDueWithOptions(context.Background(), ScheduleFireDueOptions{
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
	if result.Claimed != 3 || result.Fired != 1 || result.Skipped != 1 ||
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

func TestLegacyScheduleFireDueExposesTypedSummaryFields(t *testing.T) {
	t.Parallel()

	exec := &fakeExecutor{value: map[string]any{
		"claimed": int64(1), "fired": int64(1), "skipped": int64(0), "errors": []any{},
		"last_target_id": "target-1",
	}}
	result, err := NewClientWithExecutor(exec).ScheduleFireDue(context.Background(), nil, "scheduler", nil, Int(1))
	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 1 || result.Fired != 1 || result.Skipped != 0 || result.LastTargetID != "target-1" || len(result.Errors) != 0 {
		t.Fatalf("legacy fire-due summary = %#v", result)
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
				"claimed": int64(-1), "fired": int64(0), "skipped": int64(0), "errors": []any{},
			},
		},
		{
			name: "count exceeds requested maximum",
			response: map[string]any{
				"claimed": int64(101), "fired": int64(101),
				"skipped": int64(0), "errors": []any{},
			},
		},
		{
			name: "inconsistent outcomes",
			response: map[string]any{
				"claimed": int64(2), "fired": int64(1), "skipped": int64(0), "errors": []any{},
			},
		},
		{
			name: "malformed error",
			response: map[string]any{
				"claimed": int64(1), "fired": int64(0), "skipped": int64(0), "errors": []any{[]any{"id"}},
			},
		},
		{
			name: "missing errors",
			response: map[string]any{
				"claimed": int64(0), "fired": int64(0), "skipped": int64(0),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := NewClientWithExecutor(&fakeExecutor{value: tt.response})
			if _, err := client.ScheduleFireDueWithOptions(context.Background(), ScheduleFireDueOptions{}); err == nil {
				t.Fatal("malformed scheduler summary was accepted")
			}
		})
	}
}

func TestScheduleFireDueRejectsInvalidLeaseBeforeTransport(t *testing.T) {
	t.Parallel()

	exec := &fakeExecutor{}
	_, err := NewClientWithExecutor(exec).ScheduleFireDueWithOptions(context.Background(), ScheduleFireDueOptions{LeaseMS: Int64(0)})
	if err == nil {
		t.Fatal("zero schedule lease succeeded")
	}
	if len(exec.calls) != 0 {
		t.Fatalf("invalid schedule lease reached transport: %#v", exec.calls)
	}
}
