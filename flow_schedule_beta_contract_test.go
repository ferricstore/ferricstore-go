package ferricstore

import (
	"context"
	"testing"
)

func TestScheduleBetaContractUsesDistinctCanonicalTypes(t *testing.T) {
	t.Parallel()

	recordRaw := scheduleResponseWithMany(map[string]any{
		"state": "failed", "end_reason": "planning_failed",
		"last_planning_error": "ERR invalid recurrence",
	})
	record, err := scheduleRecord(recordRaw, nil)
	if err != nil {
		t.Fatal(err)
	}
	if record.LastPlanningError != "ERR invalid recurrence" {
		t.Fatalf("planning error = %q", record.LastPlanningError)
	}

	terminalRaw := scheduleResponseWithMany(map[string]any{
		"state": "completed", "next_run_at_ms": nil,
	})
	terminal, err := scheduleRecord(terminalRaw, nil)
	if err != nil {
		t.Fatal(err)
	}
	if terminal.NextRunAtMS != nil {
		t.Fatalf("terminal next run = %#v, want nil", terminal.NextRunAtMS)
	}

	epoch, err := scheduleRecord(scheduleResponseWith("next_run_at_ms", int64(0)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if epoch.NextRunAtMS == nil || *epoch.NextRunAtMS != 0 {
		t.Fatalf("epoch next run = %#v, want pointer to zero", epoch.NextRunAtMS)
	}

	fireClient := NewClientWithExecutor(&fakeExecutor{value: map[string]any{
		"fired": int64(1), "target_id": "daily:1000:1", "schedule": canonicalScheduleResponse(),
	}})
	fire, err := fireClient.ScheduleFire(context.Background(), "daily", ScheduleFireOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if fire.TargetID != "daily:1000:1" {
		t.Fatalf("fire result = %#v", fire)
	}

	dueClient := NewClientWithExecutor(&fakeExecutor{value: map[string]any{
		"claimed": int64(0), "fired": int64(0), "skipped": int64(0),
		"coalesced": int64(0), "errors": []any{},
	}})
	if _, err := dueClient.ScheduleFireDue(context.Background(), ScheduleFireDueOptions{}); err != nil {
		t.Fatal(err)
	}

	deleteClient := NewClientWithExecutor(&fakeExecutor{value: "OK"})
	if err := deleteClient.ScheduleDelete(context.Background(), "daily", ScheduleStatusOptions{}); err != nil {
		t.Fatal(err)
	}
}
