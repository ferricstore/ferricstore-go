package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestScheduleFireDecodesManualFireEnvelope(t *testing.T) {
	t.Parallel()

	exec := &fakeExecutor{value: map[string]any{
		"fired":     int64(1),
		"target_id": "target-1",
		"schedule":  scheduleResponseWith("id", "schedule"),
	}}
	client := NewClientWithExecutor(exec)
	result, err := client.ScheduleFire(context.Background(), "schedule", ScheduleFireOptions{
		NowMS: Int64(10), FireAtMS: Int64(20), DeadlineMS: Int64(30),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Fired != 1 || result.Skipped != 0 || result.TargetID != "target-1" ||
		result.Schedule.ID != "schedule" || result.Schedule.Kind != "interval" || result.Schedule.State != "active" {
		t.Fatalf("decoded manual fire result = %#v", result)
	}
	want := []any{
		"FLOW.SCHEDULE.FIRE", "schedule", "NOW", int64(10), "FIRE_AT_MS", int64(20), "DEADLINE_MS", int64(30),
	}
	if !reflect.DeepEqual(exec.calls, [][]any{want}) {
		t.Fatalf("manual fire command = %#v, want %#v", exec.calls, want)
	}

	command, err := buildNativeCommand(want)
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != nativeOpFlowScheduleFire {
		t.Fatalf("manual fire opcode = %#x, want %#x", command.opcode, nativeOpFlowScheduleFire)
	}
	payload, err := nativeMap(command.payload)
	if err != nil {
		t.Fatal(err)
	}
	if payload["id"] != "schedule" || payload["fire_at_ms"] != int64(20) || payload["deadline_ms"] != int64(30) {
		t.Fatalf("manual fire native payload = %#v", payload)
	}
}

func TestScheduleFireRejectsMalformedManualFireEnvelope(t *testing.T) {
	t.Parallel()

	tests := []map[string]any{
		{"fired": int64(1), "skipped": int64(1), "target_id": "target", "schedule": map[string]any{}},
		{"fired": int64(-1), "schedule": map[string]any{}},
		{"fired": int64(1), "target_id": "target"},
		{"fired": int64(1), "schedule": map[string]any{}},
		{"fired": int64(0), "skipped": int64(1), "schedule": map[string]any{}},
		{"fired": int64(0), "skipped": int64(1), "target_id": "target", "reason": "overlap", "schedule": scheduleResponseWith("id", "schedule")},
		{"fired": int64(1), "target_id": "target", "reason": "stale", "schedule": scheduleResponseWith("id", "schedule")},
	}
	for index, response := range tests {
		client := NewClientWithExecutor(&fakeExecutor{value: response})
		if _, err := client.ScheduleFire(context.Background(), "schedule", ScheduleFireOptions{}); err == nil {
			t.Fatalf("malformed manual fire response %d was accepted", index)
		}
	}
}

func TestScheduleFireOptionsRejectInvalidTimesBeforeTransport(t *testing.T) {
	t.Parallel()

	exec := &fakeExecutor{}
	_, err := NewClientWithExecutor(exec).ScheduleFire(context.Background(), "schedule", ScheduleFireOptions{
		FireAtMS: Int64(-1),
	})
	if err == nil {
		t.Fatal("negative manual fire time succeeded")
	}
	if len(exec.calls) != 0 {
		t.Fatalf("invalid manual fire reached transport: %#v", exec.calls)
	}
}
