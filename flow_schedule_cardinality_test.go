package ferricstore

import (
	"context"
	"strings"
	"testing"
)

func TestScheduleFireDueCannotExceedRequestedLimit(t *testing.T) {
	client := NewClientWithExecutor(&fakeExecutor{value: map[string]any{
		"claimed": int64(2), "fired": int64(2), "skipped": int64(0), "errors": []any{},
	}})
	if _, err := client.ScheduleFireDueWithOptions(context.Background(), ScheduleFireDueOptions{
		Limit: Int(1),
	}); err == nil || !strings.Contains(err.Error(), "claimed 2 schedules, limit is 1") {
		t.Fatalf("fire-due cardinality error = %v", err)
	}
}

func TestScheduleListCannotExceedRequestedCount(t *testing.T) {
	client := NewClientWithExecutor(&fakeExecutor{value: []any{
		map[string]any{"id": "one", "kind": "interval", "state": "active"},
		map[string]any{"id": "two", "kind": "interval", "state": "active"},
	}})
	if _, err := client.ScheduleList(context.Background(), ScheduleListOptions{
		Count: Int(1),
	}); err == nil || !strings.Contains(err.Error(), "returned 2 items, limit is 1") {
		t.Fatalf("schedule-list cardinality error = %v", err)
	}
}

func TestScheduleResponsesCannotExceedV080DefaultLimit(t *testing.T) {
	client := NewClientWithExecutor(&fakeExecutor{value: map[string]any{
		"claimed": int64(101), "fired": int64(101), "skipped": int64(0), "errors": []any{},
	}})
	if _, err := client.ScheduleFireDueWithOptions(context.Background(), ScheduleFireDueOptions{}); err == nil ||
		!strings.Contains(err.Error(), "claimed 101 schedules, limit is 100") {
		t.Fatalf("fire-due default cardinality error = %v", err)
	}

	items := make([]any, 101)
	for index := range items {
		items[index] = map[string]any{}
	}
	client = NewClientWithExecutor(&fakeExecutor{value: items})
	if _, err := client.ScheduleList(context.Background(), ScheduleListOptions{}); err == nil ||
		!strings.Contains(err.Error(), "returned 101 items, limit is 100") {
		t.Fatalf("schedule-list default cardinality error = %v", err)
	}
}

func TestScheduleFireDueRejectsTooManyErrorsBeforeItemDecoding(t *testing.T) {
	client := NewClientWithExecutor(&fakeExecutor{value: map[string]any{
		"claimed": int64(1), "fired": int64(0), "skipped": int64(0),
		"errors": []any{"malformed", "malformed"},
	}})
	_, err := client.ScheduleFireDueWithOptions(context.Background(), ScheduleFireDueOptions{Limit: Int(1)})
	if err == nil || !strings.Contains(err.Error(), "returned 2 errors for 1 claimed") {
		t.Fatalf("error cardinality = %v", err)
	}
}
