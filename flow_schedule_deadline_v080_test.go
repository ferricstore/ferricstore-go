package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestV080ScheduleUsesTypedDeadlineOptions(t *testing.T) {
	t.Parallel()

	exec := &fakeExecutor{values: []any{
		map[string]any{"id": "created", "kind": "one_shot", "status": "active"},
		map[string]any{"id": "get", "kind": "one_shot", "status": "active"},
		map[string]any{"id": "paused", "kind": "one_shot", "status": "paused"},
		[]any{},
	}}
	client := NewClientWithExecutor(exec)
	if _, err := client.ScheduleCreate(context.Background(), "created", ScheduleOptions{
		Target: map[string]any{"type": "job"}, DeadlineMS: Int64(100),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ScheduleGet(context.Background(), "get", Int64(101)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SchedulePauseWithOptions(context.Background(), "paused", ScheduleStatusOptions{
		NowMS: Int64(10), DeadlineMS: Int64(102),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ScheduleList(context.Background(), ScheduleListOptions{DeadlineMS: Int64(103)}); err != nil {
		t.Fatal(err)
	}

	want := [][]any{
		{"FLOW.SCHEDULE.CREATE", "created", "TARGET", map[string]any{"type": "job"}, "DEADLINE_MS", int64(100)},
		{"FLOW.SCHEDULE.GET", "get", "DEADLINE_MS", int64(101)},
		{"FLOW.SCHEDULE.PAUSE", "paused", "NOW", int64(10), "DEADLINE_MS", int64(102)},
		{"FLOW.SCHEDULE.LIST", "DEADLINE_MS", int64(103)},
	}
	if !reflect.DeepEqual(exec.calls, want) {
		t.Fatalf("schedule calls = %#v, want %#v", exec.calls, want)
	}
}

func TestV080ScheduleRejectsNegativeTypedDeadlinesBeforeTransport(t *testing.T) {
	t.Parallel()

	tests := []func(*Client) error{
		func(client *Client) error {
			_, err := client.ScheduleCreate(context.Background(), "schedule", ScheduleOptions{
				Target: map[string]any{"type": "job"}, DeadlineMS: Int64(-1),
			})
			return err
		},
		func(client *Client) error {
			_, err := client.ScheduleGet(context.Background(), "schedule", Int64(-1))
			return err
		},
		func(client *Client) error {
			_, err := client.ScheduleResumeWithOptions(context.Background(), "schedule", ScheduleStatusOptions{DeadlineMS: Int64(-1)})
			return err
		},
		func(client *Client) error {
			_, err := client.ScheduleList(context.Background(), ScheduleListOptions{DeadlineMS: Int64(-1)})
			return err
		},
	}
	for index, call := range tests {
		exec := &fakeExecutor{}
		if err := call(NewClientWithExecutor(exec)); err == nil {
			t.Fatalf("case %d accepted negative deadline", index)
		}
		if len(exec.calls) != 0 {
			t.Fatalf("case %d reached transport: %#v", index, exec.calls)
		}
	}
}
