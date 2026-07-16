package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestScheduleCreateRejectsAmbiguousExtraOptionsBeforeTransport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		extra map[string]any
	}{
		{
			name:  "typed field collision",
			extra: map[string]any{" target ": map[string]any{"type": "other"}},
		},
		{
			name:  "normalized duplicate",
			extra: map[string]any{"deadline_ms": int64(1), " DEADLINE_MS ": int64(2)},
		},
		{
			name:  "invalid deadline",
			extra: map[string]any{"deadline_ms": int64(-1)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			exec := &fakeExecutor{value: map[string]any{"id": "schedule", "kind": "one_shot", "status": "active"}}
			_, err := NewClientWithExecutor(exec).ScheduleCreate(context.Background(), "schedule", ScheduleOptions{
				Target:       map[string]any{"type": "email"},
				ExtraOptions: tt.extra,
			})
			if err == nil {
				t.Fatal("ambiguous schedule extra options succeeded")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid extra options reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestScheduleCreateCanonicalizesAndSortsExtraOptions(t *testing.T) {
	t.Parallel()

	exec := &fakeExecutor{value: map[string]any{"id": "schedule", "kind": "one_shot", "status": "active"}}
	_, err := NewClientWithExecutor(exec).ScheduleCreate(context.Background(), "schedule", ScheduleOptions{
		Target: map[string]any{"type": "email"},
		ExtraOptions: map[string]any{
			" z_future ":    "last",
			"a_future":      "first",
			" deadline_ms ": int64(42),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []any{
		"FLOW.SCHEDULE.CREATE", "schedule", "TARGET", map[string]any{"type": "email"},
		"A_FUTURE", "first", "DEADLINE_MS", int64(42), "Z_FUTURE", "last",
	}
	if !reflect.DeepEqual(exec.calls, [][]any{want}) {
		t.Fatalf("schedule command = %#v, want %#v", exec.calls, want)
	}
}
