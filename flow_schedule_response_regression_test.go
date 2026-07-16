package ferricstore

import (
	"reflect"
	"strings"
	"testing"
)

func TestScheduleResultDecodesCanonicalServerView(t *testing.T) {
	raw := map[string]any{
		"id":                       "daily",
		"flow_id":                  "__ferricstore_schedule__:daily",
		"state":                    "active",
		"kind":                     "interval",
		"target":                   map[string]any{"type": "email", "id_prefix": "daily"},
		"timezone":                 "Etc/UTC",
		"overlap_policy":           "skip",
		"next_run_at_ms":           int64(100),
		"last_fire_at_ms":          int64(90),
		"fire_count":               int64(3),
		"attempts":                 int64(4),
		"max_fires":                int64(10),
		"end_at_ms":                int64(1_000),
		"last_target_id":           "daily:90:3",
		"last_overlap_at_ms":       int64(80),
		"last_overlap_target_id":   "daily:70:2",
		"last_overlap_reason":      "still_running",
		"last_skipped_at_ms":       int64(80),
		"skipped_count":            int64(2),
		"overlap_queued_due_at_ms": int64(95),
		"end_reason":               "max_fires",
	}

	got, err := scheduleResult(raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "daily" || got.FlowID != "__ferricstore_schedule__:daily" ||
		got.Status != "active" || got.Kind != "interval" || got.NextFireAtMS != 100 ||
		got.LastFireAtMS != 90 || got.Fires != 3 || got.Attempts != 4 ||
		got.MaxFires != 10 || got.EndAtMS != 1_000 || got.LastTargetID != "daily:90:3" ||
		got.LastOverlapAtMS != 80 || got.LastOverlapTargetID != "daily:70:2" ||
		got.LastOverlapReason != "still_running" || got.LastSkippedAtMS != 80 ||
		got.SkippedCount != 2 || got.OverlapQueuedDueAtMS != 95 || got.EndReason != "max_fires" {
		t.Fatalf("decoded schedule = %#v", got)
	}
	if !reflect.DeepEqual(got.Target, raw["target"]) || !reflect.DeepEqual(got.Raw, raw) {
		t.Fatalf("decoded schedule maps = %#v", got)
	}
}

func TestScheduleResultRetainsReleasedResponseAliases(t *testing.T) {
	got, err := scheduleResult(map[string]any{
		"id": "daily", "kind": "interval", "status": "active",
		"next_fire_at_ms": int64(100), "fires": int64(3),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "active" || got.NextFireAtMS != 100 || got.Fires != 3 {
		t.Fatalf("decoded legacy aliases = %#v", got)
	}
}

func TestScheduleResultRejectsInvalidRecordIntegrity(t *testing.T) {
	tests := []struct {
		name string
		raw  map[string]any
		want string
	}{
		{name: "empty", raw: map[string]any{}, want: "id"},
		{name: "blank id", raw: map[string]any{"id": " ", "kind": "interval", "state": "active"}, want: "id"},
		{name: "missing kind", raw: map[string]any{"id": "daily", "state": "active"}, want: "kind"},
		{name: "missing state", raw: map[string]any{"id": "daily", "kind": "interval"}, want: "state"},
		{name: "negative fire count", raw: map[string]any{"id": "daily", "kind": "interval", "state": "active", "fire_count": int64(-1)}, want: "fire_count"},
		{name: "negative attempts", raw: map[string]any{"id": "daily", "kind": "interval", "state": "active", "attempts": int64(-1)}, want: "attempts"},
		{name: "conflicting state alias", raw: map[string]any{"id": "daily", "kind": "interval", "state": "active", "status": "paused"}, want: "conflicting"},
		{name: "conflicting count alias", raw: map[string]any{"id": "daily", "kind": "interval", "state": "active", "fire_count": int64(2), "fires": int64(3)}, want: "conflicting"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := scheduleResult(test.raw, nil)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want text %q", err, test.want)
			}
		})
	}
}
