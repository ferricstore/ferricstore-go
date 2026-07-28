package ferricstore

import (
	"reflect"
	"strings"
	"testing"
)

func TestScheduleRecordDecodesCanonicalServerView(t *testing.T) {
	raw := map[string]any{
		"id":                       "daily",
		"flow_id":                  "__ferricstore_schedule__:daily",
		"state":                    "active",
		"kind":                     "interval",
		"target":                   map[string]any{"type": "email", "id_prefix": "daily"},
		"timezone":                 "Etc/UTC",
		"catchup_policy":           "fire_once",
		"coalesced_count":          int64(12),
		"last_catchup_at_ms":       int64(85),
		"last_coalesced_count":     int64(4),
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

	got, err := scheduleRecord(raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "daily" || got.FlowID != "__ferricstore_schedule__:daily" ||
		got.State != "active" || got.Kind != "interval" || scheduleInt64(got.NextRunAtMS) != 100 ||
		scheduleInt64(got.LastFireAtMS) != 90 || got.FireCount != 3 || got.Attempts != 4 ||
		got.CatchupPolicy != "fire_once" || got.CoalescedCount != 12 ||
		scheduleInt64(got.LastCatchupAtMS) != 85 || got.LastCoalescedCount != 4 ||
		scheduleInt64(got.MaxFires) != 10 || scheduleInt64(got.EndAtMS) != 1_000 || got.LastTargetID != "daily:90:3" ||
		scheduleInt64(got.LastOverlapAtMS) != 80 || got.LastOverlapTargetID != "daily:70:2" ||
		got.LastOverlapReason != "still_running" || scheduleInt64(got.LastSkippedAtMS) != 80 ||
		got.SkippedCount != 2 || scheduleInt64(got.OverlapQueuedDueAtMS) != 95 || got.EndReason != "max_fires" {
		t.Fatalf("decoded schedule = %#v", got)
	}
	if !reflect.DeepEqual(got.Target, raw["target"]) || !reflect.DeepEqual(got.Raw, raw) {
		t.Fatalf("decoded schedule maps = %#v", got)
	}
}

func scheduleInt64(value *int64) int64 {
	if value == nil {
		return -1
	}
	return *value
}

func TestScheduleRecordAcceptsLeasedRunningState(t *testing.T) {
	raw := scheduleResponseWith("state", "running")
	got, err := scheduleRecord(raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "running" {
		t.Fatalf("status = %q, want running", got.State)
	}
}

func TestScheduleRecordRejectsNonCanonicalResponseAliases(t *testing.T) {
	raw := canonicalScheduleResponse()
	delete(raw, "state")
	raw["status"] = "active"
	if _, err := scheduleRecord(raw, nil); err == nil || !strings.Contains(err.Error(), "state") {
		t.Fatalf("state alias error = %v", err)
	}

	raw = canonicalScheduleResponse()
	delete(raw, "fire_count")
	raw["fires"] = int64(3)
	if _, err := scheduleRecord(raw, nil); err == nil || !strings.Contains(err.Error(), "fire_count") {
		t.Fatalf("fire count alias error = %v", err)
	}
}

func TestScheduleRecordRejectsInvalidRecordIntegrity(t *testing.T) {
	tests := []struct {
		name string
		raw  map[string]any
		want string
	}{
		{name: "empty", raw: map[string]any{}, want: "id"},
		{name: "blank id", raw: scheduleResponseWith("id", " "), want: "id"},
		{name: "missing kind", raw: scheduleResponseWithout("kind"), want: "kind"},
		{name: "missing state", raw: scheduleResponseWithout("state"), want: "state"},
		{name: "missing interval catch-up policy", raw: scheduleResponseWithout("catchup_policy"), want: "catchup_policy"},
		{name: "invalid interval catch-up policy", raw: scheduleResponseWith("catchup_policy", "replay_all"), want: "catchup_policy"},
		{name: "catch-up policy on cron", raw: scheduleResponseWithMany(map[string]any{"kind": "cron", "catchup_policy": "fire_once"}), want: "catchup_policy"},
		{name: "invalid kind", raw: scheduleResponseWith("kind", "weekly"), want: "kind"},
		{name: "invalid state", raw: scheduleResponseWith("state", "waiting"), want: "state"},
		{name: "missing target", raw: scheduleResponseWithout("target"), want: "target"},
		{name: "target missing type", raw: scheduleResponseWith("target", map[string]any{}), want: "target type"},
		{name: "invalid flow id", raw: scheduleResponseWith("flow_id", int64(7)), want: "flow_id"},
		{name: "empty optional target id", raw: scheduleResponseWith("last_target_id", ""), want: "last_target_id"},
		{name: "empty nullable catch-up policy", raw: scheduleResponseWithMany(map[string]any{"kind": "one_shot", "catchup_policy": ""}), want: "catchup_policy"},
		{name: "missing overlap policy", raw: scheduleResponseWithout("overlap_policy"), want: "overlap_policy"},
		{name: "negative fire count", raw: scheduleResponseWith("fire_count", int64(-1)), want: "fire_count"},
		{name: "negative attempts", raw: scheduleResponseWith("attempts", int64(-1)), want: "attempts"},
		{name: "fire count exceeds exact range", raw: scheduleResponseWith("fire_count", maxFlowExactIntegerV080+1), want: "fire_count"},
		{name: "missing coalesced count", raw: scheduleResponseWithout("coalesced_count"), want: "coalesced_count"},
		{name: "non-interval coalesced count", raw: scheduleResponseWithMany(map[string]any{"kind": "one_shot", "catchup_policy": nil, "coalesced_count": int64(1)}), want: "coalesced_count"},
		{name: "latest coalesced exceeds cumulative", raw: scheduleResponseWithMany(map[string]any{"coalesced_count": int64(3), "last_coalesced_count": int64(4), "last_catchup_at_ms": int64(100)}), want: "last_coalesced_count"},
		{name: "latest coalesced missing catch-up time", raw: scheduleResponseWithMany(map[string]any{"coalesced_count": int64(1), "last_coalesced_count": int64(1)}), want: "last_catchup_at_ms"},
		{name: "non-interval epoch catch-up time", raw: scheduleResponseWithMany(map[string]any{"kind": "one_shot", "catchup_policy": nil, "last_catchup_at_ms": int64(0)}), want: "last_catchup_at_ms"},
		{name: "epoch catch-up time without coalescing", raw: scheduleResponseWith("last_catchup_at_ms", int64(0)), want: "last_catchup_at_ms"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := scheduleRecord(test.raw, nil)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want text %q", err, test.want)
			}
		})
	}
}

func canonicalScheduleResponse() map[string]any {
	return map[string]any{
		"id": "daily", "flow_id": "__ferricstore_schedule__:daily",
		"kind": "interval", "state": "active",
		"target":         map[string]any{"id_prefix": "daily", "type": "task", "state": "queued"},
		"catchup_policy": "fire_once", "overlap_policy": "allow",
		"next_run_at_ms": int64(100), "fire_count": int64(0), "attempts": int64(0),
		"skipped_count": int64(0), "coalesced_count": int64(0),
		"last_coalesced_count": int64(0),
	}
}

func scheduleResponseWith(field string, value any) map[string]any {
	raw := canonicalScheduleResponse()
	raw[field] = value
	return raw
}

func scheduleResponseWithMany(values map[string]any) map[string]any {
	raw := canonicalScheduleResponse()
	for field, value := range values {
		raw[field] = value
	}
	return raw
}

func scheduleResponseWithout(field string) map[string]any {
	raw := canonicalScheduleResponse()
	delete(raw, field)
	return raw
}

func oneShotScheduleResponse(id, state string) map[string]any {
	return scheduleResponseWithMany(map[string]any{
		"id": id, "kind": "one_shot", "state": state, "catchup_policy": nil,
	})
}
