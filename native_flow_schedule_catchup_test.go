package ferricstore

import "testing"

func TestNativeScheduleCreateMapsCatchupPolicy(t *testing.T) {
	command, handled, err := buildFlowScheduleCreateNative([]any{
		"daily",
		"KIND", "interval",
		"EVERY_MS", int64(1_000),
		"CATCHUP_POLICY", "fire_once",
		"TARGET", map[string]any{"type": "email"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("CATCHUP_POLICY forced native schedule fallback")
	}
	payload, ok := command.payload.(map[string]any)
	if !ok {
		t.Fatalf("native schedule payload = %T", command.payload)
	}
	if got := payload["catchup_policy"]; got != "fire_once" {
		t.Fatalf("native catchup_policy = %#v", got)
	}
}
