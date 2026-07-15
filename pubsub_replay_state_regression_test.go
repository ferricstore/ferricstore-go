package ferricstore

import "testing"

func TestEventReplayReplacesSupersededFlowWakeFilter(t *testing.T) {
	p := &PubSub{exec: &NativeExecutor{}}
	p.trackEventSubscription(nativeOpSubscribeEvents, map[string]any{
		"events": []any{"TOPOLOGY_CHANGED"},
	})
	for _, jobType := range []string{"email", "sms"} {
		p.trackEventSubscription(nativeOpSubscribeEvents, map[string]any{
			"events": []any{"FLOW_WAKE"},
			"flow_wake": map[string]any{
				"type": jobType,
			},
		})
	}

	if len(p.eventReplays) != 2 {
		t.Fatalf("event replay count = %d, want unrelated event plus latest FLOW_WAKE filter", len(p.eventReplays))
	}
	flowWake, ok := p.eventReplays[1].payload["flow_wake"].(map[string]any)
	if !ok || asString(flowWake["type"]) != "sms" {
		t.Fatalf("retained FLOW_WAKE filter = %#v, want latest sms filter", p.eventReplays[1].payload)
	}
}

func TestEventReplayEmptySubscribeMatchesServerNoop(t *testing.T) {
	p := &PubSub{exec: &NativeExecutor{}}
	p.trackEventSubscription(nativeOpSubscribeEvents, map[string]any{
		"events": []any{},
	})

	if len(p.eventReplays) != 0 {
		t.Fatalf("empty server-side no-op retained replay state: %#v", p.eventReplays)
	}
}

func TestPubSubReplayStateReleasesEmptyBackingStorage(t *testing.T) {
	p := &PubSub{exec: &NativeExecutor{}}
	p.trackPubSubCommand([]any{"SUBSCRIBE", "jobs", "alerts"})
	p.trackPubSubCommand([]any{"UNSUBSCRIBE"})
	if p.channels != nil {
		t.Fatalf("unsubscribe-all retained an empty channel map with capacity")
	}

	p.trackPubSubCommand([]any{"PSUBSCRIBE", "jobs:*", "alerts:*"})
	p.trackPubSubCommand([]any{"PUNSUBSCRIBE"})
	if p.patterns != nil {
		t.Fatalf("punsubscribe-all retained an empty pattern map with capacity")
	}

	replays := []pubSubEventReplay{{
		opcode:  nativeOpSubscribeEvents,
		payload: map[string]any{"events": []any{"FLOW_WAKE"}},
	}}
	if filtered := filterEventReplays(replays, []string{"FLOW_WAKE"}); filtered != nil {
		t.Fatalf("fully removed event replays retained backing storage: %#v", filtered)
	}
}
