package ferricstore

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

func TestNativeEventRejectsMalformedFields(t *testing.T) {
	tests := []struct {
		name  string
		value map[string]any
	}{
		{name: "missing event name", value: map[string]any{"payload": map[string]any{}}},
		{name: "non-text event name", value: map[string]any{"event": int64(7)}},
		{name: "invalid timestamp", value: map[string]any{"event": "FLOW_WAKE", "at_ms": "soon"}},
		{name: "invalid payload", value: map[string]any{"event": "FLOW_WAKE", "payload": int64(7)}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if event, err := nativeEventFromValue(tc.value); err == nil {
				t.Fatalf("accepted malformed native event as %#v", event)
			}
		})
	}
}

func TestGoAwayEventAllowsControlPayloadWithoutEventEnvelope(t *testing.T) {
	event, err := nativeEventFromServerValue(nativeServerEvent{
		opcode: nativeOpGoAway,
		value:  map[string]any{"reason": "maintenance"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if event.Name != "GOAWAY" || event.Payload["reason"] != "maintenance" {
		t.Fatalf("unexpected GOAWAY event: %#v", event)
	}
}

func TestPubSubMessageParserContainsEmptyNestedArray(t *testing.T) {
	raw := []any{[]any{}}
	message := pubSubMessageFromNative(raw)
	if message.Kind != "" {
		t.Fatalf("malformed pubsub kind = %q, want empty", message.Kind)
	}
	if message.Raw == nil || message.Payload == nil {
		t.Fatalf("malformed pubsub payload was not preserved: %#v", message)
	}
}

func TestEventSubscriptionRejectsMalformedNameLists(t *testing.T) {
	listener, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any {
		return map[string]any{
			"subscribed": []any{"FLOW_WAKE", int64(7)},
			"supported":  []any{"FLOW_WAKE"},
		}
	})
	pubsub := NewPubSub(
		listener.Addr().String(),
		WithNativeTimeout(time.Second),
		WithNativeHeartbeat(0, 0),
		WithNativeReconnect(0),
	)
	defer func() { _ = pubsub.Close() }()

	if _, err := pubsub.SubscribeEvents(context.Background(), "FLOW_WAKE"); err == nil {
		t.Fatal("event subscription accepted a non-text subscribed event name")
	}
	if len(pubsub.eventReplays) != 0 {
		t.Fatalf("malformed acknowledgement changed replay state: %#v", pubsub.eventReplays)
	}
}

func TestFlowWakeSubscriptionRejectsInvalidFiltersBeforeTransport(t *testing.T) {
	tests := []struct {
		name string
		opt  FlowWakeSubscriptionOptions
		want string
	}{
		{name: "missing type", opt: FlowWakeSubscriptionOptions{}, want: "type"},
		{name: "empty states", opt: FlowWakeSubscriptionOptions{Type: "email", States: []string{}}, want: "states"},
		{name: "empty state", opt: FlowWakeSubscriptionOptions{Type: "email", States: []string{"queued", ""}}, want: "states"},
		{name: "ANY with explicit state", opt: FlowWakeSubscriptionOptions{Type: "email", States: []string{"ANY", "queued"}}, want: "ANY"},
		{name: "empty partitions", opt: FlowWakeSubscriptionOptions{Type: "email", PartitionKeys: []string{}}, want: "partition"},
		{name: "empty partition", opt: FlowWakeSubscriptionOptions{Type: "email", PartitionKeys: []string{"tenant", ""}}, want: "partition"},
		{name: "negative priority", opt: FlowWakeSubscriptionOptions{Type: "email", Priority: Int64(-1)}, want: "priority"},
		{name: "large priority", opt: FlowWakeSubscriptionOptions{Type: "email", Priority: Int64(3)}, want: "priority"},
		{name: "zero limit", opt: FlowWakeSubscriptionOptions{Type: "email", Limit: Int(0)}, want: "limit"},
		{name: "negative limit", opt: FlowWakeSubscriptionOptions{Type: "email", Limit: Int(-1)}, want: "limit"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exec := newNativeExecutor(defaultNativeOptions("127.0.0.1:6388", false))
			_ = exec.Close()
			_, err := (&PubSub{exec: exec}).SubscribeFlowWake(context.Background(), tc.opt)
			if err == nil || errors.Is(err, net.ErrClosed) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("invalid FLOW_WAKE filter error = %v; want local %q validation", err, tc.want)
			}
		})
	}
}
