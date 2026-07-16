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
		{name: "negative timestamp", value: map[string]any{"event": "FLOW_WAKE", "at_ms": int64(-1)}},
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

func TestParsePubSubMessageRejectsMalformedServerEvents(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{name: "non array", value: "message"},
		{name: "empty array", value: []any{}},
		{name: "unknown kind", value: []any{"unknown", "jobs"}},
		{name: "non text kind", value: []any{int64(1), "jobs", "body"}},
		{name: "short message", value: []any{"message", "jobs"}},
		{name: "long message", value: []any{"message", "jobs", "body", "extra"}},
		{name: "non text channel", value: []any{"message", int64(7), "body"}},
		{name: "short pattern message", value: []any{"pmessage", "jobs:*", "jobs"}},
		{name: "invalid pattern", value: []any{"pmessage", int64(7), "jobs", "body"}},
		{name: "invalid acknowledgement count", value: []any{"subscribe", "jobs", "many"}},
		{name: "negative acknowledgement count", value: []any{"unsubscribe", "jobs", int64(-1)}},
		{name: "empty nested acknowledgement", value: []any{[]any{}}},
		{name: "mixed nested acknowledgements", value: []any{
			[]any{"subscribe", "jobs", int64(1)},
			[]any{"unsubscribe", "jobs", int64(0)},
		}},
		{name: "native missing kind", value: map[string]any{
			"event": "PUBSUB_MESSAGE", "payload": map[string]any{"channel": "jobs", "message": "body"},
		}},
		{name: "native unknown kind", value: map[string]any{
			"event": "PUBSUB_MESSAGE", "payload": map[string]any{"kind": "other", "channel": "jobs", "message": "body"},
		}},
		{name: "native missing channel", value: map[string]any{
			"event": "PUBSUB_MESSAGE", "payload": map[string]any{"kind": "message", "message": "body"},
		}},
		{name: "native missing pattern", value: map[string]any{
			"event": "PUBSUB_MESSAGE", "payload": map[string]any{"kind": "pmessage", "channel": "jobs", "message": "body"},
		}},
		{name: "native missing payload", value: map[string]any{
			"event": "PUBSUB_MESSAGE", "payload": map[string]any{"kind": "message", "channel": "jobs"},
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if message, err := parsePubSubMessage(tc.value); err == nil {
				t.Fatalf("accepted malformed pubsub event as %#v", message)
			}
		})
	}
}

func TestParsePubSubMessageAcceptsProtocolShapes(t *testing.T) {
	tests := []struct {
		name        string
		value       any
		wantKind    string
		wantChannel string
		wantPattern string
		wantPayload any
		wantCount   int64
	}{
		{name: "message", value: []any{[]byte("message"), []byte("jobs"), nil}, wantKind: "message", wantChannel: "jobs"},
		{name: "pattern message", value: []any{"pmessage", "jobs:*", "jobs:1", "body"}, wantKind: "pmessage", wantPattern: "jobs:*", wantChannel: "jobs:1", wantPayload: "body"},
		{name: "acknowledgement", value: []any{"subscribe", "jobs", int64(1)}, wantKind: "subscribe", wantChannel: "jobs", wantCount: 1},
		{name: "nested acknowledgements", value: []any{
			[]any{"subscribe", "jobs", int64(1)},
			[]any{"subscribe", "alerts", int64(2)},
		}, wantKind: "subscribe", wantChannel: "alerts", wantCount: 2},
		{name: "native message", value: map[string]any{
			"event":   "PUBSUB_MESSAGE",
			"payload": map[string]any{"kind": "PMESSAGE", "pattern": "jobs:*", "channel": "jobs:1", "message": []byte("body")},
		}, wantKind: "pmessage", wantPattern: "jobs:*", wantChannel: "jobs:1", wantPayload: []byte("body")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			message, err := parsePubSubMessage(tc.value)
			if err != nil {
				t.Fatal(err)
			}
			if message.Kind != tc.wantKind || message.Channel != tc.wantChannel || message.Pattern != tc.wantPattern ||
				message.Count != tc.wantCount || asString(message.Payload) != asString(tc.wantPayload) || message.Raw == nil {
				t.Fatalf("decoded pubsub event = %#v", message)
			}
		})
	}
}

func TestPubSubNextRejectsMalformedMessage(t *testing.T) {
	exec := newNativeExecutor(defaultNativeOptions("unused", false))
	exec.enableEventDelivery()
	exec.deliverEvent([]any{"message", int64(7), "body"})
	t.Cleanup(func() { _ = exec.Close() })

	if message, err := newPubSub(exec, false).Next(context.Background()); err == nil {
		t.Fatalf("Next accepted malformed server event as %#v", message)
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
			_, err := newPubSub(exec, false).SubscribeFlowWake(context.Background(), tc.opt)
			if err == nil || errors.Is(err, net.ErrClosed) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("invalid FLOW_WAKE filter error = %v; want local %q validation", err, tc.want)
			}
		})
	}
}
