package ferricstore

import (
	"context"
	"testing"
	"time"
)

func TestPubSubNextDemultiplexesNativeEventBeforeMessage(t *testing.T) {
	pubsub := pubSubDemuxFixture(t)
	pubsub.exec.deliverEvent(pubSubDemuxNativeEvent())
	pubsub.exec.deliverEvent(pubSubDemuxMessage())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	message, err := pubsub.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if message.Kind != "message" || message.Channel != "jobs" || asString(message.Payload) != "ready" {
		t.Fatalf("pubsub message = %#v", message)
	}
	event, err := pubsub.NextEvent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if event.Name != "FLOW_WAKE" || asString(event.Payload["type"]) != "email" {
		t.Fatalf("native event = %#v", event)
	}
}

func TestPubSubNextEventDemultiplexesMessageBeforeNativeEvent(t *testing.T) {
	pubsub := pubSubDemuxFixture(t)
	pubsub.exec.deliverEvent(pubSubDemuxMessage())
	pubsub.exec.deliverEvent(pubSubDemuxNativeEvent())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	event, err := pubsub.NextEvent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if event.Name != "FLOW_WAKE" || asString(event.Payload["type"]) != "email" {
		t.Fatalf("native event = %#v", event)
	}
	message, err := pubsub.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if message.Kind != "message" || message.Channel != "jobs" || asString(message.Payload) != "ready" {
		t.Fatalf("pubsub message = %#v", message)
	}
}

func pubSubDemuxFixture(t *testing.T) *PubSub {
	t.Helper()
	exec := NewNativeExecutor("unused", WithNativeHeartbeat(0, 0))
	exec.enableEventDelivery()
	t.Cleanup(func() { _ = exec.Close() })
	return &PubSub{exec: exec}
}

func pubSubDemuxMessage() nativeServerEvent {
	return nativeServerEvent{
		opcode: nativeOpEvent,
		value:  []any{[]byte("message"), []byte("jobs"), []byte("ready")},
	}
}

func pubSubDemuxNativeEvent() nativeServerEvent {
	return nativeServerEvent{
		opcode: nativeOpEvent,
		value: map[string]any{
			"event":   "FLOW_WAKE",
			"payload": map[string]any{"type": "email"},
		},
	}
}
