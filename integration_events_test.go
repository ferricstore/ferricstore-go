//go:build integration

package ferricstore

import (
	"testing"
	"time"
)

func TestIntegrationNativePubSubAndFlowWakeEvents(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationClient(JSONCodec{})
	defer client.Close()
	eventClient := integrationDirectClient(JSONCodec{})
	defer eventClient.Close()

	pubsub, err := eventClient.OpenPubSub()
	if err != nil {
		t.Fatal(err)
	}

	runID := integrationSuffix("events")
	channel := "go-sdk:events:" + runID
	ack, err := pubsub.Subscribe(ctx, channel)
	if err != nil {
		t.Fatal(err)
	}
	recordIntegrationCommand([]any{"SUBSCRIBE"})
	if ack.Kind != "subscribe" || ack.Channel != channel || ack.Count < 1 {
		t.Fatalf("unexpected subscribe ack: %#v", ack)
	}

	requireNonNegative(t, must[int64](t)(client.Publish(ctx, channel, "hello")))
	message, err := pubsub.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if message.Kind != "message" || message.Channel != channel || asString(message.Payload) != "hello" {
		t.Fatalf("unexpected pubsub message: %#v", message)
	}
	unack, err := pubsub.Unsubscribe(ctx, channel)
	if err != nil {
		t.Fatal(err)
	}
	recordIntegrationCommand([]any{"UNSUBSCRIBE"})
	if unack.Kind != "unsubscribe" || unack.Channel != channel {
		t.Fatalf("unexpected unsubscribe ack: %#v", unack)
	}

	pattern := "go-sdk:events:" + runID + ":*"
	patternChannel := "go-sdk:events:" + runID + ":pattern"
	pack, err := pubsub.PSubscribe(ctx, pattern)
	if err != nil {
		t.Fatal(err)
	}
	recordIntegrationCommand([]any{"PSUBSCRIBE"})
	if pack.Kind != "psubscribe" || pack.Pattern != pattern {
		t.Fatalf("unexpected psubscribe ack: %#v", pack)
	}
	requireNonNegative(t, must[int64](t)(client.Publish(ctx, patternChannel, "pattern-message")))
	pmessage, err := pubsub.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pmessage.Kind != "pmessage" || pmessage.Pattern != pattern || pmessage.Channel != patternChannel || asString(pmessage.Payload) != "pattern-message" {
		t.Fatalf("unexpected pattern pubsub message: %#v", pmessage)
	}
	punack, err := pubsub.PUnsubscribe(ctx, pattern)
	if err != nil {
		t.Fatal(err)
	}
	recordIntegrationCommand([]any{"PUNSUBSCRIBE"})
	if punack.Kind != "punsubscribe" || punack.Pattern != pattern {
		t.Fatalf("unexpected punsubscribe ack: %#v", punack)
	}

	typeName := "go-sdk-wake-" + runID
	partition := "go-sdk:wake:" + runID + ":partition"
	sub, err := pubsub.SubscribeFlowWake(ctx, FlowWakeSubscriptionOptions{
		Type:  typeName,
		State: "queued",
		Limit: Int(10),
	})
	if err != nil {
		t.Fatal(err)
	}
	recordIntegrationCommand([]any{"SUBSCRIBE_EVENTS"})
	if !contains(sub.Subscribed, "FLOW_WAKE") {
		t.Fatalf("expected FLOW_WAKE subscription: %#v", sub)
	}

	now := time.Now().UnixMilli()
	_ = must[*FlowRecord](t)(client.Create(ctx, CreateOptions{
		ID:           "go-sdk:wake:" + runID,
		Type:         typeName,
		State:        "queued",
		PartitionKey: partition,
		RunAtMS:      now,
		NowMS:        now,
		Idempotent:   Bool(true),
	}))
	event, err := pubsub.NextEvent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if event.Name != "FLOW_WAKE" || asString(event.Payload["type"]) != typeName || asInt64(event.Payload["credit"]) <= 0 {
		t.Fatalf("unexpected flow wake event: %#v", event)
	}

	unsub, err := pubsub.UnsubscribeEvents(ctx, "FLOW_WAKE")
	if err != nil {
		t.Fatal(err)
	}
	recordIntegrationCommand([]any{"UNSUBSCRIBE_EVENTS"})
	if contains(unsub.Subscribed, "FLOW_WAKE") {
		t.Fatalf("expected FLOW_WAKE to be unsubscribed: %#v", unsub)
	}
}
