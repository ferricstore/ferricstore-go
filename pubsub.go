package ferricstore

import (
	"context"
	"errors"
)

type PubSub struct {
	exec  *NativeExecutor
	owned bool
}

// PubSubMessage is a Redis-compatible pub/sub message or subscription ack.
type PubSubMessage struct {
	Kind    string
	Channel string
	Pattern string
	Payload any
	Count   int64
	Raw     any
}

// NativeEvent is an unsolicited native protocol event delivered on request_id=0.
type NativeEvent struct {
	Name    string
	Payload map[string]any
	AtMS    int64
	Raw     map[string]any
}

// EventSubscription describes native event subscriptions acknowledged by the server.
type EventSubscription struct {
	Subscribed []string
	Supported  []string
	Raw        map[string]any
}

// FlowWakeSubscriptionOptions filters FLOW_WAKE events for queue/workflow workers.
type FlowWakeSubscriptionOptions struct {
	Type          string
	State         string
	States        []string
	PartitionKey  string
	PartitionKeys []string
	Priority      *int64
	Limit         *int
}

// NewPubSub creates an isolated native protocol connection for long-lived pub/sub use.
func NewPubSub(addr string, opts ...NativeOption) *PubSub {
	exec := NewNativeExecutor(addr, opts...)
	return &PubSub{exec: exec, owned: true}
}

// NewPubSubFromURL creates an isolated pub/sub connection from a ferric:// or ferrics:// URL.
func NewPubSubFromURL(rawurl string, opts ...NativeOption) (*PubSub, error) {
	exec, err := NewNativeExecutorFromURL(rawurl, opts...)
	if err != nil {
		return nil, err
	}
	return &PubSub{exec: exec, owned: true}, nil
}

// OpenPubSub opens a pub/sub view over the client's existing multiplexed native connection.
func (c *Client) OpenPubSub() (*PubSub, error) {
	native, ok := c.exec.(*NativeExecutor)
	if ok {
		return &PubSub{exec: native}, nil
	}
	topology, ok := c.exec.(*TopologyNativeExecutor)
	if ok {
		control, err := topology.controlAdapter(context.Background())
		if err != nil {
			return nil, err
		}
		return &PubSub{exec: control}, nil
	}
	return nil, errors.New("pubsub requires a native client executor")
}

// Close closes isolated pub/sub connections. Shared client pub/sub views are left open.
func (p *PubSub) Close() error {
	if p == nil || p.exec == nil {
		return nil
	}
	if !p.owned {
		return nil
	}
	return p.exec.Close()
}

// DroppedEvents returns native events dropped because the client event buffer was full.
func (p *PubSub) DroppedEvents() uint64 {
	if p == nil || p.exec == nil {
		return 0
	}
	return p.exec.DroppedEvents()
}

// Subscribe subscribes to Redis-compatible pub/sub channels.
func (p *PubSub) Subscribe(ctx context.Context, channels ...string) (PubSubMessage, error) {
	args := []any{"SUBSCRIBE"}
	for _, channel := range channels {
		args = append(args, channel)
	}
	return p.pubsubCommand(ctx, args...)
}

// Unsubscribe unsubscribes from Redis-compatible pub/sub channels.
func (p *PubSub) Unsubscribe(ctx context.Context, channels ...string) (PubSubMessage, error) {
	args := []any{"UNSUBSCRIBE"}
	for _, channel := range channels {
		args = append(args, channel)
	}
	return p.pubsubCommand(ctx, args...)
}

// PSubscribe subscribes to Redis-compatible pub/sub patterns.
func (p *PubSub) PSubscribe(ctx context.Context, patterns ...string) (PubSubMessage, error) {
	args := []any{"PSUBSCRIBE"}
	for _, pattern := range patterns {
		args = append(args, pattern)
	}
	return p.pubsubCommand(ctx, args...)
}

// PUnsubscribe unsubscribes from Redis-compatible pub/sub patterns.
func (p *PubSub) PUnsubscribe(ctx context.Context, patterns ...string) (PubSubMessage, error) {
	args := []any{"PUNSUBSCRIBE"}
	for _, pattern := range patterns {
		args = append(args, pattern)
	}
	return p.pubsubCommand(ctx, args...)
}

// SubscribeEvents subscribes to native protocol events such as FLOW_WAKE.
func (p *PubSub) SubscribeEvents(ctx context.Context, events ...string) (EventSubscription, error) {
	return p.eventSubscription(ctx, nativeOpSubscribeEvents, map[string]any{"events": eventArgs(events)})
}

// UnsubscribeEvents unsubscribes from native protocol events.
func (p *PubSub) UnsubscribeEvents(ctx context.Context, events ...string) (EventSubscription, error) {
	return p.eventSubscription(ctx, nativeOpUnsubscribeEvents, map[string]any{"events": eventArgs(events)})
}

// SubscribeFlowWake subscribes to server-side wake hints for due Flow work.
func (p *PubSub) SubscribeFlowWake(ctx context.Context, opt FlowWakeSubscriptionOptions) (EventSubscription, error) {
	if opt.State != "" && len(opt.States) > 0 {
		return EventSubscription{}, errors.New("state and states are mutually exclusive")
	}
	if opt.PartitionKey != "" && len(opt.PartitionKeys) > 0 {
		return EventSubscription{}, errors.New("partition_key and partition_keys are mutually exclusive")
	}
	flowWake := map[string]any{"type": opt.Type}
	appendOptMap(flowWake, "state", opt.State)
	if len(opt.States) > 0 {
		states := make([]any, 0, len(opt.States))
		for _, state := range opt.States {
			states = append(states, state)
		}
		flowWake["states"] = states
	}
	appendOptMap(flowWake, "partition_key", opt.PartitionKey)
	if len(opt.PartitionKeys) > 0 {
		partitions := make([]any, 0, len(opt.PartitionKeys))
		for _, partition := range opt.PartitionKeys {
			partitions = append(partitions, partition)
		}
		flowWake["partition_keys"] = partitions
	}
	if opt.Priority != nil {
		flowWake["priority"] = *opt.Priority
	} else {
		flowWake["priority"] = int64(0)
	}
	if opt.Limit != nil {
		flowWake["limit"] = int64(*opt.Limit)
	}
	return p.eventSubscription(ctx, nativeOpSubscribeEvents, map[string]any{
		"events":    []any{"FLOW_WAKE"},
		"flow_wake": flowWake,
	})
}

// Next waits for the next Redis-compatible pub/sub message.
func (p *PubSub) Next(ctx context.Context) (PubSubMessage, error) {
	if p == nil || p.exec == nil {
		return PubSubMessage{}, errors.New("pubsub is closed")
	}
	value, err := p.exec.nextEvent(ctx)
	if err != nil {
		return PubSubMessage{}, err
	}
	return pubSubMessageFromNative(value), nil
}

// NextEvent waits for the next native protocol event.
func (p *PubSub) NextEvent(ctx context.Context) (NativeEvent, error) {
	if p == nil || p.exec == nil {
		return NativeEvent{}, errors.New("pubsub is closed")
	}
	value, err := p.exec.nextEvent(ctx)
	if err != nil {
		return NativeEvent{}, err
	}
	return nativeEventFromValue(value)
}

func (p *PubSub) pubsubCommand(ctx context.Context, args ...any) (PubSubMessage, error) {
	if p == nil || p.exec == nil {
		return PubSubMessage{}, errors.New("pubsub is closed")
	}
	value, err := p.exec.command(ctx, args...)
	if err != nil {
		return PubSubMessage{}, err
	}
	return pubSubMessageFromNative(value), nil
}

func (p *PubSub) eventSubscription(ctx context.Context, opcode uint16, payload map[string]any) (EventSubscription, error) {
	if p == nil || p.exec == nil {
		return EventSubscription{}, errors.New("pubsub is closed")
	}
	value, err := p.exec.request(ctx, opcode, 0, payload, 0)
	if err != nil {
		return EventSubscription{}, err
	}
	m, err := nativeMap(value)
	if err != nil {
		return EventSubscription{}, err
	}
	return EventSubscription{
		Subscribed: stringList(m["subscribed"]),
		Supported:  stringList(m["supported"]),
		Raw:        m,
	}, nil
}

func pubSubMessageFromNative(value any) PubSubMessage {
	message := PubSubMessage{Raw: value}
	if event, err := nativeEventFromValue(value); err == nil && event.Name == "PUBSUB_MESSAGE" {
		if len(event.Payload) == 0 {
			message.Kind = "message"
			return message
		}
		message.Kind = asString(event.Payload["kind"])
		message.Channel = asString(event.Payload["channel"])
		message.Pattern = asString(event.Payload["pattern"])
		message.Payload = event.Payload["message"]
		return message
	}
	items, ok := value.([]any)
	if !ok || len(items) == 0 {
		message.Payload = value
		return message
	}
	if nested, ok := items[0].([]any); ok {
		items = nested
	}
	message.Kind = asString(items[0])
	switch message.Kind {
	case "subscribe", "unsubscribe":
		if len(items) > 1 {
			message.Channel = asString(items[1])
		}
		if len(items) > 2 {
			message.Count = asInt64(items[2])
		}
	case "psubscribe", "punsubscribe":
		if len(items) > 1 {
			message.Pattern = asString(items[1])
		}
		if len(items) > 2 {
			message.Count = asInt64(items[2])
		}
	case "message":
		if len(items) > 1 {
			message.Channel = asString(items[1])
		}
		if len(items) > 2 {
			message.Payload = items[2]
		}
	case "pmessage":
		if len(items) > 1 {
			message.Pattern = asString(items[1])
		}
		if len(items) > 2 {
			message.Channel = asString(items[2])
		}
		if len(items) > 3 {
			message.Payload = items[3]
		}
	default:
		if len(items) > 1 {
			message.Payload = items[1:]
		}
	}
	return message
}

func nativeEventFromValue(value any) (NativeEvent, error) {
	m, err := nativeMap(value)
	if err != nil {
		return NativeEvent{}, err
	}
	return NativeEvent{
		Name:    asString(m["event"]),
		Payload: stringObjectMap(m["payload"]),
		AtMS:    asInt64(m["at_ms"]),
		Raw:     m,
	}, nil
}

func eventArgs(events []string) []any {
	out := make([]any, 0, len(events))
	for _, event := range events {
		out = append(out, event)
	}
	return out
}

func appendOptMap(m map[string]any, name string, value string) {
	if value != "" {
		m[name] = value
	}
}

func stringList(value any) []string {
	if items, ok := value.([]string); ok {
		return append([]string(nil), items...)
	}
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, asString(item))
	}
	return out
}
