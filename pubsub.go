package ferricstore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

type PubSub struct {
	// exec retains the originally selected executor for compatibility with
	// package-internal diagnostics. Runtime operations use currentExec so a
	// topology-backed view can atomically move away from a retired adapter.
	exec           *NativeExecutor
	currentExec    *atomic.Pointer[NativeExecutor]
	selectExecutor pubSubExecutorSelector
	owned          bool
	*pubSubState
}

type pubSubExecutorSelector interface {
	pubSubControlAdapter(context.Context, *NativeExecutor) (*NativeExecutor, error)
}

type pubSubState struct {
	mu             sync.Mutex
	replayMu       sync.Mutex
	channels       map[string]struct{}
	patterns       map[string]struct{}
	eventReplays   []pubSubEventReplay
	lastExecutor   *NativeExecutor
	lastGeneration uint64

	eventMu            sync.Mutex
	eventReading       bool
	eventChanged       chan struct{}
	messageEvents      []nativeQueuedEvent
	nativeEvents       []nativeQueuedEvent
	eventBufferedBytes int
}

func newPubSub(exec *NativeExecutor, owned bool) *PubSub {
	pubsub := &PubSub{
		exec:        exec,
		currentExec: new(atomic.Pointer[NativeExecutor]),
		owned:       owned,
		pubSubState: &pubSubState{},
	}
	pubsub.currentExec.Store(exec)
	return pubsub
}

func (p *PubSub) nativeExecutor() *NativeExecutor {
	if p == nil {
		return nil
	}
	if p.currentExec != nil {
		if exec := p.currentExec.Load(); exec != nil {
			return exec
		}
	}
	return p.exec
}

type pubSubEventReplay struct {
	opcode  uint16
	payload map[string]any
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
	Opcode  uint16
	LaneID  uint32
	Flags   byte
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
	exec.enableEventDelivery()
	return newPubSub(exec, true)
}

// NewPubSubFromURL creates an isolated pub/sub connection from a ferric:// or ferrics:// URL.
func NewPubSubFromURL(rawurl string, opts ...NativeOption) (*PubSub, error) {
	exec, err := NewNativeExecutorFromURL(rawurl, opts...)
	if err != nil {
		return nil, err
	}
	exec.enableEventDelivery()
	return newPubSub(exec, true), nil
}

// OpenPubSub opens a pub/sub view over the client's existing multiplexed native connection.
func (c *Client) OpenPubSub() (*PubSub, error) {
	if c == nil {
		return nil, errors.New("pubsub requires a native client executor")
	}
	exec := eventExecutor(c.exec)
	native, ok := exec.(*NativeExecutor)
	if ok {
		native.enableEventDelivery()
		return newPubSub(native, false), nil
	}
	topology, ok := exec.(*TopologyNativeExecutor)
	if ok {
		control, err := topology.controlAdapter(context.Background())
		if err != nil {
			return nil, err
		}
		control.enableEventDelivery()
		pubsub := newPubSub(control, false)
		pubsub.selectExecutor = topology
		return pubsub, nil
	}
	return nil, errors.New("pubsub requires a native client executor")
}

func eventExecutor(exec Executor) Executor {
	// Built-in batching wrappers retain the owning client rather than hiding a
	// separate transport. Unwrap only these known layers; arbitrary executors
	// remain opaque. The depth bound also keeps malformed internal wrapper
	// cycles from looping forever.
	for range 16 {
		switch wrapped := exec.(type) {
		case *AutoBatchExecutor:
			if wrapped == nil || wrapped.client == nil {
				return nil
			}
			exec = wrapped.client.exec
		case *BufferedExecutor:
			if wrapped == nil || wrapped.client == nil {
				return nil
			}
			exec = wrapped.client.exec
		default:
			return exec
		}
	}
	return nil
}

// DroppedEvents returns the aggregate number of events dropped by active
// topology connections. An adapter is counted once even if multiple endpoint
// identities temporarily refer to it during a topology transition.
func (e *TopologyNativeExecutor) DroppedEvents() uint64 {
	if e == nil {
		return 0
	}
	e.mu.Lock()
	adapters := make(map[*NativeExecutor]struct{}, len(e.adapters)+len(e.retiringAdapters))
	for _, adapter := range e.adapters {
		if adapter != nil {
			adapters[adapter] = struct{}{}
		}
	}
	for adapter := range e.retiringAdapters {
		if adapter != nil {
			adapters[adapter] = struct{}{}
		}
	}
	e.mu.Unlock()
	var dropped uint64
	for adapter := range adapters {
		dropped += adapter.DroppedEvents()
	}
	return dropped
}

// Close closes isolated pub/sub connections. Shared client pub/sub views are left open.
func (p *PubSub) Close() error {
	exec := p.nativeExecutor()
	if exec == nil {
		return nil
	}
	if !p.owned {
		return nil
	}
	return exec.Close()
}

// DroppedEvents returns native events dropped because the client event buffer was full.
func (p *PubSub) DroppedEvents() uint64 {
	exec := p.nativeExecutor()
	if exec == nil {
		return 0
	}
	return exec.DroppedEvents()
}

// Subscribe subscribes to Redis-compatible pub/sub channels.
func (p *PubSub) Subscribe(ctx context.Context, channels ...string) (PubSubMessage, error) {
	if len(channels) == 0 {
		return PubSubMessage{}, errors.New("SUBSCRIBE requires at least one channel")
	}
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
	if len(patterns) == 0 {
		return PubSubMessage{}, errors.New("PSUBSCRIBE requires at least one pattern")
	}
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
	if err := validateFlowWakeSubscriptionOptions(opt); err != nil {
		return EventSubscription{}, err
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
	if p.nativeExecutor() == nil {
		return PubSubMessage{}, errors.New("pubsub is closed")
	}
	value, err := p.nextDemultiplexedEvent(ctx, pubSubRedisEvent)
	if err != nil {
		return PubSubMessage{}, err
	}
	return parsePubSubMessage(nativeServerEventValue(value))
}

// NextEvent waits for the next native protocol event.
func (p *PubSub) NextEvent(ctx context.Context) (NativeEvent, error) {
	if p.nativeExecutor() == nil {
		return NativeEvent{}, errors.New("pubsub is closed")
	}
	value, err := p.nextDemultiplexedEvent(ctx, pubSubNativeEvent)
	if err != nil {
		return NativeEvent{}, err
	}
	return nativeEventFromServerValue(value)
}

func (p *PubSub) pubsubCommand(ctx context.Context, args ...any) (PubSubMessage, error) {
	if p.nativeExecutor() == nil {
		return PubSubMessage{}, errors.New("pubsub is closed")
	}
	p.replayMu.Lock()
	defer p.replayMu.Unlock()
	value, stamp, err := p.requestWithReplayRetryLocked(ctx, func(exec *NativeExecutor) (any, error) {
		return exec.command(ctx, args...)
	})
	if err != nil {
		return PubSubMessage{}, err
	}
	message, err := pubSubAcknowledgementFromNative(value, asString(args[0]), args[1:]...)
	if err != nil {
		return PubSubMessage{}, err
	}
	p.trackPubSubCommand(args)
	reconnected := !p.connectionStampCurrent(stamp)
	if reconnected {
		p.requireReplayFromStamp(stamp)
		if err := p.reconnectAndReplayLocked(ctx); err != nil {
			return PubSubMessage{}, err
		}
	}
	if reconnected {
		message.Count = p.trackedPubSubCount()
	}
	return message, nil
}

func (p *PubSub) eventSubscription(ctx context.Context, opcode uint16, payload map[string]any) (EventSubscription, error) {
	if p.nativeExecutor() == nil {
		return EventSubscription{}, errors.New("pubsub is closed")
	}
	p.replayMu.Lock()
	defer p.replayMu.Unlock()
	value, stamp, err := p.requestWithReplayRetryLocked(ctx, func(exec *NativeExecutor) (any, error) {
		return exec.request(ctx, opcode, 0, payload, 0)
	})
	if err != nil {
		return EventSubscription{}, err
	}
	m, err := nativeMap(value)
	if err != nil {
		return EventSubscription{}, err
	}
	subscribed, err := pubSubStringList(m["subscribed"], "subscribed")
	if err != nil {
		return EventSubscription{}, err
	}
	supported, err := pubSubStringList(m["supported"], "supported")
	if err != nil {
		return EventSubscription{}, err
	}
	p.trackEventSubscription(opcode, payload)
	if !p.connectionStampCurrent(stamp) {
		p.requireReplayFromStamp(stamp)
		if err := p.reconnectAndReplayLocked(ctx); err != nil {
			return EventSubscription{}, err
		}
	}
	return EventSubscription{
		Subscribed: subscribed,
		Supported:  supported,
		Raw:        m,
	}, nil
}
