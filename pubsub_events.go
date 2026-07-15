package ferricstore

import (
	"context"
	"errors"
	"net"
	"strings"
)

type pubSubEventKind uint8

const (
	pubSubRedisEvent pubSubEventKind = iota
	pubSubNativeEvent
)

func (p *PubSub) nextDemultiplexedEvent(ctx context.Context, want pubSubEventKind) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		p.eventMu.Lock()
		if value, ok := p.popDemultiplexedEventLocked(want); ok {
			p.eventMu.Unlock()
			return value, nil
		}
		if !p.eventReading {
			p.eventReading = true
			p.eventMu.Unlock()

			value, err := p.readNextServerEvent(ctx)
			p.eventMu.Lock()
			p.eventReading = false
			p.signalDemultiplexedEventLocked()
			if err != nil {
				p.eventMu.Unlock()
				return nil, err
			}
			kind := classifyPubSubServerEvent(value)
			if kind == want {
				p.eventMu.Unlock()
				return value, nil
			}
			p.queueDemultiplexedEventLocked(kind, value)
			p.eventMu.Unlock()
			continue
		}
		changed := p.demultiplexedEventChangedLocked()
		p.eventMu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (p *PubSub) readNextServerEvent(ctx context.Context) (any, error) {
	for {
		if err := p.replayAfterExternalReconnect(ctx); err != nil {
			return nil, err
		}
		value, err := p.exec.nextEvent(ctx)
		if err == nil {
			return value, nil
		}
		if !errors.Is(err, errNativeConnectionUnavailable) && !errors.Is(err, net.ErrClosed) {
			return nil, err
		}
		if err := p.reconnectAndReplay(ctx); err != nil {
			return nil, err
		}
	}
}

func classifyPubSubServerEvent(value any) pubSubEventKind {
	if event, ok := value.(nativeServerEvent); ok && event.opcode == nativeOpGoAway {
		return pubSubNativeEvent
	}
	raw := nativeServerEventValue(value)
	if mapping, ok := raw.(map[string]any); ok {
		event, err := nativeEventFromValue(mapping)
		if err == nil && strings.EqualFold(event.Name, "PUBSUB_MESSAGE") {
			return pubSubRedisEvent
		}
		return pubSubNativeEvent
	}
	return pubSubRedisEvent
}

func (p *PubSub) queueDemultiplexedEventLocked(kind pubSubEventKind, value any) {
	size := nativeBufferedEventSize(value)
	count := len(p.messageEvents) + len(p.nativeEvents)
	if count >= nativeEventBufferCapacity || size > nativeMaxBufferedEventBytes-p.eventBufferedBytes {
		p.exec.droppedEvents.Add(1)
		return
	}
	event := nativeQueuedEvent{value: value, bytes: size}
	if kind == pubSubNativeEvent {
		p.nativeEvents = append(p.nativeEvents, event)
	} else {
		p.messageEvents = append(p.messageEvents, event)
	}
	p.eventBufferedBytes += size
}

func (p *PubSub) popDemultiplexedEventLocked(kind pubSubEventKind) (any, bool) {
	queue := &p.messageEvents
	if kind == pubSubNativeEvent {
		queue = &p.nativeEvents
	}
	if len(*queue) == 0 {
		return nil, false
	}
	event := (*queue)[0]
	(*queue)[0] = nativeQueuedEvent{}
	*queue = (*queue)[1:]
	if len(*queue) == 0 {
		*queue = nil
	}
	p.eventBufferedBytes -= event.bytes
	if p.eventBufferedBytes < 0 {
		p.eventBufferedBytes = 0
	}
	return event.value, true
}

func (p *PubSub) demultiplexedEventChangedLocked() <-chan struct{} {
	if p.eventChanged == nil {
		p.eventChanged = make(chan struct{})
	}
	return p.eventChanged
}

func (p *PubSub) signalDemultiplexedEventLocked() {
	if p.eventChanged == nil {
		return
	}
	close(p.eventChanged)
	p.eventChanged = make(chan struct{})
}
