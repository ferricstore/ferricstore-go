package ferricstore

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
)

func (p *PubSub) requestWithReplayRetryLocked(ctx context.Context, request func() (any, error)) (any, uint64, error) {
	if err := p.reconnectAndReplayLocked(ctx); err != nil {
		return nil, 0, err
	}
	retries := p.exec.opts.ReconnectMaxRetries
	for {
		generation := p.exec.currentConnectionGeneration()
		value, err := request()
		if err == nil {
			return value, generation, nil
		}
		if retries <= 0 || !isNativeReconnectableTransportError(err) || ctx != nil && ctx.Err() != nil {
			return nil, generation, err
		}
		retries--
		if err := p.reconnectAndReplayLocked(ctx); err != nil {
			return nil, generation, err
		}
	}
}

func (p *PubSub) requireReplayFromGeneration(generation uint64) {
	p.mu.Lock()
	p.lastGeneration = generation
	p.mu.Unlock()
}

func (p *PubSub) trackedPubSubCount() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return int64(len(p.channels) + len(p.patterns))
}

func (p *PubSub) trackPubSubCommand(args []any) {
	if len(args) == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	name := strings.ToUpper(asString(args[0]))
	subscribe := name == "SUBSCRIBE" || name == "PSUBSCRIBE"
	patternCommand := name == "PSUBSCRIBE" || name == "PUNSUBSCRIBE"
	target := p.channels
	if patternCommand {
		if p.patterns == nil && subscribe {
			p.patterns = make(map[string]struct{})
		}
		target = p.patterns
	} else if p.channels == nil && subscribe {
		p.channels = make(map[string]struct{})
		target = p.channels
	}
	if !subscribe && len(args) == 1 {
		clear(target)
	} else {
		for _, arg := range args[1:] {
			key := asString(arg)
			if subscribe {
				target[key] = struct{}{}
			} else {
				delete(target, key)
			}
		}
	}
	if len(target) == 0 {
		if patternCommand {
			p.patterns = nil
		} else {
			p.channels = nil
		}
	}
	p.lastGeneration = p.exec.currentConnectionGeneration()
}

func (p *PubSub) trackEventSubscription(opcode uint16, payload map[string]any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if opcode == nativeOpSubscribeEvents {
		tracked := cloneNativeMapValue(payload)
		events := normalizedNativeEventNames(payload["events"])
		if len(events) == 0 {
			p.lastGeneration = p.exec.currentConnectionGeneration()
			return
		}
		tracked["events"] = eventArgs(events)
		if _, filtered := tracked["flow_wake"]; filtered && slices.Contains(events, "FLOW_WAKE") {
			p.eventReplays = filterEventReplays(p.eventReplays, []string{"FLOW_WAKE"})
		}
		for _, replay := range p.eventReplays {
			if replay.opcode == opcode && reflect.DeepEqual(replay.payload, tracked) {
				p.lastGeneration = p.exec.currentConnectionGeneration()
				return
			}
		}
		p.eventReplays = append(p.eventReplays, pubSubEventReplay{opcode: opcode, payload: tracked})
	} else {
		unsubscribed := normalizedNativeEventNames(payload["events"])
		if len(unsubscribed) == 0 {
			p.lastGeneration = p.exec.currentConnectionGeneration()
			return
		}
		p.eventReplays = filterEventReplays(p.eventReplays, unsubscribed)
	}
	p.lastGeneration = p.exec.currentConnectionGeneration()
}

func filterEventReplays(replays []pubSubEventReplay, removed []string) []pubSubEventReplay {
	filtered := replays[:0]
	for _, replay := range replays {
		remaining := stringListDifference(normalizedNativeEventNames(replay.payload["events"]), removed)
		if len(remaining) == 0 {
			continue
		}
		updated := cloneNativeMapValue(replay.payload)
		updated["events"] = eventArgs(remaining)
		if !slices.Contains(remaining, "FLOW_WAKE") {
			delete(updated, "flow_wake")
		}
		filtered = append(filtered, pubSubEventReplay{opcode: replay.opcode, payload: updated})
	}
	clear(replays[len(filtered):])
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

func (p *PubSub) reconnectAndReplay(ctx context.Context) error {
	p.replayMu.Lock()
	defer p.replayMu.Unlock()
	return p.reconnectAndReplayLocked(ctx)
}

func (p *PubSub) reconnectAndReplayLocked(ctx context.Context) error {
	retries := max(0, p.exec.opts.ReconnectMaxRetries)
	for attempt := 0; ; attempt++ {
		if err := p.exec.ensureConnected(ctx); err != nil {
			return err
		}
		generation := p.exec.currentConnectionGeneration()
		p.mu.Lock()
		if generation == p.lastGeneration {
			p.mu.Unlock()
			return nil
		}
		channels := mapKeys(p.channels)
		patterns := mapKeys(p.patterns)
		replays := append([]pubSubEventReplay(nil), p.eventReplays...)
		p.mu.Unlock()

		if err := p.replayTrackedState(ctx, channels, patterns, replays); err != nil {
			return err
		}
		if p.exec.currentConnectionGeneration() == generation {
			p.mu.Lock()
			p.lastGeneration = generation
			p.mu.Unlock()
			if p.exec.currentConnectionGeneration() == generation {
				return nil
			}
		}
		if attempt >= retries {
			return fmt.Errorf("pubsub replay could not stabilize within its reconnect budget: %w", errNativeConnectionUnavailable)
		}
	}
}

func (p *PubSub) replayTrackedState(ctx context.Context, channels, patterns []string, replays []pubSubEventReplay) error {
	if len(channels) > 0 {
		args := make([]any, 1, len(channels)+1)
		args[0] = "SUBSCRIBE"
		for _, channel := range channels {
			args = append(args, channel)
		}
		if _, err := p.exec.command(ctx, args...); err != nil {
			return err
		}
	}
	if len(patterns) > 0 {
		args := make([]any, 1, len(patterns)+1)
		args[0] = "PSUBSCRIBE"
		for _, pattern := range patterns {
			args = append(args, pattern)
		}
		if _, err := p.exec.command(ctx, args...); err != nil {
			return err
		}
	}
	for _, replay := range replays {
		if _, err := p.exec.request(ctx, replay.opcode, 0, replay.payload, 0); err != nil {
			return err
		}
	}
	return nil
}

func (p *PubSub) replayAfterExternalReconnect(ctx context.Context) error {
	generation := p.exec.currentConnectionGeneration()
	p.mu.Lock()
	tracked := len(p.channels) > 0 || len(p.patterns) > 0 || len(p.eventReplays) > 0
	changed := tracked && p.lastGeneration != 0 && generation != 0 && generation != p.lastGeneration
	p.mu.Unlock()
	if !changed {
		return nil
	}
	return p.reconnectAndReplay(ctx)
}

func mapKeys(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	slices.Sort(out)
	return out
}

func cloneNativeMapValue(value map[string]any) map[string]any {
	encoded, err := encodeNativeValue(value)
	if err != nil {
		return value
	}
	decoded, _, err := decodeNativeValue(encoded)
	if err != nil {
		return value
	}
	cloned, ok := decoded.(map[string]any)
	if !ok {
		return value
	}
	return cloned
}

func stringListDifference(values, removed []string) []string {
	set := make(map[string]struct{}, len(removed))
	for _, value := range removed {
		set[value] = struct{}{}
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, found := set[value]; !found {
			out = append(out, value)
		}
	}
	return out
}

func normalizedNativeEventNames(value any) []string {
	values := stringList(value)
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.ToUpper(value)
		if _, duplicate := seen[value]; duplicate {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
