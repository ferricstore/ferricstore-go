package ferricstore

import (
	"errors"
	"fmt"
	"strings"
)

func parsePubSubMessage(value any) (PubSubMessage, error) {
	switch value.(type) {
	case map[string]any, map[interface{}]interface{}:
		return parseNativePubSubMessage(value)
	}

	items, ok := value.([]any)
	if !ok {
		return PubSubMessage{}, fmt.Errorf("pubsub event returned %T, expected array or native event", value)
	}
	if len(items) == 0 {
		return PubSubMessage{}, errors.New("pubsub event array is empty")
	}
	if _, nested := items[0].([]any); nested {
		return parseNestedPubSubAcknowledgements(items, value)
	}
	message, err := parseFlatPubSubMessage(items)
	if err != nil {
		return PubSubMessage{}, err
	}
	message.Raw = value
	return message, nil
}

func parseNativePubSubMessage(value any) (PubSubMessage, error) {
	event, err := nativeEventFromValue(value)
	if err != nil {
		return PubSubMessage{}, fmt.Errorf("invalid native pubsub event: %w", err)
	}
	if !strings.EqualFold(event.Name, "PUBSUB_MESSAGE") {
		return PubSubMessage{}, fmt.Errorf("native event is %q, expected PUBSUB_MESSAGE", event.Name)
	}
	kind, err := requiredPubSubString(event.Payload, "kind")
	if err != nil {
		return PubSubMessage{}, err
	}
	kind = strings.ToLower(kind)
	message := PubSubMessage{Kind: kind, Raw: value}
	switch kind {
	case "message":
		message.Channel, err = requiredPubSubString(event.Payload, "channel")
	case "pmessage":
		message.Pattern, err = requiredPubSubString(event.Payload, "pattern")
		if err == nil {
			message.Channel, err = requiredPubSubString(event.Payload, "channel")
		}
	default:
		return PubSubMessage{}, fmt.Errorf("unsupported native pubsub message kind %q", kind)
	}
	if err != nil {
		return PubSubMessage{}, err
	}
	payload, present := event.Payload["message"]
	if !present {
		return PubSubMessage{}, errors.New("native pubsub message is missing message")
	}
	message.Payload = payload
	return message, nil
}

func requiredPubSubString(payload map[string]any, field string) (string, error) {
	value, present := payload[field]
	if !present {
		return "", fmt.Errorf("native pubsub message is missing %s", field)
	}
	text, err := responseString(value, nil)
	if err != nil {
		return "", fmt.Errorf("native pubsub message has invalid %s: %w", field, err)
	}
	return text, nil
}

func parseNestedPubSubAcknowledgements(items []any, raw any) (PubSubMessage, error) {
	first, ok := items[0].([]any)
	if !ok || len(first) == 0 {
		return PubSubMessage{}, errors.New("pubsub acknowledgement array is empty")
	}
	kind, err := responseString(first[0], nil)
	if err != nil {
		return PubSubMessage{}, fmt.Errorf("invalid pubsub acknowledgement kind: %w", err)
	}
	kind = strings.ToLower(kind)
	if !isPubSubAcknowledgementKind(kind) {
		return PubSubMessage{}, fmt.Errorf("nested pubsub event kind %q is not an acknowledgement", kind)
	}

	var message PubSubMessage
	for index, item := range items {
		acknowledgement, ok := item.([]any)
		if !ok {
			return PubSubMessage{}, fmt.Errorf("pubsub acknowledgement %d returned %T, expected array", index, item)
		}
		message, err = parsePubSubAcknowledgement(acknowledgement, kind)
		if err != nil {
			return PubSubMessage{}, fmt.Errorf("pubsub acknowledgement %d: %w", index, err)
		}
	}
	message.Raw = raw
	return message, nil
}

func parseFlatPubSubMessage(items []any) (PubSubMessage, error) {
	kind, err := responseString(items[0], nil)
	if err != nil {
		return PubSubMessage{}, fmt.Errorf("invalid pubsub event kind: %w", err)
	}
	kind = strings.ToLower(kind)
	switch kind {
	case "message":
		if len(items) != 3 {
			return PubSubMessage{}, fmt.Errorf("message event expected 3 fields, got %d", len(items))
		}
		channel, err := responseString(items[1], nil)
		if err != nil {
			return PubSubMessage{}, fmt.Errorf("message event has invalid channel: %w", err)
		}
		return PubSubMessage{Kind: kind, Channel: channel, Payload: items[2]}, nil
	case "pmessage":
		if len(items) != 4 {
			return PubSubMessage{}, fmt.Errorf("pmessage event expected 4 fields, got %d", len(items))
		}
		pattern, err := responseString(items[1], nil)
		if err != nil {
			return PubSubMessage{}, fmt.Errorf("pmessage event has invalid pattern: %w", err)
		}
		channel, err := responseString(items[2], nil)
		if err != nil {
			return PubSubMessage{}, fmt.Errorf("pmessage event has invalid channel: %w", err)
		}
		return PubSubMessage{Kind: kind, Pattern: pattern, Channel: channel, Payload: items[3]}, nil
	case "subscribe", "unsubscribe", "psubscribe", "punsubscribe":
		message, err := parsePubSubAcknowledgement(items, kind)
		if err != nil {
			return PubSubMessage{}, fmt.Errorf("invalid %s acknowledgement: %w", kind, err)
		}
		return message, nil
	default:
		return PubSubMessage{}, fmt.Errorf("unsupported pubsub event kind %q", kind)
	}
}

func isPubSubAcknowledgementKind(kind string) bool {
	switch kind {
	case "subscribe", "unsubscribe", "psubscribe", "punsubscribe":
		return true
	default:
		return false
	}
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
	if _, nested := items[0].([]any); nested {
		last, ok := items[len(items)-1].([]any)
		if !ok {
			message.Payload = value
			return message
		}
		items = last
	}
	if len(items) == 0 {
		message.Payload = value
		return message
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

func pubSubAcknowledgementFromNative(value any, command string, expectedTargets ...any) (PubSubMessage, error) {
	items, ok := value.([]any)
	if !ok || len(items) == 0 {
		return PubSubMessage{}, fmt.Errorf("%s returned %T, expected acknowledgement array", command, value)
	}
	expectedKind := strings.ToLower(command)
	if _, nested := items[0].([]any); nested {
		if len(expectedTargets) > 0 && len(items) != len(expectedTargets) {
			return PubSubMessage{}, fmt.Errorf(
				"%s returned %d acknowledgements for %d targets",
				command, len(items), len(expectedTargets),
			)
		}
		var message PubSubMessage
		for index, item := range items {
			acknowledgement, ok := item.([]any)
			if !ok {
				return PubSubMessage{}, fmt.Errorf("%s acknowledgement %d returned %T, expected array", command, index, item)
			}
			parsed, err := parsePubSubAcknowledgement(acknowledgement, expectedKind)
			if err != nil {
				return PubSubMessage{}, fmt.Errorf("%s acknowledgement %d: %w", command, index, err)
			}
			if len(expectedTargets) > 0 {
				if err := validatePubSubAcknowledgementTarget(parsed, expectedKind, expectedTargets[index]); err != nil {
					return PubSubMessage{}, fmt.Errorf("%s acknowledgement %d: %w", command, index, err)
				}
			}
			message = parsed
		}
		message.Raw = value
		return message, nil
	}

	if len(expectedTargets) > 1 {
		return PubSubMessage{}, fmt.Errorf(
			"%s returned 1 acknowledgement for %d targets",
			command, len(expectedTargets),
		)
	}
	message, err := parsePubSubAcknowledgement(items, expectedKind)
	if err != nil {
		return PubSubMessage{}, fmt.Errorf("%s acknowledgement 0: %w", command, err)
	}
	if len(expectedTargets) == 1 {
		if err := validatePubSubAcknowledgementTarget(message, expectedKind, expectedTargets[0]); err != nil {
			return PubSubMessage{}, fmt.Errorf("%s acknowledgement 0: %w", command, err)
		}
	}
	message.Raw = value
	return message, nil
}

func validatePubSubAcknowledgementTarget(message PubSubMessage, kind string, expected any) error {
	actual := message.Channel
	if kind == "psubscribe" || kind == "punsubscribe" {
		actual = message.Pattern
	}
	want := asString(expected)
	if actual != want {
		return fmt.Errorf("target is %q, expected %q", actual, want)
	}
	return nil
}

func parsePubSubAcknowledgement(items []any, expectedKind string) (PubSubMessage, error) {
	if len(items) != 3 {
		return PubSubMessage{}, fmt.Errorf("expected 3 fields, got %d", len(items))
	}
	kind, err := responseString(items[0], nil)
	if err != nil {
		return PubSubMessage{}, fmt.Errorf("invalid kind: %w", err)
	}
	if !strings.EqualFold(kind, expectedKind) {
		return PubSubMessage{}, fmt.Errorf("kind is %q, expected %q", kind, expectedKind)
	}
	count, err := responseInt64(items[2], nil)
	if err != nil {
		return PubSubMessage{}, fmt.Errorf("invalid count: %w", err)
	}
	if count < 0 {
		return PubSubMessage{}, fmt.Errorf("count is negative: %d", count)
	}
	message := PubSubMessage{Kind: expectedKind, Count: count}
	if items[1] == nil {
		if expectedKind == "subscribe" || expectedKind == "psubscribe" {
			return PubSubMessage{}, errors.New("subscription target is nil")
		}
		return message, nil
	}
	target, err := responseString(items[1], nil)
	if err != nil {
		return PubSubMessage{}, fmt.Errorf("invalid target: %w", err)
	}
	if expectedKind == "psubscribe" || expectedKind == "punsubscribe" {
		message.Pattern = target
	} else {
		message.Channel = target
	}
	return message, nil
}

func nativeEventFromValue(value any) (NativeEvent, error) {
	m, err := nativeMap(value)
	if err != nil {
		return NativeEvent{}, err
	}
	name, err := responseString(m["event"], nil)
	if err != nil {
		return NativeEvent{}, err
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return NativeEvent{}, errors.New("native event name is empty")
	}
	payload := map[string]any{}
	if raw, present := m["payload"]; present && raw != nil {
		payload, err = nativeMap(raw)
		if err != nil {
			return NativeEvent{}, err
		}
	}
	var atMS int64
	if raw, present := m["at_ms"]; present && raw != nil {
		atMS, err = responseInt64(raw, nil)
		if err != nil {
			return NativeEvent{}, err
		}
		if atMS < 0 {
			return NativeEvent{}, errors.New("native event timestamp must be non-negative")
		}
	}
	return NativeEvent{
		Name:    name,
		Payload: payload,
		AtMS:    atMS,
		Raw:     m,
	}, nil
}

type nativeServerEvent struct {
	flags     byte
	laneID    uint32
	opcode    uint16
	value     any
	wireBytes int
}

func nativeServerEventValue(value any) any {
	if event, ok := value.(nativeServerEvent); ok {
		return event.value
	}
	return value
}

func nativeEventFromServerValue(value any) (NativeEvent, error) {
	serverEvent, ok := value.(nativeServerEvent)
	if !ok {
		return nativeEventFromValue(value)
	}
	if serverEvent.opcode == nativeOpGoAway {
		raw, err := nativeMap(serverEvent.value)
		if err != nil {
			return NativeEvent{}, err
		}
		return NativeEvent{
			Opcode: serverEvent.opcode, LaneID: serverEvent.laneID, Flags: serverEvent.flags,
			Name: "GOAWAY", Payload: raw, Raw: raw,
		}, nil
	}
	event, err := nativeEventFromValue(serverEvent.value)
	if err != nil {
		return NativeEvent{}, err
	}
	event.Opcode = serverEvent.opcode
	event.LaneID = serverEvent.laneID
	event.Flags = serverEvent.flags
	return event, nil
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

func pubSubStringList(value any, field string) ([]string, error) {
	if values, ok := value.([]string); ok {
		return append([]string(nil), values...), nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("event subscription %s returned %T, expected array", field, value)
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		text, err := responseString(item, nil)
		if err != nil {
			return nil, fmt.Errorf("event subscription %s: %w", field, err)
		}
		out = append(out, text)
	}
	return out, nil
}
