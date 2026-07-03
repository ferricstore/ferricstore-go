package ferricstore

import (
	"encoding/binary"
	"fmt"
	"strings"
)

func compactPipelinePayload(commands [][]any) ([]byte, bool, error) {
	if len(commands) == 0 {
		return nil, false, nil
	}
	if len(commands[0]) == 0 {
		return nil, false, nil
	}
	first := strings.ToUpper(asString(commands[0][0]))
	switch first {
	case "SET":
		payload, ok, err := compactSetPipelinePayload(commands)
		return payload, ok, err
	case "GET":
		payload, ok, err := compactGetPipelinePayload(commands)
		return payload, ok, err
	default:
		return nil, false, nil
	}
}

func compactSetPipelinePayload(commands [][]any) ([]byte, bool, error) {
	size := 5
	for _, command := range commands {
		if len(command) != 3 || !strings.EqualFold(asString(command[0]), "SET") {
			return nil, false, nil
		}
		size += 8 + compactBinarySize(command[1]) + compactBinarySize(command[2])
	}
	payload := make([]byte, 0, size)
	payload = append(payload, nativeCompactPipelineRequest, 0x81)
	payload = appendUint32(payload, uint32(len(commands)))
	for _, command := range commands {
		payload = appendCompactAny(payload, command[1])
		payload = appendCompactAny(payload, command[2])
	}
	return payload, true, nil
}

func compactGetPipelinePayload(commands [][]any) ([]byte, bool, error) {
	size := 5
	for _, command := range commands {
		if len(command) != 2 || !strings.EqualFold(asString(command[0]), "GET") {
			return nil, false, nil
		}
		size += 4 + compactBinarySize(command[1])
	}
	payload := make([]byte, 0, size)
	payload = append(payload, nativeCompactPipelineRequest, 0x82)
	payload = appendUint32(payload, uint32(len(commands)))
	for _, command := range commands {
		payload = appendCompactAny(payload, command[1])
	}
	return payload, true, nil
}

func compactBinarySize(value any) int {
	switch v := value.(type) {
	case nil:
		return 0
	case string:
		return len(v)
	case []byte:
		return len(v)
	default:
		return len(asBytes(v))
	}
}

func appendCompactAny(payload []byte, value any) []byte {
	switch v := value.(type) {
	case nil:
		return appendUint32(payload, 0)
	case string:
		payload = appendUint32(payload, uint32(len(v)))
		return append(payload, v...)
	case []byte:
		payload = appendUint32(payload, uint32(len(v)))
		return append(payload, v...)
	default:
		bytes := asBytes(v)
		payload = appendUint32(payload, uint32(len(bytes)))
		return append(payload, bytes...)
	}
}

func appendUint32(payload []byte, value uint32) []byte {
	offset := len(payload)
	payload = append(payload, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(payload[offset:offset+4], value)
	return payload
}

func pipelineValues(value any, expected int) ([]any, error) {
	if count, ok := value.(nativeCompactOKCount); ok {
		if int(count) != expected {
			return nil, fmt.Errorf("PIPELINE returned OK count %d, expected %d", count, expected)
		}
		out := make([]any, int(count))
		for idx := range out {
			out[idx] = []byte("OK")
		}
		return out, nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("PIPELINE returned %T, expected array", value)
	}
	if len(items) != expected {
		return nil, fmt.Errorf("PIPELINE returned %d results, expected %d", len(items), expected)
	}
	out := make([]any, 0, len(items))
	for _, item := range items {
		value, err := pipelineItemValue(item)
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, nil
}

func pipelineItemValue(item any) (any, error) {
	if pair, ok := item.([]any); ok && len(pair) == 2 {
		if strings.EqualFold(asString(pair[0]), "ok") {
			return pair[1], nil
		}
		return nil, NativeError{Status: 1, Value: pair[1]}
	}
	if mapping, ok := item.(map[string]any); ok {
		if status, ok := mapping["status"]; ok {
			if strings.EqualFold(asString(status), "ok") {
				return mapping["value"], nil
			}
			return nil, NativeError{Status: 1, Value: mapping["value"]}
		}
	}
	return item, nil
}
