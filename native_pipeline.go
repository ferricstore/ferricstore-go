package ferricstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

// ErrPipelineNotExecuted marks commands that were not attempted after an
// earlier non-atomic pipeline chunk failed.
var ErrPipelineNotExecuted = errors.New("ferricstore pipeline command was not executed")

type pipelineChunkExecutionError struct {
	cause    error
	affected int
}

type pipelineCommandBuildError struct {
	index int
	cause error
}

func (e *pipelineCommandBuildError) Error() string {
	if e == nil || e.cause == nil {
		return ""
	}
	return e.cause.Error()
}

func (e *pipelineCommandBuildError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *pipelineChunkExecutionError) Error() string {
	if e == nil || e.cause == nil {
		return ""
	}
	return e.cause.Error()
}

func (e *pipelineChunkExecutionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func pipelineChunkAffectedCommands(err error, fallback int) int {
	var chunkErr *pipelineChunkExecutionError
	if errors.As(err, &chunkErr) && chunkErr.affected > 0 {
		return min(chunkErr.affected, fallback)
	}
	return fallback
}

func compactPipelinePayload(commands [][]any) ([]byte, bool, error) {
	return compactPipelinePayloadWithLimit(commands, nativeMaxFrameBytes)
}

func compactPipelinePayloadWithLimit(commands [][]any, limit int) ([]byte, bool, error) {
	plan, ok, err := compactPipelinePlanWithLimit(commands, limit)
	if !ok || err != nil {
		return nil, ok, err
	}
	return plan.encode(), true, nil
}

type nativeCompactPipelinePlan struct {
	kind     byte
	commands [][]any
	size     int
}

func compactPipelinePlanWithLimit(commands [][]any, limit int) (nativeCompactPipelinePlan, bool, error) {
	if len(commands) == 0 {
		return nativeCompactPipelinePlan{}, false, nil
	}
	if len(commands[0]) == 0 {
		return nativeCompactPipelinePlan{}, false, nil
	}
	first := strings.ToUpper(asString(commands[0][0]))
	switch first {
	case "SET":
		return compactSetPipelinePlanWithLimit(commands, limit)
	case "GET":
		return compactGetPipelinePlanWithLimit(commands, limit)
	default:
		return nativeCompactPipelinePlan{}, false, nil
	}
}

func compactSetPipelinePayload(commands [][]any) ([]byte, bool, error) {
	return compactSetPipelinePayloadWithLimit(commands, nativeMaxFrameBytes)
}

func compactSetPipelinePayloadWithLimit(commands [][]any, limit int) ([]byte, bool, error) {
	plan, ok, err := compactSetPipelinePlanWithLimit(commands, limit)
	if !ok || err != nil {
		return nil, ok, err
	}
	return plan.encode(), true, nil
}

func compactSetPipelinePlanWithLimit(commands [][]any, limit int) (nativeCompactPipelinePlan, bool, error) {
	size := 5
	if limit < size {
		return nativeCompactPipelinePlan{}, true, nativeEncodeLimitError{limit: limit}
	}
	for _, command := range commands {
		if len(command) != 3 || !strings.EqualFold(asString(command[0]), "SET") {
			return nativeCompactPipelinePlan{}, false, nil
		}
		key, keyOK := compactBytes(command[1])
		value, valueOK := compactBytes(command[2])
		if !keyOK || !valueOK {
			return nativeCompactPipelinePlan{}, false, nil
		}
		remaining := limit - size
		if remaining < 8 || len(key) > remaining-8 {
			return nativeCompactPipelinePlan{}, true, nativeEncodeLimitError{limit: limit}
		}
		remaining -= 8 + len(key)
		if len(value) > remaining {
			return nativeCompactPipelinePlan{}, true, nativeEncodeLimitError{limit: limit}
		}
		size = limit - (remaining - len(value))
	}
	return nativeCompactPipelinePlan{kind: 0x81, commands: commands, size: size}, true, nil
}

func compactGetPipelinePayload(commands [][]any) ([]byte, bool, error) {
	return compactGetPipelinePayloadWithLimit(commands, nativeMaxFrameBytes)
}

func compactGetPipelinePayloadWithLimit(commands [][]any, limit int) ([]byte, bool, error) {
	plan, ok, err := compactGetPipelinePlanWithLimit(commands, limit)
	if !ok || err != nil {
		return nil, ok, err
	}
	return plan.encode(), true, nil
}

func compactGetPipelinePlanWithLimit(commands [][]any, limit int) (nativeCompactPipelinePlan, bool, error) {
	size := 5
	if limit < size {
		return nativeCompactPipelinePlan{}, true, nativeEncodeLimitError{limit: limit}
	}
	for _, command := range commands {
		if len(command) != 2 || !strings.EqualFold(asString(command[0]), "GET") {
			return nativeCompactPipelinePlan{}, false, nil
		}
		key, ok := compactBytes(command[1])
		if !ok {
			return nativeCompactPipelinePlan{}, false, nil
		}
		remaining := limit - size
		if remaining < 4 || len(key) > remaining-4 {
			return nativeCompactPipelinePlan{}, true, nativeEncodeLimitError{limit: limit}
		}
		size += 4 + len(key)
	}
	return nativeCompactPipelinePlan{kind: 0x82, commands: commands, size: size}, true, nil
}

func (p nativeCompactPipelinePlan) encode() []byte {
	payload := make([]byte, 0, p.size)
	payload = append(payload, nativeCompactPipelineRequest, p.kind)
	payload = appendUint32(payload, uint32(len(p.commands)))
	for _, command := range p.commands {
		key, _ := compactBytes(command[1])
		payload = appendCompactBinary(payload, key)
		if p.kind == 0x81 {
			value, _ := compactBytes(command[2])
			payload = appendCompactBinary(payload, value)
		}
	}
	return payload
}

func (p nativeCompactPipelinePlan) encodeNativeCustomPayload(limit int) ([]byte, error) {
	if p.size > limit {
		return nil, nativeEncodeLimitError{limit: limit}
	}
	return p.encode(), nil
}

func appendCompactBinary(payload, value []byte) []byte {
	payload = appendUint32(payload, uint32(len(value)))
	return append(payload, value...)
}

func appendUint32(payload []byte, value uint32) []byte {
	offset := len(payload)
	payload = append(payload, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(payload[offset:offset+4], value)
	return payload
}

func pipelineValues(value any, expected int) ([]any, error) {
	results, err := pipelineItemResults(value, expected)
	if err != nil {
		return nil, err
	}
	return pipelineResultValues(results)
}

func pipelineItemResults(value any, expected int) ([]pipelineItemResult, error) {
	if count, ok := value.(nativeCompactOKCount); ok {
		if int(count) != expected {
			return nil, fmt.Errorf("PIPELINE returned OK count %d, expected %d", count, expected)
		}
		out := make([]pipelineItemResult, int(count))
		for idx := range out {
			out[idx].value = []byte("OK")
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
	out := make([]pipelineItemResult, 0, len(items))
	for _, item := range items {
		value, err := pipelineItemValue(item)
		out = append(out, pipelineItemResult{value: value, err: err})
	}
	return out, nil
}

func pipelineResultValues(results []pipelineItemResult) ([]any, error) {
	values := make([]any, len(results))
	failures := make([]PipelineFailure, 0)
	for index, result := range results {
		if result.err != nil {
			values[index] = result.err
			failures = append(failures, PipelineFailure{Index: index, Err: result.err})
			continue
		}
		values[index], _ = unwrapTypedCommandState(result.value)
	}
	if len(failures) > 0 {
		return values, &PipelineError{Failures: failures}
	}
	return values, nil
}

func markPipelineNotExecuted(results []pipelineItemResult, cause error) {
	for index := range results {
		results[index].err = fmt.Errorf("%w after earlier failure: %w", ErrPipelineNotExecuted, cause)
	}
}

func pipelineItemValue(item any) (any, error) {
	if pair, ok := item.([]any); ok && len(pair) == 2 {
		kind := strings.ToLower(asString(pair[0]))
		if kind == "ok" {
			return pair[1], nil
		}
		return nil, NativeError{Status: 1, Kind: kind, Value: pair[1]}
	}
	if mapping, ok := item.(map[string]any); ok {
		if status, ok := mapping["status"]; ok {
			kind := strings.ToLower(asString(status))
			if kind == "ok" {
				return mapping["value"], nil
			}
			return nil, NativeError{Status: 1, Kind: kind, Value: mapping["value"]}
		}
	}
	return item, nil
}
