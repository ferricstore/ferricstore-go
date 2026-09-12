package ferricstore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

type nativePipelineBodyProvider interface {
	nativePipelineBody() (any, error)
}

func (e *NativeExecutor) pipelineChunkWithoutGate(ctx context.Context, commands [][]any, laneID uint32, maxFrameBytes int) ([]pipelineItemResult, error) {
	payload, flags, policy, err := nativePipelinePayloadWithCapabilities(
		commands, laneID, maxFrameBytes,
		e.supportsCompactStreamXAdd(), e.supportsCompactPubSubPublish(),
	)
	if err != nil {
		var limitErr nativeEncodeLimitError
		if !errors.As(err, &limitErr) {
			return nil, err
		}
		return e.splitOversizedPipelineChunk(ctx, commands, laneID, maxFrameBytes)
	}
	value, err := e.requestWithoutSessionGateWithReplayPolicy(
		ctx, nativeOpPipeline, laneID, payload, flags, policy.budget, policy.replayPolicy,
	)
	if err != nil {
		var limitErr nativeEncodeLimitError
		if errors.As(err, &limitErr) {
			return e.splitOversizedPipelineChunk(ctx, commands, laneID, maxFrameBytes)
		}
		return nil, &pipelineChunkExecutionError{
			cause:    fmt.Errorf("PIPELINE: %w", err),
			affected: len(commands),
		}
	}
	items, err := pipelineItemResults(value, len(commands))
	if err != nil {
		return nil, err
	}
	if delay, retry := retryableBusyPipeline(items); retry && policy.replayPolicy != nativeReplayNever {
		if err := waitNativeRetry(ctx, delay); err != nil {
			return nil, err
		}
		value, err = e.requestWithoutSessionGateWithReplayPolicy(
			ctx, nativeOpPipeline, laneID, payload, flags, policy.budget, policy.replayPolicy,
		)
		if err != nil {
			return nil, &pipelineChunkExecutionError{
				cause:    fmt.Errorf("PIPELINE: %w", err),
				affected: len(commands),
			}
		}
		return pipelineItemResults(value, len(commands))
	}
	return items, nil
}

func (e *NativeExecutor) splitOversizedPipelineChunk(ctx context.Context, commands [][]any, laneID uint32, maxFrameBytes int) ([]pipelineItemResult, error) {
	if len(commands) == 1 {
		return nil, &pipelineChunkExecutionError{
			cause:    fmt.Errorf("PIPELINE command exceeds server-advertised %d-byte frame limit", maxFrameBytes),
			affected: 1,
		}
	}
	middle := len(commands) / 2
	left, err := e.pipelineChunkWithoutGate(ctx, commands[:middle], laneID, maxFrameBytes)
	if err != nil {
		return left, err
	}
	right, err := e.pipelineChunkWithoutGate(ctx, commands[middle:], laneID, maxFrameBytes)
	if err != nil {
		return append(left, right...), err
	}
	return append(left, right...), nil
}

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

const (
	nativeCompactPipelineHeaderBytes     = 6
	nativeCompactSetPipelineValuesOnly   = 0x81
	nativeCompactGetPipelineValuesOnly   = 0x82
	nativeCompactStreamXAddValuesOnly    = 0x80 | 34
	nativeCompactPubSubPublishValuesOnly = 0x80 | 35
	nativeCompactStreamXAddMaxFieldPairs = 1<<16 - 1
)

func compactPipelinePlanWithLimit(commands [][]any, limit int) (nativeCompactPipelinePlan, bool, error) {
	if len(commands) == 0 {
		return nativeCompactPipelinePlan{}, false, nil
	}
	if len(commands[0]) == 0 {
		return nativeCompactPipelinePlan{}, false, nil
	}
	first := commandPart(commands[0][0])
	switch first {
	case "SET":
		return compactSetPipelinePlanWithLimit(commands, limit)
	case "GET":
		return compactGetPipelinePlanWithLimit(commands, limit)
	case "XADD":
		return compactStreamXAddPipelinePlanWithLimit(commands, limit)
	case "PUBLISH":
		return compactPubSubPublishPipelinePlanWithLimit(commands, limit)
	default:
		return nativeCompactPipelinePlan{}, false, nil
	}
}

func compactPubSubPublishPipelinePlanWithLimit(commands [][]any, limit int) (nativeCompactPipelinePlan, bool, error) {
	size := nativeCompactPipelineHeaderBytes
	if limit < size {
		return nativeCompactPipelinePlan{}, true, nativeEncodeLimitError{limit: limit}
	}
	for _, command := range commands {
		if len(command) != 3 || commandPart(command[0]) != "PUBLISH" {
			return nativeCompactPipelinePlan{}, false, nil
		}
		channel, channelOK := compactBytes(command[1])
		message, messageOK := compactBytes(command[2])
		if !channelOK || !messageOK {
			return nativeCompactPipelinePlan{}, false, nil
		}
		remaining := limit - size
		if remaining < 8 || len(channel) > remaining-8 {
			return nativeCompactPipelinePlan{}, true, nativeEncodeLimitError{limit: limit}
		}
		remaining -= 8 + len(channel)
		if len(message) > remaining {
			return nativeCompactPipelinePlan{}, true, nativeEncodeLimitError{limit: limit}
		}
		size = limit - (remaining - len(message))
	}
	return nativeCompactPipelinePlan{
		kind: nativeCompactPubSubPublishValuesOnly, commands: commands, size: size,
	}, true, nil
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
	size := nativeCompactPipelineHeaderBytes
	if limit < size {
		return nativeCompactPipelinePlan{}, true, nativeEncodeLimitError{limit: limit}
	}
	for _, command := range commands {
		if len(command) != 3 || commandPart(command[0]) != "SET" {
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
	return nativeCompactPipelinePlan{kind: nativeCompactSetPipelineValuesOnly, commands: commands, size: size}, true, nil
}

func compactGetPipelinePlanWithLimit(commands [][]any, limit int) (nativeCompactPipelinePlan, bool, error) {
	size := nativeCompactPipelineHeaderBytes
	if limit < size {
		return nativeCompactPipelinePlan{}, true, nativeEncodeLimitError{limit: limit}
	}
	for _, command := range commands {
		if len(command) != 2 || commandPart(command[0]) != "GET" {
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
	return nativeCompactPipelinePlan{kind: nativeCompactGetPipelineValuesOnly, commands: commands, size: size}, true, nil
}

func compactStreamXAddPipelinePlanWithLimit(commands [][]any, limit int) (nativeCompactPipelinePlan, bool, error) {
	size := nativeCompactPipelineHeaderBytes
	if limit < size {
		return nativeCompactPipelinePlan{}, true, nativeEncodeLimitError{limit: limit}
	}
	for _, command := range commands {
		if _, ok := compactStreamXAddPairCount(command); !ok {
			return nativeCompactPipelinePlan{}, false, nil
		}
		key, ok := compactBytes(command[1])
		if !ok {
			return nativeCompactPipelinePlan{}, false, nil
		}
		remaining := limit - size
		if remaining < 6 || len(key) > remaining-6 {
			return nativeCompactPipelinePlan{}, true, nativeEncodeLimitError{limit: limit}
		}
		remaining -= 6 + len(key)
		for _, rawValue := range command[3:] {
			value, ok := compactBytes(rawValue)
			if !ok {
				return nativeCompactPipelinePlan{}, false, nil
			}
			if remaining < 4 || len(value) > remaining-4 {
				return nativeCompactPipelinePlan{}, true, nativeEncodeLimitError{limit: limit}
			}
			remaining -= 4 + len(value)
		}
		size = limit - remaining
	}
	return nativeCompactPipelinePlan{
		kind: nativeCompactStreamXAddValuesOnly, commands: commands, size: size,
	}, true, nil
}

func compactStreamXAddPairCount(command []any) (int, bool) {
	if len(command) < 5 || (len(command)-3)%2 != 0 ||
		commandPart(command[0]) != "XADD" || commandPart(command[2]) != "*" {
		return 0, false
	}
	pairCount := (len(command) - 3) / 2
	return pairCount, pairCount <= nativeCompactStreamXAddMaxFieldPairs
}

func (p nativeCompactPipelinePlan) encode() []byte {
	payload := make([]byte, 0, p.size)
	payload = append(payload, nativeCompactPipelineRequest, p.kind)
	payload = appendUint32(payload, uint32(len(p.commands)))
	for _, command := range p.commands {
		key, _ := compactBytes(command[1])
		payload = appendCompactBinary(payload, key)
		switch p.kind {
		case nativeCompactSetPipelineValuesOnly, nativeCompactPubSubPublishValuesOnly:
			value, _ := compactBytes(command[2])
			payload = appendCompactBinary(payload, value)
		case nativeCompactStreamXAddValuesOnly:
			pairCount, _ := compactStreamXAddPairCount(command)
			payload = appendUint16(payload, uint16(pairCount))
			for _, rawValue := range command[3:] {
				value, _ := compactBytes(rawValue)
				payload = appendCompactBinary(payload, value)
			}
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

func appendUint16(payload []byte, value uint16) []byte {
	offset := len(payload)
	payload = append(payload, 0, 0)
	binary.BigEndian.PutUint16(payload[offset:offset+2], value)
	return payload
}

func pipelineItemResults(value any, expected int) ([]pipelineItemResult, error) {
	if compact, ok := value.(nativeCompactPipelineValues); ok {
		return compactPipelineItemResults(compact.value, expected)
	}
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

type nativeCompactPipelineValues struct {
	value any
}

func compactPipelineItemResults(value any, expected int) ([]pipelineItemResult, error) {
	switch values := value.(type) {
	case []any:
		if len(values) != expected {
			return nil, fmt.Errorf("PIPELINE returned %d compact values, expected %d", len(values), expected)
		}
		out := make([]pipelineItemResult, len(values))
		for index := range values {
			out[index].value = values[index]
		}
		return out, nil
	case []ClaimedItem:
		if len(values) != expected {
			return nil, fmt.Errorf("PIPELINE returned %d compact values, expected %d", len(values), expected)
		}
		out := make([]pipelineItemResult, len(values))
		for index := range values {
			out[index].value = values[index]
		}
		return out, nil
	default:
		if expected != 1 {
			return nil, fmt.Errorf("PIPELINE returned one compact value, expected %d", expected)
		}
		return []pipelineItemResult{{value: value}}, nil
	}
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
