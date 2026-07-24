package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
	"unicode/utf8"
)

type nativeFlowQueryPayload struct {
	query      string
	parameters []flowQueryParameter
	deadlineMS int64
}

func (payload nativeFlowQueryPayload) nativeEncodedSize(limit int) (int, error) {
	size := 0
	add := func(part int) error {
		if part < 0 || part > limit-size {
			return nativeEncodeLimitError{limit: limit}
		}
		size += part
		return nil
	}
	addBinary := func(length int) error {
		if err := add(5); err != nil {
			return err
		}
		return add(length)
	}
	addMapKey := func(name string) error {
		if err := add(4); err != nil {
			return err
		}
		return add(len(name))
	}
	if err := add(5); err != nil {
		return 0, err
	}
	if payload.deadlineMS > 0 {
		if err := addMapKey("deadline_ms"); err != nil {
			return 0, err
		}
		if err := add(9); err != nil {
			return 0, err
		}
	}
	if len(payload.parameters) > 0 {
		if err := addMapKey("params"); err != nil {
			return 0, err
		}
		if err := add(5); err != nil {
			return 0, err
		}
		for _, parameter := range payload.parameters {
			if err := addMapKey(parameter.name); err != nil {
				return 0, err
			}
			switch value := parameter.value.(type) {
			case string:
				if err := addBinary(len(value)); err != nil {
					return 0, err
				}
			case []byte:
				if err := addBinary(len(value)); err != nil {
					return 0, err
				}
			case bool:
				if err := add(1); err != nil {
					return 0, err
				}
			case int64, float64:
				if err := add(9); err != nil {
					return 0, err
				}
			default:
				return 0, fmt.Errorf("FLOW.QUERY prepared parameter %q has unsupported type %T", parameter.name, parameter.value)
			}
		}
	}
	if err := addMapKey("query"); err != nil {
		return 0, err
	}
	if err := addBinary(len(payload.query)); err != nil {
		return 0, err
	}
	if err := addMapKey("version"); err != nil {
		return 0, err
	}
	if err := addBinary(len(flowQueryLanguageVersion)); err != nil {
		return 0, err
	}
	return size, nil
}

func newNativeFlowQueryCommand(query preparedFlowQuery) nativeCommand {
	return nativeCommand{
		name:    "FLOW.QUERY",
		opcode:  nativeOpFlowQuery,
		laneID:  1,
		payload: nativeFlowQueryPayload{query: query.query, parameters: query.parameters},
	}
}

func newNativeFlowQueryCommandForContext(ctx context.Context, query preparedFlowQuery) nativeCommand {
	command := newNativeFlowQueryCommand(query)
	payload := command.payload.(nativeFlowQueryPayload)
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok {
			payload.deadlineMS = deadline.UnixMilli()
			if payload.deadlineMS > 0 && payload.deadlineMS < math.MaxInt64 && deadline.After(time.UnixMilli(payload.deadlineMS)) {
				payload.deadlineMS++
			}
			if payload.deadlineMS < 0 {
				payload.deadlineMS = 0
			}
		}
	}
	command.payload = payload
	return command
}

func writeNativeFlowQueryPayload(
	buf *nativeEncodeBuffer,
	payload nativeFlowQueryPayload,
	state *nativeEncodeState,
	depth int,
) error {
	fieldCount := 2
	if payload.deadlineMS > 0 {
		fieldCount++
	}
	if len(payload.parameters) > 0 {
		fieldCount++
	}
	if err := ensureNativeEncodeContainerBudget("map", fieldCount, state.remaining); err != nil {
		return err
	}
	if err := writeNativeContainerHeader(buf, 6, fieldCount); err != nil {
		return err
	}
	if payload.deadlineMS > 0 {
		if err := writeNativeMapKey(buf, "deadline_ms"); err != nil {
			return err
		}
		if err := writeNativeValue(buf, payload.deadlineMS, state, depth+1); err != nil {
			return err
		}
	}
	if len(payload.parameters) > 0 {
		if err := writeNativeMapKey(buf, "params"); err != nil {
			return err
		}
		leave, err := state.enter(struct{}{}, depth+1)
		if err != nil {
			return err
		}
		defer leave()
		if err := ensureNativeEncodeContainerBudget("map", len(payload.parameters), state.remaining); err != nil {
			return err
		}
		if err := writeNativeContainerHeader(buf, 6, len(payload.parameters)); err != nil {
			return err
		}
		for _, parameter := range payload.parameters {
			if err := writeNativeMapKey(buf, parameter.name); err != nil {
				return err
			}
			if err := writeNativeValue(buf, parameter.value, state, depth+2); err != nil {
				return err
			}
		}
	}
	if err := writeNativeMapKey(buf, "query"); err != nil {
		return err
	}
	if err := writeNativeValue(buf, payload.query, state, depth+1); err != nil {
		return err
	}
	if err := writeNativeMapKey(buf, "version"); err != nil {
		return err
	}
	return writeNativeValue(buf, flowQueryLanguageVersion, state, depth+1)
}

func buildFlowQueryRequestNative(args []any) (nativeCommand, bool, error) {
	if len(args) < 2 {
		return nativeCommand{}, true, errors.New("FLOW.QUERY requires version and query")
	}
	if len(args)%2 != 0 {
		return nativeCommand{}, true, errors.New("FLOW.QUERY parameters must be name/value pairs")
	}
	version, ok := commandText(args[0])
	if !ok || version != flowQueryLanguageVersion {
		return nativeCommand{}, true, fmt.Errorf("FLOW.QUERY requires version %s", flowQueryLanguageVersion)
	}
	query, ok := commandText(args[1])
	if !ok {
		return nativeCommand{}, true, errors.New("FLOW.QUERY query must be text")
	}
	if err := validateFlowQueryText(query); err != nil {
		return nativeCommand{}, true, err
	}
	parameterCount := (len(args) - 2) / 2
	if parameterCount > flowQueryMaxParameters {
		return nativeCommand{}, true, fmt.Errorf("FLOW.QUERY accepts at most %d named parameters", flowQueryMaxParameters)
	}
	payload := map[string]any{"version": version, "query": query}
	if parameterCount > 0 {
		params := make(map[string]any, parameterCount)
		for index := 2; index < len(args); index += 2 {
			name, ok := commandText(args[index])
			if !ok || !utf8.ValidString(name) || name == "" || len(name) > flowQueryMaxParameterName {
				return nativeCommand{}, true, fmt.Errorf("FLOW.QUERY parameter names must be 1..%d bytes", flowQueryMaxParameterName)
			}
			if _, exists := params[name]; exists {
				return nativeCommand{}, true, fmt.Errorf("FLOW.QUERY parameter %q is duplicated", name)
			}
			value, err := normalizeFlowQueryParameter(args[index+1])
			if err != nil {
				return nativeCommand{}, true, fmt.Errorf("FLOW.QUERY parameter %q: %w", name, err)
			}
			params[name] = value
		}
		payload["params"] = params
	}
	return nativeCommand{name: "FLOW.QUERY", opcode: nativeOpFlowQuery, laneID: 1, payload: payload}, true, nil
}
