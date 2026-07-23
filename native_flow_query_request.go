package ferricstore

import (
	"errors"
	"fmt"
	"unicode/utf8"
)

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
