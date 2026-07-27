package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"unicode/utf8"
)

type flowQueryParameter struct {
	name  string
	value any
}

type preparedFlowQuery struct {
	query      string
	parameters []flowQueryParameter
}

const flowQueryASCIIWhitespace = " \t\n\r"

func trimFlowQueryASCIIWhitespace(value string) string {
	return strings.Trim(value, flowQueryASCIIWhitespace)
}

func trimFlowQueryASCIIWhitespaceStart(value string) string {
	return strings.TrimLeft(value, flowQueryASCIIWhitespace)
}

// flowQueryExecutor lets built-in native transports retain validated, typed
// parameters through wire encoding. Custom and queueing executors continue to
// receive the public command argument shape through the fallback path.
type flowQueryExecutor interface {
	executePreparedFlowQuery(ctx context.Context, query preparedFlowQuery) (any, error)
}

func prepareFlowQuery(query string, params map[string]any) (preparedFlowQuery, error) {
	if err := validateFlowQueryText(query); err != nil {
		return preparedFlowQuery{}, err
	}
	if len(params) > flowQueryMaxParameters {
		return preparedFlowQuery{}, fmt.Errorf("FLOW.QUERY accepts at most %d named parameters", flowQueryMaxParameters)
	}
	parameters := make([]flowQueryParameter, 0, len(params))
	for name, value := range params {
		if !validFlowQueryParameterName(name) {
			return preparedFlowQuery{}, fmt.Errorf(
				"FLOW.QUERY parameter names must be 1..%d ASCII letters, digits, '_', '.', or '-'",
				flowQueryMaxParameterName,
			)
		}
		normalized, err := normalizeFlowQueryParameter(value)
		if err != nil {
			return preparedFlowQuery{}, fmt.Errorf("FLOW.QUERY parameter %q: %w", name, err)
		}
		parameters = append(parameters, flowQueryParameter{name: name, value: normalized})
	}
	slices.SortFunc(parameters, func(left, right flowQueryParameter) int {
		return strings.Compare(left.name, right.name)
	})
	return preparedFlowQuery{query: query, parameters: parameters}, nil
}

func (query preparedFlowQuery) commandArgs() []any {
	args := make([]any, 0, 3+len(query.parameters)*2)
	args = append(args, "FLOW.QUERY", flowQueryLanguageVersion, query.query)
	for _, parameter := range query.parameters {
		args = append(args, parameter.name, parameter.value)
	}
	return args
}

func flowQueryCommandArgs(query string, params map[string]any) ([]any, error) {
	prepared, err := prepareFlowQuery(query, params)
	if err != nil {
		return nil, err
	}
	return prepared.commandArgs(), nil
}

func validateFlowQueryText(query string) error {
	if !utf8.ValidString(query) {
		return errors.New("FLOW.QUERY query must be valid UTF-8")
	}
	if strings.TrimSpace(query) == "" {
		return errors.New("FLOW.QUERY query must not be empty")
	}
	if len(query) > flowQueryMaxBytes {
		return fmt.Errorf("FLOW.QUERY query exceeds %d bytes", flowQueryMaxBytes)
	}
	return nil
}

func normalizeFlowQueryParameter(value any) (any, error) {
	switch typed := value.(type) {
	case string:
		if !utf8.ValidString(typed) {
			return nil, errors.New("text values must be valid UTF-8")
		}
		if len(typed) > flowQueryMaxParameterValue {
			return nil, fmt.Errorf("value exceeds %d bytes", flowQueryMaxParameterValue)
		}
		return typed, nil
	case []byte:
		if len(typed) > flowQueryMaxParameterValue {
			return nil, fmt.Errorf("value exceeds %d bytes", flowQueryMaxParameterValue)
		}
		return typed, nil
	case bool:
		return typed, nil
	case float32:
		value := float64(typed)
		if !math.IsNaN(value) && !math.IsInf(value, 0) {
			return value, nil
		}
	case float64:
		if !math.IsNaN(typed) && !math.IsInf(typed, 0) {
			return typed, nil
		}
	case int:
		return int64(typed), nil
	case int8:
		return int64(typed), nil
	case int16:
		return int64(typed), nil
	case int32:
		return int64(typed), nil
	case int64:
		return typed, nil
	case uint:
		if uint64(typed) <= math.MaxInt64 {
			return int64(typed), nil
		}
	case uint8:
		return int64(typed), nil
	case uint16:
		return int64(typed), nil
	case uint32:
		return int64(typed), nil
	case uint64:
		if typed <= math.MaxInt64 {
			return int64(typed), nil
		}
	}
	return nil, errors.New("value must be a string, byte slice, boolean, finite float, or signed 64-bit integer")
}

func validFlowQueryParameterName(name string) bool {
	if len(name) == 0 || len(name) > flowQueryMaxParameterName {
		return false
	}
	for index := 0; index < len(name); index++ {
		value := name[index]
		if (value < 'a' || value > 'z') &&
			(value < 'A' || value > 'Z') &&
			(value < '0' || value > '9') &&
			value != '_' && value != '.' && value != '-' {
			return false
		}
	}
	return true
}
