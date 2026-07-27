package ferricstore

import (
	"errors"
	"math"
	"unicode/utf8"
)

const (
	flowQueryDiagnosticTextBytes       = 1_024
	flowQueryDiagnosticContextEntries  = 16
	flowQueryDiagnosticContextItems    = 32
	flowQueryDiagnosticContextKeyBytes = 128
	flowQueryDiagnosticContextDepth    = 6
	flowQueryDiagnosticContextNodes    = 512
)

func requiredFlowQueryDiagnosticText(mapping map[string]any, key string) (string, error) {
	value, err := requiredFlowQueryStringField(mapping, key, "FLOW.QUERY error")
	if err != nil || len(value) > flowQueryDiagnosticTextBytes {
		return "", errors.New("diagnostic text is missing or exceeds its byte limit")
	}
	return value, nil
}

func optionalFlowQueryDiagnosticText(mapping map[string]any, key string) (string, error) {
	raw, present := mapping[key]
	if !present || raw == nil {
		return "", nil
	}
	value, err := flowQueryResponseString(raw, "FLOW.QUERY error "+key)
	if err != nil || len(value) > flowQueryDiagnosticTextBytes {
		return "", errors.New("diagnostic optional text exceeds its byte limit")
	}
	return value, nil
}

func validateFlowQueryDiagnosticContext(value map[string]any) error {
	if len(value) > flowQueryDiagnosticContextEntries {
		return errors.New("diagnostic context contains too many entries")
	}
	remaining := flowQueryDiagnosticContextNodes
	return validateFlowQueryDiagnosticContextValue(value, flowQueryDiagnosticContextDepth, &remaining)
}

func validateFlowQueryDiagnosticContextValue(value any, depth int, remaining *int) error {
	if *remaining <= 0 {
		return errors.New("diagnostic context exceeds its node limit")
	}
	*remaining--
	switch typed := value.(type) {
	case nil, bool:
		return nil
	case int, int8, int16, int32, int64:
		return nil
	case uint:
		if uint64(typed) <= math.MaxInt64 {
			return nil
		}
	case uint8:
		return nil
	case uint16:
		return nil
	case uint32:
		return nil
	case uint64:
		if typed <= math.MaxInt64 {
			return nil
		}
	case string:
		if utf8.ValidString(typed) && len(typed) <= flowQueryDiagnosticTextBytes {
			return nil
		}
	case []byte:
		if utf8.Valid(typed) && len(typed) <= flowQueryDiagnosticTextBytes {
			return nil
		}
	case map[string]any:
		if depth <= 0 || len(typed) > flowQueryDiagnosticContextEntries {
			break
		}
		for key, item := range typed {
			if key == "" || len(key) > flowQueryDiagnosticContextKeyBytes || !utf8.ValidString(key) {
				return errors.New("diagnostic context contains an invalid key")
			}
			if err := validateFlowQueryDiagnosticContextValue(item, depth-1, remaining); err != nil {
				return err
			}
		}
		return nil
	case []any:
		if depth <= 0 || len(typed) > flowQueryDiagnosticContextItems {
			break
		}
		for _, item := range typed {
			if err := validateFlowQueryDiagnosticContextValue(item, depth-1, remaining); err != nil {
				return err
			}
		}
		return nil
	}
	return errors.New("diagnostic context contains an invalid value")
}
