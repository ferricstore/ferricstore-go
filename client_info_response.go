package ferricstore

import (
	"errors"
	"fmt"
	"strings"
)

func clientInfoResponse(value any) (map[string]any, error) {
	var text string
	switch typed := value.(type) {
	case map[interface{}]interface{}, map[string]any, []any:
		result, err := nativeMap(value)
		if err != nil {
			return nil, err
		}
		if len(result) == 0 {
			return nil, errors.New("expected non-empty CLIENT INFO map")
		}
		return result, nil
	case string:
		text = typed
	case []byte:
		text = string(typed)
	default:
		return nil, fmt.Errorf("expected CLIENT INFO map or text, got %T", value)
	}

	result := make(map[string]any)
	for position := skipClientInfoSpace(text, 0); position < len(text); {
		keyStart := position
		for position < len(text) && isClientInfoKeyByte(text[position]) {
			position++
		}
		if position == keyStart || position >= len(text) || text[position] != '=' {
			return nil, fmt.Errorf("malformed CLIENT INFO field at byte %d", keyStart)
		}
		key := text[keyStart:position]
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("duplicate CLIENT INFO field %q", key)
		}
		position++
		valueStart := position
		valueEnd := len(text)
		nextPosition := len(text)
		for position < len(text) {
			if !isClientInfoSpace(text[position]) {
				position++
				continue
			}
			candidate := skipClientInfoSpace(text, position)
			if _, ok := clientInfoKeyAt(text, candidate); ok {
				valueEnd = position
				nextPosition = candidate
				break
			}
			position = candidate
		}
		result[key] = coerceTextValue(strings.TrimSpace(text[valueStart:valueEnd]))
		position = nextPosition
	}
	if len(result) == 0 {
		return nil, errors.New("expected non-empty CLIENT INFO response")
	}
	if err := validateClientInfoFields(result); err != nil {
		return nil, err
	}
	return result, nil
}

func clientInfoKeyAt(text string, position int) (string, bool) {
	start := position
	for position < len(text) && isClientInfoKeyByte(text[position]) {
		position++
	}
	if position == start || position >= len(text) || text[position] != '=' {
		return "", false
	}
	return text[start:position], true
}

func skipClientInfoSpace(text string, position int) int {
	for position < len(text) && isClientInfoSpace(text[position]) {
		position++
	}
	return position
}

func isClientInfoSpace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\r' || value == '\n'
}

func isClientInfoKeyByte(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' ||
		value >= '0' && value <= '9' || value == '_' || value == '-'
}

func validateClientInfoFields(fields map[string]any) error {
	for _, name := range []string{"id", "fd", "age"} {
		value, exists := fields[name]
		if !exists {
			continue
		}
		number, err := responseInt64(value, nil)
		if err != nil || number < 0 {
			return fmt.Errorf("CLIENT INFO field %q must be a non-negative integer", name)
		}
	}
	if value, exists := fields["addr"]; exists {
		address, err := responseString(value, nil)
		if err != nil || strings.TrimSpace(address) == "" {
			return errors.New("CLIENT INFO field \"addr\" must be a non-empty string")
		}
	}
	if value, exists := fields["name"]; exists {
		if _, err := responseString(value, nil); err != nil {
			return errors.New("CLIENT INFO field \"name\" must be a string")
		}
	}
	return nil
}
