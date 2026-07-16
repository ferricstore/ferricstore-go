package ferricstore

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func nativeMap(value any) (map[string]any, error) {
	budget := nativeNormalizeBudget{remaining: nativeMaxContainerItems}
	return nativeMapDepth(value, &budget, 0)
}

type nativeNormalizeBudget struct {
	remaining int
}

func nativeMapDepth(value any, budget *nativeNormalizeBudget, depth int) (map[string]any, error) {
	if depth > nativeMaxDecodeDepth {
		return nil, fmt.Errorf("native response exceeds maximum nesting depth %d", nativeMaxDecodeDepth)
	}
	switch v := value.(type) {
	case map[interface{}]interface{}:
		if err := budget.consume(len(v)); err != nil {
			return nil, err
		}
		out := make(map[string]any, len(v))
		for key, val := range v {
			name, err := nativeMapKey(key)
			if err != nil {
				return nil, err
			}
			if _, exists := out[name]; exists {
				return nil, fmt.Errorf("duplicate native map key %q", name)
			}
			normalized, err := normalizeNativeCheckedDepth(val, budget, depth+1)
			if err != nil {
				return nil, err
			}
			out[name] = normalized
		}
		return out, nil
	case map[string]any:
		if err := budget.consume(len(v)); err != nil {
			return nil, err
		}
		out := make(map[string]any, len(v))
		for key, val := range v {
			normalized, err := normalizeNativeCheckedDepth(val, budget, depth+1)
			if err != nil {
				return nil, err
			}
			out[key] = normalized
		}
		return out, nil
	case []any:
		return pairArrayMapDepth(v, budget, depth)
	default:
		return nil, fmt.Errorf("expected native map, got %T", value)
	}
}

func pairArrayMapDepth(items []any, budget *nativeNormalizeBudget, depth int) (map[string]any, error) {
	if depth > nativeMaxDecodeDepth {
		return nil, fmt.Errorf("native response exceeds maximum nesting depth %d", nativeMaxDecodeDepth)
	}
	if err := budget.consume(len(items)); err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return map[string]any{}, nil
	}
	if allPairs(items) {
		out := make(map[string]any, len(items))
		for _, pair := range items {
			p := pair.([]any)
			if err := budget.consume(len(p)); err != nil {
				return nil, err
			}
			key, err := nativeMapKey(p[0])
			if err != nil {
				return nil, err
			}
			if _, exists := out[key]; exists {
				return nil, fmt.Errorf("duplicate native map key %q", key)
			}
			normalized, err := normalizeNativeCheckedDepth(p[1], budget, depth+2)
			if err != nil {
				return nil, err
			}
			out[key] = normalized
		}
		return out, nil
	}
	if len(items)%2 != 0 {
		return nil, fmt.Errorf("odd native map array length %d", len(items))
	}
	out := make(map[string]any, len(items)/2)
	for i := 0; i < len(items); i += 2 {
		key, err := nativeMapKey(items[i])
		if err != nil {
			return nil, err
		}
		if _, exists := out[key]; exists {
			return nil, fmt.Errorf("duplicate native map key %q", key)
		}
		normalized, err := normalizeNativeCheckedDepth(items[i+1], budget, depth+1)
		if err != nil {
			return nil, err
		}
		out[key] = normalized
	}
	return out, nil
}

func nativeMapKey(value any) (string, error) {
	switch key := value.(type) {
	case string:
		return key, nil
	case []byte:
		return string(key), nil
	default:
		return "", fmt.Errorf("native map key must be string or []byte, got %T", value)
	}
}

func normalizeNativeCheckedDepth(value any, budget *nativeNormalizeBudget, depth int) (any, error) {
	if depth > nativeMaxDecodeDepth {
		return nil, fmt.Errorf("native response exceeds maximum nesting depth %d", nativeMaxDecodeDepth)
	}
	switch v := value.(type) {
	case map[interface{}]interface{}:
		return nativeMapDepth(v, budget, depth)
	case map[string]any:
		return nativeMapDepth(v, budget, depth)
	case []interface{}:
		if err := budget.consume(len(v)); err != nil {
			return nil, err
		}
		out := make([]any, len(v))
		for i, item := range v {
			normalized, err := normalizeNativeCheckedDepth(item, budget, depth+1)
			if err != nil {
				return nil, err
			}
			out[i] = normalized
		}
		return out, nil
	default:
		return value, nil
	}
}

func (budget *nativeNormalizeBudget) consume(count int) error {
	if count < 0 || count > budget.remaining {
		return fmt.Errorf("native response exceeds maximum item count %d", nativeMaxContainerItems)
	}
	budget.remaining -= count
	return nil
}

func allPairs(items []any) bool {
	for _, item := range items {
		pair, ok := item.([]any)
		if !ok || len(pair) != 2 {
			return false
		}
	}
	return true
}

func kvResponse(value any) (map[string]any, error) {
	if value == nil {
		return map[string]any{}, nil
	}
	var text string
	switch typed := value.(type) {
	case map[interface{}]interface{}, map[string]any, []any:
		return nativeMap(value)
	case string:
		text = typed
	case []byte:
		text = string(typed)
	default:
		return nil, fmt.Errorf("expected key/value map or text response, got %T", value)
	}
	out := make(map[string]any)
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, raw, ok := strings.Cut(line, ":")
		if !ok {
			key, raw, ok = strings.Cut(line, "=")
			if !ok {
				fields := strings.Fields(line)
				if len(fields) < 2 {
					return nil, fmt.Errorf("malformed key/value text line %q", line)
				}
				key = fields[0]
				raw = strings.Join(fields[1:], " ")
			}
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, errors.New("key/value text response contains an empty key")
		}
		if _, exists := out[key]; exists {
			return nil, fmt.Errorf("duplicate key/value text field %q", key)
		}
		out[key] = coerceTextValue(strings.TrimSpace(raw))
	}
	if len(out) == 0 && strings.TrimSpace(text) != "" {
		return nil, errors.New("expected key/value text response")
	}
	return out, nil
}

func coerceTextValue(value string) any {
	if value == "" {
		return ""
	}
	if n, err := strconv.ParseInt(value, 10, 64); err == nil {
		return n
	}
	if f, err := strconv.ParseFloat(value, 64); err == nil {
		return f
	}
	lower := strings.ToLower(value)
	if lower == "true" || lower == "yes" || lower == "ok" {
		return true
	}
	if lower == "false" || lower == "no" {
		return false
	}
	return value
}
