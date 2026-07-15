package ferricstore

import (
	"fmt"
	"strconv"
	"strings"
)

func nativeMap(value any) (map[string]any, error) {
	switch v := value.(type) {
	case map[interface{}]interface{}:
		out := make(map[string]any, len(v))
		for key, val := range v {
			name, err := nativeMapKey(key)
			if err != nil {
				return nil, err
			}
			if _, exists := out[name]; exists {
				return nil, fmt.Errorf("duplicate native map key %q", name)
			}
			normalized, err := normalizeNativeChecked(val)
			if err != nil {
				return nil, err
			}
			out[name] = normalized
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, val := range v {
			normalized, err := normalizeNativeChecked(val)
			if err != nil {
				return nil, err
			}
			out[key] = normalized
		}
		return out, nil
	case []any:
		return pairArrayMap(v)
	default:
		return nil, fmt.Errorf("expected native map, got %T", value)
	}
}

func pairArrayMap(items []any) (map[string]any, error) {
	if len(items) == 0 {
		return map[string]any{}, nil
	}
	if allPairs(items) {
		out := make(map[string]any, len(items))
		for _, pair := range items {
			p := pair.([]any)
			key, err := nativeMapKey(p[0])
			if err != nil {
				return nil, err
			}
			if _, exists := out[key]; exists {
				return nil, fmt.Errorf("duplicate native map key %q", key)
			}
			normalized, err := normalizeNativeChecked(p[1])
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
		normalized, err := normalizeNativeChecked(items[i+1])
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

func normalizeNativeChecked(value any) (any, error) {
	switch v := value.(type) {
	case map[interface{}]interface{}:
		return nativeMap(v)
	case map[string]any:
		return nativeMap(v)
	case []interface{}:
		out := make([]any, len(v))
		for i, item := range v {
			normalized, err := normalizeNativeChecked(item)
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

func allPairs(items []any) bool {
	for _, item := range items {
		pair, ok := item.([]any)
		if !ok || len(pair) != 2 {
			return false
		}
	}
	return true
}

func stringObjectMap(value any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	mapping, err := nativeMap(value)
	if err != nil {
		return map[string]any{}
	}
	return mapping
}

func kvResponse(value any) (map[string]any, error) {
	if value == nil {
		return map[string]any{}, nil
	}
	if mapping, err := nativeMap(value); err == nil {
		return mapping, nil
	}
	text := asString(value)
	out := make(map[string]any)
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		key, raw, ok := strings.Cut(line, ":")
		if !ok {
			key, raw, ok = strings.Cut(line, "=")
			if !ok {
				fields := strings.Fields(line)
				if len(fields) < 2 {
					continue
				}
				key = fields[0]
				raw = strings.Join(fields[1:], " ")
			}
		}
		out[strings.TrimSpace(key)] = coerceTextValue(strings.TrimSpace(raw))
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

func normalizeNative(value any) any {
	switch v := value.(type) {
	case map[interface{}]interface{}:
		out := make(map[string]any, len(v))
		for key, val := range v {
			out[asString(key)] = normalizeNative(val)
		}
		return out
	case []interface{}:
		out := make([]any, len(v))
		for i, val := range v {
			out[i] = normalizeNative(val)
		}
		return out
	default:
		return value
	}
}
