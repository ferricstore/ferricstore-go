package ferricstore

import (
	"errors"
	"fmt"
	"strconv"
)

func mapResult(value any, err error) (map[string]any, error) {
	if err != nil {
		return nil, err
	}
	return nativeMap(value)
}

func statsCount(stats map[string]any) (int64, error) {
	value, ok := stats["count"]
	if !ok {
		return 0, errors.New("FLOW.STATS response missing count")
	}
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case uint64:
		if v > uint64(^uint64(0)>>1) {
			return 0, errors.New("FLOW.STATS response count overflows int64")
		}
		return int64(v), nil
	case uint:
		if uint64(v) > uint64(^uint64(0)>>1) {
			return 0, errors.New("FLOW.STATS response count overflows int64")
		}
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case string:
		count, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, errors.New("FLOW.STATS response count is not numeric")
		}
		return count, nil
	case []byte:
		count, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return 0, errors.New("FLOW.STATS response count is not numeric")
		}
		return count, nil
	default:
		return 0, errors.New("FLOW.STATS response count is not numeric")
	}
}

func mapList(value any, err error) ([]map[string]any, error) {
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, errors.New("expected array response")
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		m, err := nativeMap(item)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

func adminInt64(m map[string]any, field string) (int64, error) {
	value, exists := m[field]
	if !exists || value == nil {
		return 0, nil
	}
	parsed, err := responseInt64(value, nil)
	if err != nil {
		return 0, fmt.Errorf("decode admin field %s: %w", field, err)
	}
	return parsed, nil
}

func adminFloat64(m map[string]any, field string) (float64, error) {
	value, exists := m[field]
	if !exists || value == nil {
		return 0, nil
	}
	parsed, err := responseFloat64(value, nil)
	if err != nil {
		return 0, fmt.Errorf("decode admin field %s: %w", field, err)
	}
	return parsed, nil
}

func adminString(m map[string]any, field string) (string, error) {
	value, exists := m[field]
	if !exists || value == nil {
		return "", nil
	}
	parsed, err := responseString(value, nil)
	if err != nil {
		return "", fmt.Errorf("decode admin field %s: %w", field, err)
	}
	return parsed, nil
}

func adminStringOrInt(m map[string]any, field string) (string, error) {
	value, exists := m[field]
	if !exists || value == nil {
		return "", nil
	}
	if parsed, err := responseString(value, nil); err == nil {
		return parsed, nil
	}
	parsed, err := responseInt64(value, nil)
	if err != nil {
		return "", fmt.Errorf("decode admin field %s: expected string or integer response", field)
	}
	return strconv.FormatInt(parsed, 10), nil
}

func adminStringList(value any, field string) ([]string, error) {
	if value == nil {
		return []string{}, nil
	}
	if values, ok := value.([]string); ok {
		return append([]string(nil), values...), nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("decode %s: expected array, got %T", field, value)
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		text, err := responseString(item, nil)
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", field, err)
		}
		out = append(out, text)
	}
	return out, nil
}
