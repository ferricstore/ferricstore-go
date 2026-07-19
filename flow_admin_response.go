package ferricstore

import (
	"errors"
	"fmt"
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
	count, err := responseInt64(value, nil)
	if err != nil {
		return 0, errors.New("FLOW.STATS response count is not numeric")
	}
	if count < 0 {
		return 0, fmt.Errorf("FLOW.STATS response count must be non-negative, got %d", count)
	}
	return count, nil
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
