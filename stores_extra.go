package ferricstore

import (
	"sort"
	"strconv"
)

func boolArray(value any, err error) ([]bool, error) {
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		return nil, nil
	}
	out := make([]bool, 0, len(items))
	for _, item := range items {
		out = append(out, asBool(item))
	}
	return out, nil
}

func stringArray(value any, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		if value == nil {
			return nil, nil
		}
		return []string{asString(value)}, nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, asString(item))
	}
	return out, nil
}

func intArray(value any, err error) ([]int64, error) {
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		if value == nil {
			return nil, nil
		}
		return []int64{asInt64(value)}, nil
	}
	out := make([]int64, 0, len(items))
	for _, item := range items {
		out = append(out, asInt64(item))
	}
	return out, nil
}

func floatArray(value any, err error) ([]float64, error) {
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		if value == nil {
			return nil, nil
		}
		return []float64{asFloat64(value)}, nil
	}
	out := make([]float64, 0, len(items))
	for _, item := range items {
		out = append(out, asFloat64(item))
	}
	return out, nil
}

func floatArg(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
