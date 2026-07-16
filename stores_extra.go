package ferricstore

import (
	"fmt"
	"math"
	"sort"
	"strconv"
)

func validateFiniteFloat(command string, value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return fmt.Errorf("%s value must be finite", command)
	}
	return nil
}

func nonNegativeInt64Response(command string, value any, err error) (int64, error) {
	result, err := responseInt64(value, err)
	if err != nil {
		return 0, err
	}
	if result < 0 {
		return 0, fmt.Errorf("%s returned negative value %d", command, result)
	}
	return result, nil
}

func boolArray(value any, err error) ([]bool, error) {
	return boolArrayExact(value, err, -1, "boolean array response")
}

func boolArrayExact(value any, err error, expected int, command string) ([]bool, error) {
	items, err := exactArrayItems(value, err, expected, command)
	if err != nil {
		return nil, err
	}
	out := make([]bool, 0, len(items))
	for _, item := range items {
		parsed, err := responseBool(item, nil)
		if err != nil {
			return nil, err
		}
		out = append(out, parsed)
	}
	return out, nil
}

func stringArray(value any, err error) ([]string, error) {
	return stringArrayExact(value, err, -1, "string array response")
}

func stringArrayExact(value any, err error, expected int, command string) ([]string, error) {
	items, err := exactArrayItems(value, err, expected, command)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		parsed, err := responseString(item, nil)
		if err != nil {
			return nil, err
		}
		out = append(out, parsed)
	}
	return out, nil
}

func intArray(value any, err error) ([]int64, error) {
	return intArrayExact(value, err, -1, "integer array response")
}

func intArrayExact(value any, err error, expected int, command string) ([]int64, error) {
	items, err := exactArrayItems(value, err, expected, command)
	if err != nil {
		return nil, err
	}
	out := make([]int64, 0, len(items))
	for _, item := range items {
		parsed, err := responseInt64(item, nil)
		if err != nil {
			return nil, err
		}
		out = append(out, parsed)
	}
	return out, nil
}

func nullableFloatArray(value any, err error) ([]float64, error) {
	return floatArrayPolicy(value, err, true, false)
}

func nullableFloatArrayExact(value any, err error, expected int, command string) ([]float64, error) {
	items, err := exactArrayItems(value, err, expected, command)
	if err != nil {
		return nil, err
	}
	return floatArrayPolicy(items, nil, true, false)
}

func nonFiniteFloatArrayExact(value any, err error, expected int, command string) ([]float64, error) {
	items, err := exactArrayItems(value, err, expected, command)
	if err != nil {
		return nil, err
	}
	return floatArrayPolicy(items, nil, false, true)
}

func nonFiniteFloatArray(value any, err error) ([]float64, error) {
	return floatArrayPolicy(value, err, false, true)
}

func floatArrayPolicy(value any, err error, nullable, allowNonFinite bool) ([]float64, error) {
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		if value == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("expected float array response, got %T", value)
	}
	out := make([]float64, 0, len(items))
	for _, item := range items {
		if item == nil && nullable {
			out = append(out, math.NaN())
			continue
		}
		var parsed float64
		var err error
		if allowNonFinite {
			parsed, err = responseFloat64NonFinite(item, nil)
		} else {
			parsed, err = responseFloat64(item, nil)
		}
		if err != nil {
			return nil, err
		}
		out = append(out, parsed)
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

func mapKeysForCodec[V any](values map[string]V, codec Codec) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	if codecNeedsDeterministicMapOrder(codec) {
		sort.Strings(keys)
	}
	return keys
}

// deterministicMapKeysForCodec returns nil for codecs whose result cannot
// depend on invocation order, preserving allocation-free map iteration on the
// built-in fast path. Custom codecs receive a stable sequence.
func deterministicMapKeysForCodec[V any](values map[string]V, codec Codec) []string {
	if !codecNeedsDeterministicMapOrder(codec) {
		return nil
	}
	return sortedKeys(values)
}
