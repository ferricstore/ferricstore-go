package ferricstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

const nativeMaxDecodeDepth = 64

type nativeDecodeBudget struct {
	maxDepth   int
	remaining  int
	copyBinary bool
}

func decodeNativeValue(data []byte) (any, []byte, error) {
	return decodeNativeValueWithLimits(data, nativeMaxDecodeDepth, nativeMaxContainerItems)
}

func decodeNativeValueWithLimits(data []byte, maxDepth, maxItems int) (any, []byte, error) {
	return decodeNativeValueWithOwnership(data, maxDepth, maxItems, true)
}

// decodeNativeOwnedValue decodes a response buffer whose ownership is being
// transferred to the returned value. Binary slices are capacity-limited views
// into that buffer, avoiding another payload-sized allocation while preventing
// append from overwriting adjacent encoded fields.
func decodeNativeOwnedValue(data []byte) (any, []byte, error) {
	return decodeNativeValueWithOwnership(data, nativeMaxDecodeDepth, nativeMaxContainerItems, false)
}

func decodeNativeValueWithOwnership(data []byte, maxDepth, maxItems int, copyBinary bool) (any, []byte, error) {
	if maxDepth < 0 || maxItems <= 0 {
		return nil, nil, errors.New("ferricstore native decoder limits are invalid")
	}
	budget := &nativeDecodeBudget{maxDepth: maxDepth, remaining: maxItems, copyBinary: copyBinary}
	return decodeNativeValueBudget(data, budget, 0)
}

func decodeNativeValueBudget(data []byte, budget *nativeDecodeBudget, depth int) (any, []byte, error) {
	if len(data) == 0 {
		return nil, nil, errors.New("ferricstore native value is empty")
	}
	if depth > budget.maxDepth {
		return nil, nil, fmt.Errorf("ferricstore native value exceeds maximum nesting depth %d", budget.maxDepth)
	}
	budget.remaining--
	if budget.remaining < 0 {
		return nil, nil, errors.New("ferricstore native value exceeds aggregate item limit")
	}
	tag := data[0]
	rest := data[1:]
	switch tag {
	case 0:
		return nil, rest, nil
	case 1:
		return true, rest, nil
	case 2:
		return false, rest, nil
	case 3:
		if len(rest) < 8 {
			return nil, nil, errors.New("ferricstore native integer is truncated")
		}
		return int64(binary.BigEndian.Uint64(rest[:8])), rest[8:], nil
	case 4:
		if len(rest) < 4 {
			return nil, nil, errors.New("ferricstore native binary length is truncated")
		}
		size := int(binary.BigEndian.Uint32(rest[:4]))
		rest = rest[4:]
		if len(rest) < size {
			return nil, nil, errors.New("ferricstore native binary is truncated")
		}
		value := rest[:size:size]
		if budget.copyBinary {
			value = append([]byte(nil), value...)
		}
		return value, rest[size:], nil
	case 5:
		if len(rest) < 4 {
			return nil, nil, errors.New("ferricstore native array length is truncated")
		}
		count, err := nativeBoundedItemCount("array", binary.BigEndian.Uint32(rest[:4]), len(rest)-4, 1)
		if err != nil {
			return nil, nil, err
		}
		rest = rest[4:]
		if count > budget.remaining {
			return nil, nil, errors.New("ferricstore native array exceeds aggregate item limit")
		}
		items := make([]any, 0, count)
		for i := 0; i < count; i++ {
			value, next, err := decodeNativeValueBudget(rest, budget, depth+1)
			if err != nil {
				return nil, nil, err
			}
			items = append(items, value)
			rest = next
		}
		return items, rest, nil
	case 6:
		if len(rest) < 4 {
			return nil, nil, errors.New("ferricstore native map length is truncated")
		}
		count, err := nativeBoundedItemCount("map", binary.BigEndian.Uint32(rest[:4]), len(rest)-4, 5)
		if err != nil {
			return nil, nil, err
		}
		rest = rest[4:]
		if count > budget.remaining {
			return nil, nil, errors.New("ferricstore native map exceeds aggregate item limit")
		}
		mapping := make(map[string]any, count)
		for i := 0; i < count; i++ {
			if len(rest) < 4 {
				return nil, nil, errors.New("ferricstore native map key length is truncated")
			}
			keySize := int(binary.BigEndian.Uint32(rest[:4]))
			rest = rest[4:]
			if len(rest) < keySize {
				return nil, nil, errors.New("ferricstore native map key is truncated")
			}
			key := string(rest[:keySize])
			rest = rest[keySize:]
			if _, exists := mapping[key]; exists {
				return nil, nil, fmt.Errorf("ferricstore native map contains duplicate key %q", key)
			}
			value, next, err := decodeNativeValueBudget(rest, budget, depth+1)
			if err != nil {
				return nil, nil, err
			}
			mapping[key] = value
			rest = next
		}
		return mapping, rest, nil
	case 7:
		if len(rest) < 8 {
			return nil, nil, errors.New("ferricstore native float is truncated")
		}
		return math.Float64frombits(binary.BigEndian.Uint64(rest[:8])), rest[8:], nil
	case 8:
		if len(rest) < 8 {
			return nil, nil, errors.New("ferricstore native unsigned integer is truncated")
		}
		return binary.BigEndian.Uint64(rest[:8]), rest[8:], nil
	default:
		return nil, nil, fmt.Errorf("ferricstore native value has unknown tag %d", tag)
	}
}

func nativeErrorMessage(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	case map[string]any:
		if message := asString(v["message"]); message != "" {
			return message
		}
		if code := asString(v["code"]); code != "" {
			return code
		}
	}
	return fmt.Sprint(value)
}
