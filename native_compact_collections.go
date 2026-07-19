package ferricstore

import (
	"encoding/binary"
	"errors"
	"fmt"
)

func decodeNativeCompactIntegerList(data []byte) ([]any, error) {
	if len(data) < 5 || data[0] != nativeCompactIntegerList {
		return nil, errors.New("ferricstore native compact integer list is invalid")
	}
	count, err := nativeBoundedItemCount(
		"compact integer list", binary.BigEndian.Uint32(data[1:5]), len(data)-5, 8,
	)
	if err != nil {
		return nil, err
	}
	if uint64(count)*8 != uint64(len(data)-5) {
		return nil, errors.New("ferricstore native compact integer list length mismatch")
	}
	values := make([]any, count)
	offset := 5
	for index := range values {
		values[index] = int64(binary.BigEndian.Uint64(data[offset : offset+8]))
		offset += 8
	}
	return values, nil
}

func decodeNativeCompactBinaryListList(data []byte) ([]any, error) {
	if len(data) < 5 || data[0] != nativeCompactBinaryListList {
		return nil, errors.New("ferricstore native compact binary list list is invalid")
	}
	count, err := nativeBoundedItemCount(
		"compact binary list list", binary.BigEndian.Uint32(data[1:5]), len(data)-5, 4,
	)
	if err != nil {
		return nil, err
	}
	budget := nativeMaxContainerItems
	if err := consumeNativeCompactItems("compact binary list list", &budget, count); err != nil {
		return nil, err
	}
	values := make([]any, 0, count)
	offset := 5
	for index := 0; index < count; index++ {
		value, next, err := takeNativeCompactBinaryList(data, offset, &budget)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
		offset = next
	}
	if offset != len(data) {
		return nil, errors.New("ferricstore native compact binary list list has trailing bytes")
	}
	return values, nil
}

func decodeNativeCompactBinaryMapList(data []byte) ([]any, error) {
	if len(data) < 5 || data[0] != nativeCompactBinaryMapList {
		return nil, errors.New("ferricstore native compact binary map list is invalid")
	}
	count, err := nativeBoundedItemCount(
		"compact binary map list", binary.BigEndian.Uint32(data[1:5]), len(data)-5, 4,
	)
	if err != nil {
		return nil, err
	}
	budget := nativeMaxContainerItems
	if err := consumeNativeCompactItems("compact binary map list", &budget, count); err != nil {
		return nil, err
	}
	values := make([]any, 0, count)
	offset := 5
	for index := 0; index < count; index++ {
		value, next, err := takeNativeCompactBinaryMap(data, offset, &budget)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
		offset = next
	}
	if offset != len(data) {
		return nil, errors.New("ferricstore native compact binary map list has trailing bytes")
	}
	return values, nil
}

func takeNativeCompactBinaryList(data []byte, offset int, budget *int) ([]any, int, error) {
	if len(data)-offset < 4 {
		return nil, offset, errors.New("ferricstore native compact binary list count is truncated")
	}
	count, err := nativeBoundedItemCount(
		"compact binary list", binary.BigEndian.Uint32(data[offset:offset+4]), len(data)-offset-4, 4,
	)
	if err != nil {
		return nil, offset, err
	}
	if err := consumeNativeCompactItems("compact binary list", budget, count); err != nil {
		return nil, offset, err
	}
	offset += 4
	values := make([]any, 0, count)
	for index := 0; index < count; index++ {
		value, next, err := readNativeCompactBinary(data, offset)
		if err != nil {
			return nil, offset, err
		}
		values = append(values, value)
		offset = next
	}
	return values, offset, nil
}

func takeNativeCompactBinaryMap(data []byte, offset int, budget *int) (map[string]any, int, error) {
	if len(data)-offset < 4 {
		return nil, offset, errors.New("ferricstore native compact binary map count is truncated")
	}
	count, err := nativeBoundedItemCount(
		"compact binary map", binary.BigEndian.Uint32(data[offset:offset+4]), len(data)-offset-4, 8,
	)
	if err != nil {
		return nil, offset, err
	}
	if err := consumeNativeCompactItems("compact binary map", budget, count); err != nil {
		return nil, offset, err
	}
	offset += 4
	values := make(map[string]any, count)
	for index := 0; index < count; index++ {
		key, next, err := readNativeCompactBinary(data, offset)
		if err != nil {
			return nil, offset, err
		}
		value, next, err := readNativeCompactBinary(data, next)
		if err != nil {
			return nil, offset, err
		}
		textKey := string(key)
		if _, duplicate := values[textKey]; duplicate {
			return nil, offset, fmt.Errorf("ferricstore native compact binary map contains duplicate key %q", textKey)
		}
		values[textKey] = value
		offset = next
	}
	return values, offset, nil
}

func consumeNativeCompactItems(kind string, budget *int, count int) error {
	if count > *budget {
		return fmt.Errorf("ferricstore native %s exceeds aggregate item limit", kind)
	}
	*budget -= count
	return nil
}
