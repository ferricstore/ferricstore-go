package ferricstore

import (
	"fmt"
	"math"
)

func validateCMSIncrementResponse(value any, err error, expected int) (any, error) {
	items, err := exactArrayItems(value, err, expected, "CMS.INCRBY")
	if err != nil {
		return nil, err
	}
	for index, item := range items {
		count, err := responseInt64(item, nil)
		if err != nil {
			return nil, fmt.Errorf("CMS.INCRBY count %d: %w", index, err)
		}
		if count < 0 {
			return nil, fmt.Errorf("CMS.INCRBY returned negative count %d", count)
		}
	}
	return value, nil
}

// TopKEntry is one item returned by TOPK.LIST WITHCOUNT.
type TopKEntry struct {
	// Item is decoded with the Client's configured Codec.
	Item any
	// Count is the item's non-negative estimated frequency.
	Count int64
}

func decodeTopKList(codec Codec, value any, err error) ([]any, error) {
	if streamCodecIsRaw(codec) {
		return topKListItems(value, err)
	}
	items, err := topKListItems(value, err)
	if err != nil {
		return nil, err
	}
	return decodeArrayExactWithCodec(codec, items, nil, -1, "TOPK.LIST")
}

func topKListItems(value any, err error) ([]any, error) {
	items, err := exactArrayItems(value, err, -1, "TOPK.LIST")
	if err != nil {
		return nil, err
	}
	if err := validateTopKListItems(items); err != nil {
		return nil, err
	}
	return items, nil
}

func validateTopKListItems(items []any) error {
	for index, item := range items {
		switch item.(type) {
		case string, []byte:
		default:
			return fmt.Errorf("TOPK.LIST item %d is not binary", index)
		}
	}
	return nil
}

func decodeTopKListWithCount(codec Codec, value any, err error) ([]TopKEntry, error) {
	if streamCodecIsRaw(codec) {
		return decodeTopKEntries(nil, value, err)
	}
	return decodeTopKEntries(codec, value, err)
}

func decodeTopKEntries(codec Codec, value any, err error) ([]TopKEntry, error) {
	items, err := exactArrayItems(value, err, -1, "TOPK.LIST WITHCOUNT")
	if err != nil {
		return nil, err
	}
	if len(items)%2 != 0 {
		return nil, fmt.Errorf("TOPK.LIST returned odd item/count array length %d", len(items))
	}
	entries := make([]TopKEntry, len(items)/2)
	for index := range entries {
		item := items[index*2]
		if err := validateStringResponse(item, nil); err != nil {
			return nil, fmt.Errorf("TOPK.LIST item %d: %w", index, err)
		}
		if codec != nil {
			item, err = decodeValue(codec, item)
			if err != nil {
				return nil, fmt.Errorf("decode TOPK.LIST item %d: %w", index, err)
			}
		}
		count, err := responseInt64(items[index*2+1], nil)
		if err != nil {
			return nil, fmt.Errorf("TOPK.LIST count %d: %w", index, err)
		}
		if count < 0 {
			return nil, fmt.Errorf("TOPK.LIST returned negative count %d", count)
		}
		entries[index] = TopKEntry{Item: item, Count: count}
	}
	return entries, nil
}

func tdigestRankArray(value any, err error, expected int, command string) ([]int64, error) {
	ranks, err := intArrayExact(value, err, expected, command)
	if err != nil {
		return nil, err
	}
	for _, rank := range ranks {
		if rank < -2 {
			return nil, fmt.Errorf("%s returned invalid rank %d", command, rank)
		}
	}
	return ranks, nil
}

func tdigestFloatArray(value any, err error, expected int, command string, unitInterval bool) ([]float64, error) {
	values, err := nonFiniteFloatArrayExact(value, err, expected, command)
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		if math.IsInf(value, 0) {
			return nil, fmt.Errorf("%s returned infinite value", command)
		}
		if unitInterval && !math.IsNaN(value) && (value < 0 || value > 1) {
			return nil, fmt.Errorf("%s returned value %v outside 0..1", command, value)
		}
	}
	return values, nil
}

func tdigestFiniteOrNaNResponse(value any, err error, command string) (float64, error) {
	result, err := responseFloat64NonFinite(value, err)
	if err != nil {
		return 0, err
	}
	if math.IsInf(result, 0) {
		return 0, fmt.Errorf("%s returned infinite value", command)
	}
	return result, nil
}
