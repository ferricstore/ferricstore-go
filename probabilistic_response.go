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

func decodeTopKListWithCount(codec Codec, value any, err error) (any, error) {
	if err != nil {
		return nil, err
	}
	items, err := exactArrayItems(value, nil, -1, "TOPK.LIST")
	if err != nil {
		return nil, err
	}
	if len(items)%2 != 0 {
		return nil, fmt.Errorf("TOPK.LIST returned odd item/count array length %d", len(items))
	}
	for index := 1; index < len(items); index += 2 {
		count, err := responseInt64(items[index], nil)
		if err != nil {
			return nil, fmt.Errorf("TOPK.LIST count %d: %w", index/2, err)
		}
		if count < 0 {
			return nil, fmt.Errorf("TOPK.LIST returned negative count %d", count)
		}
	}
	return decodeAlternatingCollectionValues(codec, items, nil, 0, "TOPK.LIST")
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
