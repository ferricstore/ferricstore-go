package ferricstore

import (
	"errors"
	"fmt"
)

type hashFieldResultKind uint8

const (
	hashFieldExpiryResult hashFieldResultKind = iota
	hashFieldTTLResult
	hashFieldPersistResult
)

func validateHashFieldArgs(command string, fields []string) error {
	if len(fields) == 0 {
		return errors.New(command + " requires at least one field")
	}
	return nil
}

func hashFieldIntegerResponse(
	command string,
	value any,
	err error,
	expected int,
	kind hashFieldResultKind,
) (any, error) {
	items, err := exactArrayItems(value, err, expected, command)
	if err != nil {
		return nil, err
	}
	for index, item := range items {
		result, err := responseInt64(item, nil)
		if err != nil {
			return nil, fmt.Errorf("%s result %d: %w", command, index, err)
		}
		if !validHashFieldResult(result, kind) {
			return nil, fmt.Errorf("%s result %d has invalid value %d", command, index, result)
		}
	}
	return value, nil
}

func validHashFieldResult(value int64, kind hashFieldResultKind) bool {
	switch kind {
	case hashFieldExpiryResult:
		return value == 1 || value == -2
	case hashFieldTTLResult:
		return value >= 0 || value == -1 || value == -2
	case hashFieldPersistResult:
		return value == 1 || value == -1 || value == -2
	default:
		return false
	}
}

func decodeBlockingListPop(codec Codec, value any, err error, command string, keys []string) (any, error) {
	if err != nil || value == nil {
		return value, err
	}
	items, err := exactArrayItems(value, nil, 2, command)
	if err != nil {
		return nil, err
	}
	if err := validateBlockingListKey(items[0], keys, command); err != nil {
		return nil, err
	}
	if streamCodecIsRaw(codec) {
		return value, nil
	}
	decoded, err := decodeValue(codec, items[1])
	if err != nil {
		return nil, fmt.Errorf("decode %s value: %w", command, err)
	}
	return []any{items[0], decoded}, nil
}

func decodeBlockingListMPop(codec Codec, value any, err error, keys []string, count *int) (any, error) {
	if err != nil || value == nil {
		return value, err
	}
	items, err := exactArrayItems(value, nil, 2, "BLMPOP")
	if err != nil {
		return nil, err
	}
	if err := validateBlockingListKey(items[0], keys, "BLMPOP"); err != nil {
		return nil, err
	}
	values, ok := items[1].([]any)
	if !ok {
		return nil, fmt.Errorf("BLMPOP values returned %T, expected array", items[1])
	}
	if len(values) == 0 {
		return nil, errors.New("BLMPOP returned an empty value array")
	}
	maximum := 1
	if count != nil {
		maximum = *count
	}
	if len(values) > maximum {
		return nil, fmt.Errorf("BLMPOP returned %d values for COUNT %d", len(values), maximum)
	}
	if streamCodecIsRaw(codec) {
		return value, nil
	}
	decoded, err := decodeArray(codec, values, nil)
	if err != nil {
		return nil, fmt.Errorf("decode BLMPOP values: %w", err)
	}
	return []any{items[0], decoded}, nil
}

func validateBlockingListKey(value any, keys []string, command string) error {
	if err := validateStringResponse(value, nil); err != nil {
		return fmt.Errorf("%s key: %w", command, err)
	}
	for _, key := range keys {
		if responseTextEqual(value, key) {
			return nil
		}
	}
	return fmt.Errorf("%s returned an unrequested key", command)
}

func decodeAlternatingCollectionValues(
	codec Codec,
	value any,
	err error,
	firstValue int,
	command string,
) (any, error) {
	if err != nil || value == nil {
		return value, err
	}
	items, err := exactArrayItems(value, nil, -1, command)
	if err != nil {
		return nil, err
	}
	if len(items)%2 != 0 {
		return nil, fmt.Errorf("%s returned odd member/value array length %d", command, len(items))
	}
	if streamCodecIsRaw(codec) {
		return value, nil
	}
	decoded := append([]any(nil), items...)
	for index := firstValue; index < len(decoded); index += 2 {
		decodedValue, err := decodeValue(codec, decoded[index])
		if err != nil {
			return nil, fmt.Errorf("decode %s value %d: %w", command, index/2, err)
		}
		decoded[index] = decodedValue
	}
	return decoded, nil
}

type collectionScanKind uint8

const (
	setCollectionScan collectionScanKind = iota
	hashCollectionScan
	sortedSetCollectionScan
)

func decodeCollectionScan(codec Codec, value any, err error, kind collectionScanKind, command string) (any, error) {
	if err != nil {
		return nil, err
	}
	outer, err := exactArrayItems(value, nil, 2, command)
	if err != nil {
		return nil, err
	}
	if _, err := normalizeScanCursor(outer[0], true); err != nil {
		return nil, fmt.Errorf("%s returned invalid cursor: %w", command, err)
	}
	items, ok := outer[1].([]any)
	if !ok {
		return nil, fmt.Errorf("%s values returned %T, expected array", command, outer[1])
	}
	if kind != setCollectionScan && len(items)%2 != 0 {
		return nil, fmt.Errorf("%s returned odd field/value array length %d", command, len(items))
	}
	if err := validateCollectionScanMetadata(items, kind, command); err != nil {
		return nil, err
	}
	if streamCodecIsRaw(codec) {
		return value, nil
	}
	if kind == setCollectionScan {
		decoded, err := decodeArray(codec, items, nil)
		if err != nil {
			return nil, err
		}
		return []any{outer[0], decoded}, nil
	}
	firstValue := 1
	if kind == sortedSetCollectionScan {
		firstValue = 0
	}
	decoded, err := decodeAlternatingCollectionValues(codec, items, nil, firstValue, command)
	if err != nil {
		return nil, err
	}
	return []any{outer[0], decoded}, nil
}

func validateCollectionScanMetadata(items []any, kind collectionScanKind, command string) error {
	switch kind {
	case setCollectionScan:
		return nil
	case hashCollectionScan:
		for index := 0; index < len(items); index += 2 {
			if items[index] == nil {
				return fmt.Errorf("%s returned nil field at pair %d", command, index/2)
			}
			if _, err := responseString(items[index], nil); err != nil {
				return fmt.Errorf("%s field %d: %w", command, index/2, err)
			}
		}
		return nil
	case sortedSetCollectionScan:
		return validateSortedSetPairScores(items, sortedSetScoresUnordered, command)
	default:
		return fmt.Errorf("%s has unsupported scan response kind %d", command, kind)
	}
}
