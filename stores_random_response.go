package ferricstore

import "fmt"

const maxRandomReplacementCount = 10_000

type sortedSetScoreOrder uint8

const (
	sortedSetScoresUnordered sortedSetScoreOrder = iota
	sortedSetScoresAscending
	sortedSetScoresDescending
)

func countMagnitude(count int) uint64 {
	if count < 0 {
		return uint64(-(count + 1)) + 1
	}
	return uint64(count)
}

func validateRandomReplacementCount(command string, count *int) error {
	if count != nil && *count < -maxRandomReplacementCount {
		return fmt.Errorf("%s replacement count must not exceed %d", command, maxRandomReplacementCount)
	}
	return nil
}

func decodeArrayWithLimit(codec Codec, value any, err error, maximum uint64, command string) ([]any, error) {
	items, err := exactArrayItems(value, err, -1, command)
	if err != nil {
		return nil, err
	}
	if uint64(len(items)) > maximum {
		return nil, fmt.Errorf("%s returned %d values, requested at most %d", command, len(items), maximum)
	}
	if streamCodecIsRaw(codec) {
		return items, nil
	}
	return decodeArrayExactWithCodec(codec, items, nil, -1, command)
}

func decodeHashRandomField(codec Codec, value any, err error, count *int, withValues bool) (any, error) {
	if err != nil || value == nil {
		return value, err
	}
	if count == nil {
		if _, err := responseString(value, nil); err != nil {
			return nil, fmt.Errorf("HRANDFIELD: %w", err)
		}
		return value, nil
	}

	items, err := exactArrayItems(value, nil, -1, "HRANDFIELD")
	if err != nil {
		return nil, err
	}
	pairs := len(items)
	if withValues {
		if len(items)%2 != 0 {
			return nil, fmt.Errorf("HRANDFIELD returned odd field/value array length %d", len(items))
		}
		pairs /= 2
	}
	if uint64(pairs) > countMagnitude(*count) {
		return nil, fmt.Errorf("HRANDFIELD returned %d fields, requested at most %d", pairs, countMagnitude(*count))
	}
	step := 1
	if withValues {
		step = 2
	}
	for index := 0; index < len(items); index += step {
		if items[index] == nil {
			return nil, fmt.Errorf("HRANDFIELD returned nil field at index %d", index/step)
		}
		if _, err := responseString(items[index], nil); err != nil {
			return nil, fmt.Errorf("HRANDFIELD field %d: %w", index/step, err)
		}
	}
	if !withValues || streamCodecIsRaw(codec) {
		return value, nil
	}
	return decodeAlternatingCollectionValues(codec, items, nil, 1, "HRANDFIELD")
}

func decodeSortedSetPairs(
	codec Codec,
	value any,
	err error,
	maximum uint64,
	limited bool,
	order sortedSetScoreOrder,
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
		return nil, fmt.Errorf("%s returned odd member/score array length %d", command, len(items))
	}
	pairs := len(items) / 2
	if limited && uint64(pairs) > maximum {
		return nil, fmt.Errorf("%s returned %d pairs, requested at most %d", command, pairs, maximum)
	}
	if err := validateSortedSetPairScores(items, order, command); err != nil {
		return nil, err
	}
	if streamCodecIsRaw(codec) {
		return value, nil
	}
	return decodeAlternatingCollectionValues(codec, items, nil, 0, command)
}

func validateSortedSetPairScores(items []any, order sortedSetScoreOrder, command string) error {
	var previous float64
	for index := 1; index < len(items); index += 2 {
		score, err := responseFloat64(items[index], nil)
		if err != nil {
			return fmt.Errorf("%s score %d: %w", command, index/2, err)
		}
		if index > 1 && order == sortedSetScoresAscending && score < previous {
			return fmt.Errorf("%s score %d is not ascending", command, index/2)
		}
		if index > 1 && order == sortedSetScoresDescending && score > previous {
			return fmt.Errorf("%s score %d is not descending", command, index/2)
		}
		previous = score
	}
	return nil
}
