package ferricstore

import (
	"errors"
	"fmt"
)

func validateStreamInfo(value any, err error) (any, error) {
	if err != nil {
		return nil, err
	}
	info, err := nativeMap(value)
	if err != nil {
		return nil, fmt.Errorf("XINFO STREAM: %w", err)
	}
	length, err := nonNegativeInt64Response("XINFO STREAM length", info["length"], nil)
	if err != nil {
		return nil, err
	}
	if _, err := nonNegativeInt64Response("XINFO STREAM groups", info["groups"], nil); err != nil {
		return nil, err
	}
	generatedMS, generatedSequence, ok := parseStreamIDResponse(info["last-generated-id"])
	if !ok {
		return nil, errors.New("XINFO STREAM returned an invalid last-generated-id")
	}
	first, last := info["first-entry"], info["last-entry"]
	if length == 0 {
		if first != nil || last != nil {
			return nil, errors.New("XINFO STREAM returned entries for an empty stream")
		}
		return value, nil
	}
	if first == nil || last == nil {
		return nil, errors.New("XINFO STREAM omitted entries for a non-empty stream")
	}
	if err := validateStreamEntries([]any{first, last}); err != nil {
		return nil, fmt.Errorf("XINFO STREAM entries: %w", err)
	}
	firstEntry := first.([]any)
	lastEntry := last.([]any)
	firstMS, firstSequence, _ := parseStreamIDResponse(firstEntry[0])
	lastMS, lastSequence, _ := parseStreamIDResponse(lastEntry[0])
	entryComparison := compareStreamIDs(firstMS, firstSequence, lastMS, lastSequence)
	if entryComparison > 0 || length == 1 && entryComparison != 0 || length > 1 && entryComparison >= 0 {
		return nil, errors.New("XINFO STREAM first and last entries are inconsistent with length")
	}
	if compareStreamIDs(generatedMS, generatedSequence, lastMS, lastSequence) < 0 {
		return nil, errors.New("XINFO STREAM last-generated-id is behind last-entry")
	}
	return value, nil
}
