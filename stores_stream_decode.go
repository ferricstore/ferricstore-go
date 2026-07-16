package ferricstore

import (
	"errors"
	"fmt"
)

var (
	errStreamEntryArray = errors.New("expected stream entry array")
	errStreamEntryShape = errors.New("stream entry has invalid field/value shape")
	errStreamReadArray  = errors.New("expected stream read array")
	errStreamReadShape  = errors.New("stream read item must contain a key and entries")
)

type streamResponseOrder uint8

const (
	streamResponseUnordered streamResponseOrder = iota
	streamResponseAscending
	streamResponseDescending
)

// Stream entries use the server's flat [id, field, value, ...] shape. Field
// names and IDs are protocol data; only values pass through the client codec.
func decodeStreamEntries(codec Codec, value any, err error) (any, error) {
	return decodeStreamEntriesLimited(codec, value, err, nil)
}

func decodeStreamEntriesLimited(codec Codec, value any, err error, count *int) (any, error) {
	return decodeStreamEntriesLimitedOrder(codec, value, err, count, streamResponseAscending)
}

func decodeStreamEntriesLimitedOrder(
	codec Codec,
	value any,
	err error,
	count *int,
	order streamResponseOrder,
) (any, error) {
	if err != nil || value == nil {
		return value, err
	}
	entries, ok := value.([]any)
	if !ok {
		return nil, errStreamEntryArray
	}
	if count != nil && len(entries) > *count {
		return nil, fmt.Errorf("stream returned %d entries for COUNT %d", len(entries), *count)
	}
	if err := validateStreamEntriesOrder(entries, order); err != nil {
		return nil, err
	}
	if streamCodecIsRaw(codec) {
		return value, nil
	}
	decoded := make([]any, len(entries))
	for entryIndex, item := range entries {
		entry := item.([]any)
		owned := append([]any(nil), entry...)
		for valueIndex := 2; valueIndex < len(owned); valueIndex += 2 {
			decodedValue, decodeErr := codec.Decode(owned[valueIndex])
			if decodeErr != nil {
				return nil, fmt.Errorf("decode stream entry %d value %d: %w", entryIndex, valueIndex/2, decodeErr)
			}
			owned[valueIndex] = decodedValue
		}
		decoded[entryIndex] = owned
	}
	return decoded, nil
}

func validateStreamEntries(entries []any) error {
	return validateStreamEntriesOrder(entries, streamResponseUnordered)
}

func validateStreamEntriesOrder(entries []any, order streamResponseOrder) error {
	var previousMS, previousSequence uint64
	for entryIndex, item := range entries {
		entry, ok := item.([]any)
		if !ok {
			return fmt.Errorf("stream entry %d: %w", entryIndex, errStreamEntryArray)
		}
		if len(entry) == 0 || len(entry)%2 == 0 {
			return fmt.Errorf("stream entry %d: %w", entryIndex, errStreamEntryShape)
		}
		milliseconds, sequence, ok := parseStreamIDResponse(entry[0])
		if !ok || milliseconds == 0 && sequence == 0 {
			return fmt.Errorf("stream entry %d has an invalid ID", entryIndex)
		}
		if entryIndex > 0 {
			comparison := compareStreamIDs(milliseconds, sequence, previousMS, previousSequence)
			if order == streamResponseAscending && comparison <= 0 {
				return fmt.Errorf("stream entry %d ID is not strictly ascending", entryIndex)
			}
			if order == streamResponseDescending && comparison >= 0 {
				return fmt.Errorf("stream entry %d ID is not strictly descending", entryIndex)
			}
		}
		previousMS, previousSequence = milliseconds, sequence
		for fieldIndex := 1; fieldIndex < len(entry); fieldIndex += 2 {
			if err := validateStringResponse(entry[fieldIndex], nil); err != nil {
				return fmt.Errorf("stream entry %d field %d: %w", entryIndex, fieldIndex/2, err)
			}
		}
	}
	return nil
}

func streamCodecIsRaw(codec Codec) bool {
	switch codec.(type) {
	case RawCodec, *RawCodec:
		return true
	default:
		return false
	}
}

func decodeStreamRead(codec Codec, value any, err error) (any, error) {
	return decodeStreamReadExpected(codec, value, err, nil, nil)
}

func decodeStreamReadExpected(codec Codec, value any, err error, expected []StreamRef, count *int) (any, error) {
	if err != nil || value == nil {
		return value, err
	}
	streams, ok := value.([]any)
	if !ok {
		return nil, errStreamReadArray
	}
	if expected != nil && len(streams) > len(expected) {
		return nil, fmt.Errorf("stream read returned %d streams, requested %d", len(streams), len(expected))
	}
	raw := streamCodecIsRaw(codec)
	var decoded []any
	if !raw {
		decoded = make([]any, len(streams))
	}
	expectedIndex := 0
	for streamIndex, item := range streams {
		stream, ok := item.([]any)
		if !ok || len(stream) != 2 {
			return nil, errStreamReadShape
		}
		if err := validateStringResponse(stream[0], nil); err != nil {
			return nil, fmt.Errorf("stream read item %d key: %w", streamIndex, err)
		}
		if expected != nil && !nextExpectedStreamKey(stream[0], expected, &expectedIndex) {
			return nil, fmt.Errorf("stream read item %d returned an unrequested key", streamIndex)
		}
		entries, decodeErr := decodeStreamEntriesLimited(codec, stream[1], nil, count)
		if decodeErr != nil {
			return nil, fmt.Errorf("decode stream read item %d: %w", streamIndex, decodeErr)
		}
		if raw {
			continue
		}
		decoded[streamIndex] = []any{stream[0], entries}
	}
	if raw {
		return value, nil
	}
	return decoded, nil
}

func nextExpectedStreamKey(value any, expected []StreamRef, next *int) bool {
	for *next < len(expected) {
		stream := expected[*next]
		*next++
		if responseTextEqual(value, stream.Key) {
			return true
		}
	}
	return false
}
