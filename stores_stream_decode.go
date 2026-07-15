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

// Stream entries use the server's flat [id, field, value, ...] shape. Field
// names and IDs are protocol data; only values pass through the client codec.
func decodeStreamEntries(codec Codec, value any, err error) (any, error) {
	if err != nil || value == nil {
		return value, err
	}
	entries, ok := value.([]any)
	if !ok {
		return nil, errStreamEntryArray
	}
	if streamCodecIsRaw(codec) {
		if err := validateStreamEntries(entries); err != nil {
			return nil, err
		}
		return value, nil
	}
	decoded := make([]any, len(entries))
	for entryIndex, item := range entries {
		entry, ok := item.([]any)
		if !ok {
			return nil, fmt.Errorf("stream entry %d: %w", entryIndex, errStreamEntryArray)
		}
		if len(entry) == 0 || len(entry)%2 == 0 {
			return nil, fmt.Errorf("stream entry %d: %w", entryIndex, errStreamEntryShape)
		}
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
	for _, item := range entries {
		entry, ok := item.([]any)
		if !ok {
			return errStreamEntryArray
		}
		if len(entry) == 0 || len(entry)%2 == 0 {
			return errStreamEntryShape
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
	if err != nil || value == nil {
		return value, err
	}
	streams, ok := value.([]any)
	if !ok {
		return nil, errStreamReadArray
	}
	raw := streamCodecIsRaw(codec)
	var decoded []any
	if !raw {
		decoded = make([]any, len(streams))
	}
	for streamIndex, item := range streams {
		stream, ok := item.([]any)
		if !ok || len(stream) != 2 {
			return nil, errStreamReadShape
		}
		entries, decodeErr := decodeStreamEntries(codec, stream[1], nil)
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
