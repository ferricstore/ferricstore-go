package ferricstore

import "fmt"

func decodeArray(codec Codec, value any, err error) ([]any, error) {
	return decodeArrayExact(codec, value, err, -1, "array response")
}

func decodeArrayExact(codec Codec, value any, err error, expected int, command string) ([]any, error) {
	if streamCodecIsRaw(codec) {
		return exactArrayItems(value, err, expected, command)
	}
	return decodeArrayExactWithCodec(codec, value, err, expected, command)
}

func decodeArrayExactWithCodec(codec Codec, value any, err error, expected int, command string) ([]any, error) {
	items, err := exactArrayItems(value, err, expected, command)
	if err != nil {
		return nil, err
	}
	out := make([]any, 0, len(items))
	for _, item := range items {
		decoded, err := decodeValue(codec, item)
		if err != nil {
			return nil, err
		}
		out = append(out, decoded)
	}
	return out, nil
}

func exactArrayItems(value any, err error, expected int, command string) ([]any, error) {
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%s returned %T, expected array", command, value)
	}
	if expected >= 0 && len(items) != expected {
		return nil, fmt.Errorf("%s returned %d values, expected %d", command, len(items), expected)
	}
	return items, nil
}
