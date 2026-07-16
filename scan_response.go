package ferricstore

import "fmt"

func decodeKeyScan(value any, err error) (any, error) {
	outer, err := exactArrayItems(value, err, 2, "SCAN")
	if err != nil {
		return nil, err
	}
	if _, err := normalizeScanCursor(outer[0], false); err != nil {
		return nil, fmt.Errorf("SCAN returned invalid cursor: %w", err)
	}
	keys, ok := outer[1].([]any)
	if !ok {
		return nil, fmt.Errorf("SCAN keys returned %T, expected array", outer[1])
	}
	for index, key := range keys {
		if key == nil {
			return nil, fmt.Errorf("SCAN returned nil key at index %d", index)
		}
		if _, err := responseString(key, nil); err != nil {
			return nil, fmt.Errorf("SCAN key %d: %w", index, err)
		}
	}
	return value, nil
}
