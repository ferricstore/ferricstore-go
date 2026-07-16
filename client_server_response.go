package ferricstore

import "fmt"

func waitAOFResponse(value any, err error) (any, error) {
	items, err := exactArrayItems(value, err, 2, "WAITAOF")
	if err != nil {
		return nil, err
	}
	for index, item := range items {
		acknowledgements, err := responseInt64(item, nil)
		if err != nil {
			return nil, fmt.Errorf("WAITAOF acknowledgement %d: %w", index, err)
		}
		if acknowledgements < 0 {
			return nil, fmt.Errorf("WAITAOF returned negative acknowledgement count %d", acknowledgements)
		}
	}
	return value, nil
}
