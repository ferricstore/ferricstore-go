package ferricstore

import "fmt"

func listPositionResponse(value any, err error, rank, count *int64) (any, error) {
	if err != nil || value == nil {
		return value, err
	}
	if count == nil {
		position, err := responseInt64(value, nil)
		if err != nil {
			return nil, fmt.Errorf("LPOS: %w", err)
		}
		if position < 0 {
			return nil, fmt.Errorf("LPOS returned negative position %d", position)
		}
		return value, nil
	}

	positions, err := exactArrayItems(value, nil, -1, "LPOS")
	if err != nil {
		return nil, err
	}
	if *count > 0 && int64(len(positions)) > *count {
		return nil, fmt.Errorf("LPOS returned %d positions for COUNT %d", len(positions), *count)
	}
	var previous int64
	for index, item := range positions {
		position, err := responseInt64(item, nil)
		if err != nil {
			return nil, fmt.Errorf("LPOS position %d: %w", index, err)
		}
		if position < 0 {
			return nil, fmt.Errorf("LPOS returned negative position %d", position)
		}
		if index > 0 {
			reverse := rank != nil && *rank < 0
			if !reverse && position <= previous {
				return nil, fmt.Errorf("LPOS position %d is not strictly ascending", index)
			}
			if reverse && position >= previous {
				return nil, fmt.Errorf("LPOS position %d is not strictly descending", index)
			}
		}
		previous = position
	}
	return value, nil
}
