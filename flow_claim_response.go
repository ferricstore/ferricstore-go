package ferricstore

import "fmt"

const defaultFlowResponseLimitV080 = 100

// These 0.8 list APIs clamp a requested count/limit to 1,000. Keep this
// distinct from configurable claim limits and mutation-batch limits.
const maxClampedFlowListItemsV080 = 1_000

func validateFlowResponseLimit(command string, value any, limit int) error {
	if limit == 0 {
		limit = 1
	}
	var count int
	switch items := value.(type) {
	case []any:
		count = len(items)
	case []ClaimedItem:
		count = len(items)
	default:
		return nil
	}
	if count > limit {
		return fmt.Errorf("%s returned %d items, limit is %d", command, count, limit)
	}
	return nil
}

func effectiveFlowResponseLimit(limit *int, defaultLimit, maximum int) int {
	effective := defaultLimit
	if limit != nil {
		effective = *limit
	}
	if maximum > 0 && effective > maximum {
		effective = maximum
	}
	return effective
}

func validateDefaultedFlowResponseLimit(
	command string,
	value any,
	limit *int,
	defaultLimit, maximum int,
) error {
	return validateFlowResponseLimit(
		command,
		value,
		effectiveFlowResponseLimit(limit, defaultLimit, maximum),
	)
}

func mapListWithLimit(
	command string,
	limit *int,
	defaultLimit, maximum int,
	value any,
	err error,
) ([]map[string]any, error) {
	if err != nil {
		return nil, err
	}
	if err := validateDefaultedFlowResponseLimit(command, value, limit, defaultLimit, maximum); err != nil {
		return nil, err
	}
	return mapList(value, nil)
}
