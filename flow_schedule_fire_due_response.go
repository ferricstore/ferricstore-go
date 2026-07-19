package ferricstore

import (
	"errors"
	"fmt"
)

func scheduleFireDueResult(value any, err error) (ScheduleFireDueResult, error) {
	if err != nil {
		return ScheduleFireDueResult{}, err
	}
	m, err := nativeMap(value)
	if err != nil {
		return ScheduleFireDueResult{}, err
	}
	claimed, err := requiredScheduleCount(m, "claimed")
	if err != nil {
		return ScheduleFireDueResult{}, err
	}
	fired, err := requiredScheduleCount(m, "fired")
	if err != nil {
		return ScheduleFireDueResult{}, err
	}
	skipped, err := requiredScheduleCount(m, "skipped")
	if err != nil {
		return ScheduleFireDueResult{}, err
	}
	failures, err := scheduleFireDueErrors(m, claimed)
	if err != nil {
		return ScheduleFireDueResult{}, err
	}
	if fired > claimed || skipped > claimed-fired || int64(len(failures)) != claimed-fired-skipped {
		return ScheduleFireDueResult{}, fmt.Errorf(
			"FLOW.SCHEDULE.FIRE_DUE outcomes fired=%d skipped=%d errors=%d do not equal claimed=%d",
			fired, skipped, len(failures), claimed,
		)
	}
	lastTargetID, err := optionalScheduleText(m, "last_target_id")
	if err != nil {
		return ScheduleFireDueResult{}, err
	}
	lastSkipReason, err := optionalScheduleText(m, "last_skip_reason")
	if err != nil {
		return ScheduleFireDueResult{}, err
	}
	return ScheduleFireDueResult{
		Claimed: claimed, Fired: fired, Skipped: skipped, Errors: failures,
		LastTargetID: lastTargetID, LastSkipReason: lastSkipReason, Raw: m,
	}, nil
}

func requiredScheduleCount(m map[string]any, field string) (int64, error) {
	value, present := m[field]
	if !present || value == nil {
		return 0, fmt.Errorf("FLOW.SCHEDULE.FIRE_DUE response is missing %s", field)
	}
	count, err := responseInt64(value, nil)
	if err != nil {
		return 0, fmt.Errorf("FLOW.SCHEDULE.FIRE_DUE response has invalid %s: %w", field, err)
	}
	if count < 0 || count > nativeMaxContainerItems {
		return 0, fmt.Errorf(
			"FLOW.SCHEDULE.FIRE_DUE response %s %d is outside valid range 0..%d",
			field, count, nativeMaxContainerItems,
		)
	}
	return count, nil
}

func scheduleFireDueErrors(m map[string]any, claimed int64) ([]ScheduleFireDueError, error) {
	raw, present := m["errors"]
	if !present || raw == nil {
		return nil, errors.New("FLOW.SCHEDULE.FIRE_DUE response is missing errors")
	}
	items, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("FLOW.SCHEDULE.FIRE_DUE errors returned %T, expected array", raw)
	}
	if int64(len(items)) > claimed {
		return nil, fmt.Errorf(
			"FLOW.SCHEDULE.FIRE_DUE returned %d errors for %d claimed schedules",
			len(items), claimed,
		)
	}
	out := make([]ScheduleFireDueError, len(items))
	for index, item := range items {
		pair, ok := item.([]any)
		if !ok || len(pair) != 2 {
			return nil, fmt.Errorf("FLOW.SCHEDULE.FIRE_DUE error %d must be an id/reason pair", index)
		}
		id, err := responseString(pair[0], nil)
		if err != nil || id == "" {
			return nil, fmt.Errorf("FLOW.SCHEDULE.FIRE_DUE error %d has invalid id", index)
		}
		reason, err := responseString(pair[1], nil)
		if err != nil || reason == "" {
			return nil, fmt.Errorf("FLOW.SCHEDULE.FIRE_DUE error %d has invalid reason", index)
		}
		out[index] = ScheduleFireDueError{ID: id, Reason: reason}
	}
	return out, nil
}

func optionalScheduleText(m map[string]any, field string) (string, error) {
	value, present := m[field]
	if !present || value == nil {
		return "", nil
	}
	text, err := responseString(value, nil)
	if err != nil {
		return "", fmt.Errorf("FLOW.SCHEDULE.FIRE_DUE response has invalid %s: %w", field, err)
	}
	return text, nil
}
