package ferricstore

import (
	"errors"
	"fmt"
)

func scheduleFireResultWithCodec(value any, err error, codec Codec) (ScheduleFireResult, error) {
	if err != nil {
		return ScheduleFireResult{}, err
	}
	m, err := nativeMap(value)
	if err != nil {
		return ScheduleFireResult{}, err
	}
	fired, err := requiredManualFireCount(m, "fired")
	if err != nil {
		return ScheduleFireResult{}, err
	}
	skipped, err := optionalManualFireCount(m, "skipped")
	if err != nil {
		return ScheduleFireResult{}, err
	}
	if fired > 1 || skipped > 1 || fired+skipped != 1 {
		return ScheduleFireResult{}, fmt.Errorf(
			"FLOW.SCHEDULE.FIRE outcome fired=%d skipped=%d, expected exactly one outcome",
			fired, skipped,
		)
	}
	rawSchedule, present := m["schedule"]
	if !present || rawSchedule == nil {
		return ScheduleFireResult{}, errors.New("FLOW.SCHEDULE.FIRE response is missing schedule")
	}
	scheduleMap, err := nativeMap(rawSchedule)
	if err != nil {
		return ScheduleFireResult{}, fmt.Errorf("FLOW.SCHEDULE.FIRE schedule: %w", err)
	}
	schedule, err := scheduleResultFromMapWithCodec(scheduleMap, codec)
	if err != nil {
		return ScheduleFireResult{}, err
	}
	targetID, err := optionalScheduleText(m, "target_id")
	if err != nil {
		return ScheduleFireResult{}, err
	}
	reason, err := optionalScheduleText(m, "reason")
	if err != nil {
		return ScheduleFireResult{}, err
	}
	if fired == 1 && targetID == "" {
		return ScheduleFireResult{}, errors.New("FLOW.SCHEDULE.FIRE fired response is missing target_id")
	}
	if skipped == 1 && reason == "" {
		return ScheduleFireResult{}, errors.New("FLOW.SCHEDULE.FIRE skipped response is missing reason")
	}
	return ScheduleFireResult{
		Fired: fired, Skipped: skipped, TargetID: targetID, Reason: reason,
		Schedule: schedule, Raw: m,
	}, nil
}

func requiredManualFireCount(m map[string]any, field string) (int64, error) {
	value, present := m[field]
	if !present || value == nil {
		return 0, fmt.Errorf("FLOW.SCHEDULE.FIRE response is missing %s", field)
	}
	return parseManualFireCount(value, field)
}

func optionalManualFireCount(m map[string]any, field string) (int64, error) {
	value, present := m[field]
	if !present || value == nil {
		return 0, nil
	}
	return parseManualFireCount(value, field)
}

func parseManualFireCount(value any, field string) (int64, error) {
	count, err := responseInt64(value, nil)
	if err != nil {
		return 0, fmt.Errorf("FLOW.SCHEDULE.FIRE response has invalid %s: %w", field, err)
	}
	if count < 0 {
		return 0, fmt.Errorf("FLOW.SCHEDULE.FIRE response has negative %s", field)
	}
	return count, nil
}
