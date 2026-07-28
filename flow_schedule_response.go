package ferricstore

import (
	"errors"
	"fmt"
	"strings"
)

func scheduleRecord(value any, err error) (ScheduleRecord, error) {
	return scheduleRecordWithCodec(value, err, RawCodec{})
}

func scheduleRecordWithCodec(value any, err error, codec Codec) (ScheduleRecord, error) {
	if err != nil {
		return ScheduleRecord{}, err
	}
	m, err := nativeMap(value)
	if err != nil {
		return ScheduleRecord{}, err
	}
	return scheduleRecordFromMapWithCodec(m, codec)
}

func scheduleRecordFromMapWithCodec(m map[string]any, codec Codec) (ScheduleRecord, error) {
	id, err := requiredScheduleText(m, "id")
	if err != nil {
		return ScheduleRecord{}, err
	}
	flowID, err := optionalScheduleResponseText(m, "flow_id")
	if err != nil {
		return ScheduleRecord{}, err
	}
	kind, err := requiredScheduleText(m, "kind")
	if err != nil {
		return ScheduleRecord{}, err
	}
	if !validScheduleResponseKind(kind) {
		return ScheduleRecord{}, fmt.Errorf("schedule response has invalid kind %q", kind)
	}
	status, err := requiredScheduleText(m, "state")
	if err != nil {
		return ScheduleRecord{}, err
	}
	if !validScheduleResponseState(status) {
		return ScheduleRecord{}, fmt.Errorf("schedule response has invalid state %q", status)
	}

	texts := make([]string, 7)
	for index, field := range []string{
		"timezone", "cron", "last_target_id",
		"last_overlap_target_id", "last_overlap_reason", "end_reason", "last_planning_error",
	} {
		texts[index], err = optionalScheduleResponseText(m, field)
		if err != nil {
			return ScheduleRecord{}, err
		}
	}
	catchupPolicy, err := requiredNullableScheduleText(m, "catchup_policy")
	if err != nil {
		return ScheduleRecord{}, err
	}
	if err := validateScheduleResponseCatchupPolicy(kind, catchupPolicy); err != nil {
		return ScheduleRecord{}, err
	}
	target, err := decodeScheduleTarget(codec, m["target"])
	if err != nil {
		return ScheduleRecord{}, err
	}
	overlapPolicy, err := requiredScheduleText(m, "overlap_policy")
	if err != nil {
		return ScheduleRecord{}, err
	}
	if !validScheduleOverlapPolicy(overlapPolicy) {
		return ScheduleRecord{}, fmt.Errorf("schedule response has invalid overlap_policy %q", overlapPolicy)
	}
	nextRunAtMS, err := requiredNullableScheduleInt(m, "next_run_at_ms")
	if err != nil {
		return ScheduleRecord{}, err
	}
	fireCount, err := requiredScheduleResponseInt(m, "fire_count")
	if err != nil {
		return ScheduleRecord{}, err
	}

	values := make([]*int64, 6)
	for index, field := range []string{
		"last_fire_at_ms", "max_fires", "end_at_ms", "last_overlap_at_ms",
		"last_skipped_at_ms", "overlap_queued_due_at_ms",
	} {
		values[index], err = optionalNonNegativeScheduleInt(m, field)
		if err != nil {
			return ScheduleRecord{}, err
		}
	}
	attempts, err := requiredScheduleResponseInt(m, "attempts")
	if err != nil {
		return ScheduleRecord{}, err
	}
	skippedCount, err := requiredScheduleResponseInt(m, "skipped_count")
	if err != nil {
		return ScheduleRecord{}, err
	}
	coalescedCount, err := requiredScheduleResponseInt(m, "coalesced_count")
	if err != nil {
		return ScheduleRecord{}, err
	}
	lastCoalescedCount, err := requiredScheduleResponseInt(m, "last_coalesced_count")
	if err != nil {
		return ScheduleRecord{}, err
	}
	lastCatchupAtMS, err := optionalNonNegativeScheduleInt(m, "last_catchup_at_ms")
	if err != nil {
		return ScheduleRecord{}, err
	}
	if err := validateScheduleResponseCatchupState(
		kind, coalescedCount, lastCoalescedCount, lastCatchupAtMS != nil,
	); err != nil {
		return ScheduleRecord{}, err
	}

	return ScheduleRecord{
		ID:                   id,
		FlowID:               flowID,
		Kind:                 kind,
		State:                status,
		Target:               target,
		Timezone:             texts[0],
		Cron:                 texts[1],
		CatchupPolicy:        catchupPolicy,
		OverlapPolicy:        overlapPolicy,
		NextRunAtMS:          nextRunAtMS,
		LastFireAtMS:         values[0],
		FireCount:            fireCount,
		Attempts:             attempts,
		MaxFires:             values[1],
		EndAtMS:              values[2],
		LastTargetID:         texts[2],
		LastOverlapAtMS:      values[3],
		LastOverlapTargetID:  texts[3],
		LastOverlapReason:    texts[4],
		LastSkippedAtMS:      values[4],
		SkippedCount:         skippedCount,
		OverlapQueuedDueAtMS: values[5],
		CoalescedCount:       coalescedCount,
		LastCatchupAtMS:      lastCatchupAtMS,
		LastCoalescedCount:   lastCoalescedCount,
		EndReason:            texts[5],
		LastPlanningError:    texts[6],
		Raw:                  m,
	}, nil
}

func validateScheduleResponseCatchupState(
	kind string,
	coalescedCount int64,
	lastCoalescedCount int64,
	lastCatchupAtPresent bool,
) error {
	if kind != "interval" {
		if coalescedCount != 0 {
			return errors.New("schedule response non-interval coalesced_count must be zero")
		}
		if lastCoalescedCount != 0 {
			return errors.New("schedule response non-interval last_coalesced_count must be zero")
		}
		if lastCatchupAtPresent {
			return errors.New("schedule response non-interval last_catchup_at_ms must be null")
		}
		return nil
	}
	if lastCoalescedCount > coalescedCount {
		return errors.New("schedule response last_coalesced_count exceeds coalesced_count")
	}
	if lastCoalescedCount > 0 && !lastCatchupAtPresent {
		return errors.New("schedule response last_catchup_at_ms is missing after catch-up")
	}
	if lastCoalescedCount == 0 && lastCatchupAtPresent {
		return errors.New("schedule response last_catchup_at_ms requires a catch-up")
	}
	return nil
}

func validScheduleResponseKind(kind string) bool {
	switch kind {
	case "one_shot", "delay", "interval", "cron":
		return true
	default:
		return false
	}
}

func validScheduleResponseState(state string) bool {
	switch state {
	case "active", "paused", "running", "completed", "failed", "cancelled":
		return true
	default:
		return false
	}
}

func validateScheduleResponseCatchupPolicy(kind, policy string) error {
	if kind == "interval" {
		if policy != "fire_once" {
			return errors.New("schedule response interval catchup_policy must be fire_once")
		}
		return nil
	}
	if policy != "" {
		return errors.New("schedule response catchup_policy is only valid for intervals")
	}
	return nil
}

func decodeScheduleTarget(codec Codec, value any) (map[string]any, error) {
	if value == nil {
		return nil, errors.New("schedule response is missing target")
	}
	target, err := nativeMap(value)
	if err != nil {
		return nil, fmt.Errorf("decode schedule target: %w", err)
	}
	if _, err := requiredScheduleText(target, "type"); err != nil {
		return nil, fmt.Errorf("schedule response target type: %w", err)
	}
	if raw, present := target["payload"]; present && raw != nil {
		decoded, err := decodeValue(codec, raw)
		if err != nil {
			return nil, fmt.Errorf("decode schedule target payload: %w", err)
		}
		target["payload"] = decoded
	}
	if raw, present := target["values"]; present && raw != nil {
		decoded, err := decodeMap(codec, raw)
		if err != nil {
			return nil, fmt.Errorf("decode schedule target values: %w", err)
		}
		target["values"] = decoded
	}
	return target, nil
}

func requiredNullableScheduleText(m map[string]any, field string) (string, error) {
	if _, present := m[field]; !present {
		return "", fmt.Errorf("schedule response is missing %s", field)
	}
	return optionalScheduleResponseText(m, field)
}

func requiredScheduleText(m map[string]any, field string) (string, error) {
	value, present := m[field]
	if !present || value == nil {
		return "", fmt.Errorf("schedule response is missing %s", field)
	}
	text, err := responseString(value, nil)
	if err != nil {
		return "", fmt.Errorf("decode schedule field %s: %w", field, err)
	}
	if strings.TrimSpace(text) == "" {
		return "", fmt.Errorf("schedule response %s must be non-empty", field)
	}
	return text, nil
}

func optionalScheduleResponseText(m map[string]any, field string) (string, error) {
	value, present := m[field]
	if !present || value == nil {
		return "", nil
	}
	text, err := responseString(value, nil)
	if err != nil {
		return "", fmt.Errorf("decode schedule field %s: %w", field, err)
	}
	if strings.TrimSpace(text) == "" {
		return "", fmt.Errorf("schedule response %s must be non-empty", field)
	}
	return text, nil
}

func requiredNullableScheduleInt(m map[string]any, field string) (*int64, error) {
	if _, present := m[field]; !present {
		return nil, fmt.Errorf("schedule response is missing %s", field)
	}
	return optionalNonNegativeScheduleInt(m, field)
}

func requiredScheduleResponseInt(m map[string]any, field string) (int64, error) {
	value, present, err := presentScheduleInt(m, field)
	if err != nil {
		return 0, err
	}
	if !present {
		return 0, fmt.Errorf("schedule response is missing %s", field)
	}
	return value, nil
}

func optionalNonNegativeScheduleInt(m map[string]any, field string) (*int64, error) {
	value, present, err := presentScheduleInt(m, field)
	if err != nil || !present {
		return nil, err
	}
	return &value, nil
}

func presentScheduleInt(m map[string]any, field string) (int64, bool, error) {
	value, present := m[field]
	if !present || value == nil {
		return 0, false, nil
	}
	parsed, err := responseInt64(value, nil)
	if err != nil {
		return 0, false, fmt.Errorf("decode schedule field %s: %w", field, err)
	}
	if parsed < 0 {
		return 0, false, fmt.Errorf("schedule response %s must be non-negative", field)
	}
	if parsed > maxFlowExactIntegerV080 {
		return 0, false, fmt.Errorf(
			"schedule response %s exceeds FerricStore 0.8 exact integer maximum %d",
			field,
			maxFlowExactIntegerV080,
		)
	}
	return parsed, true, nil
}
