package ferricstore

import (
	"fmt"
	"strings"
)

func scheduleResult(value any, err error) (ScheduleResult, error) {
	return scheduleResultWithCodec(value, err, RawCodec{})
}

func scheduleResultWithCodec(value any, err error, codec Codec) (ScheduleResult, error) {
	if err != nil {
		return ScheduleResult{}, err
	}
	m, err := nativeMap(value)
	if err != nil {
		return ScheduleResult{}, err
	}
	return scheduleResultFromMapWithCodec(m, codec)
}

func scheduleResultFromMapWithCodec(m map[string]any, codec Codec) (ScheduleResult, error) {
	id, err := requiredScheduleText(m, "id")
	if err != nil {
		return ScheduleResult{}, err
	}
	kind, err := requiredScheduleText(m, "kind")
	if err != nil {
		return ScheduleResult{}, err
	}
	status, err := scheduleTextAlias(m, "state", "status", true)
	if err != nil {
		return ScheduleResult{}, err
	}

	texts := make([]string, 9)
	for index, field := range []string{
		"flow_id", "timezone", "cron", "overlap_policy", "last_target_id",
		"last_overlap_target_id", "last_overlap_reason", "end_reason", "last_skip_reason",
	} {
		texts[index], err = optionalScheduleResponseText(m, field)
		if err != nil {
			return ScheduleResult{}, err
		}
	}
	target, err := decodeScheduleTarget(codec, m["target"])
	if err != nil {
		return ScheduleResult{}, err
	}
	nextFireAtMS, err := scheduleIntAlias(m, "next_run_at_ms", "next_fire_at_ms")
	if err != nil {
		return ScheduleResult{}, err
	}
	fires, err := scheduleIntAlias(m, "fire_count", "fires")
	if err != nil {
		return ScheduleResult{}, err
	}

	values := make([]int64, 8)
	for index, field := range []string{
		"last_fire_at_ms", "attempts", "max_fires", "end_at_ms", "last_overlap_at_ms",
		"last_skipped_at_ms", "skipped_count", "overlap_queued_due_at_ms",
	} {
		values[index], err = optionalNonNegativeScheduleInt(m, field)
		if err != nil {
			return ScheduleResult{}, err
		}
	}

	return ScheduleResult{
		ID:                   id,
		FlowID:               texts[0],
		Kind:                 kind,
		Status:               status,
		Target:               target,
		Timezone:             texts[1],
		Cron:                 texts[2],
		OverlapPolicy:        texts[3],
		NextFireAtMS:         nextFireAtMS,
		LastFireAtMS:         values[0],
		Fires:                fires,
		Attempts:             values[1],
		MaxFires:             values[2],
		EndAtMS:              values[3],
		LastTargetID:         texts[4],
		LastOverlapAtMS:      values[4],
		LastOverlapTargetID:  texts[5],
		LastOverlapReason:    texts[6],
		LastSkippedAtMS:      values[5],
		SkippedCount:         values[6],
		OverlapQueuedDueAtMS: values[7],
		EndReason:            texts[7],
		LastSkipReason:       texts[8],
		Raw:                  m,
	}, nil
}

func decodeScheduleTarget(codec Codec, value any) (map[string]any, error) {
	target, err := optionalNativeMap(value, "schedule target")
	if err != nil {
		return nil, err
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
	return text, nil
}

func scheduleTextAlias(m map[string]any, canonical, legacy string, required bool) (string, error) {
	canonicalText, canonicalPresent, err := presentScheduleText(m, canonical)
	if err != nil {
		return "", err
	}
	legacyText, legacyPresent, err := presentScheduleText(m, legacy)
	if err != nil {
		return "", err
	}
	if canonicalPresent && legacyPresent && canonicalText != legacyText {
		return "", fmt.Errorf("schedule response has conflicting %s and %s", canonical, legacy)
	}
	text := legacyText
	if canonicalPresent {
		text = canonicalText
	}
	if required && strings.TrimSpace(text) == "" {
		return "", fmt.Errorf("schedule response is missing %s", canonical)
	}
	return text, nil
}

func presentScheduleText(m map[string]any, field string) (string, bool, error) {
	value, present := m[field]
	if !present || value == nil {
		return "", false, nil
	}
	text, err := responseString(value, nil)
	if err != nil {
		return "", false, fmt.Errorf("decode schedule field %s: %w", field, err)
	}
	return text, true, nil
}

func scheduleIntAlias(m map[string]any, canonical, legacy string) (int64, error) {
	canonicalValue, canonicalPresent, err := presentScheduleInt(m, canonical)
	if err != nil {
		return 0, err
	}
	legacyValue, legacyPresent, err := presentScheduleInt(m, legacy)
	if err != nil {
		return 0, err
	}
	if canonicalPresent && legacyPresent && canonicalValue != legacyValue {
		return 0, fmt.Errorf("schedule response has conflicting %s and %s", canonical, legacy)
	}
	if canonicalPresent {
		return canonicalValue, nil
	}
	return legacyValue, nil
}

func optionalNonNegativeScheduleInt(m map[string]any, field string) (int64, error) {
	value, _, err := presentScheduleInt(m, field)
	return value, err
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
