package ferricstore

import (
	"errors"
	"fmt"
	"math"
	"strings"
)

func validateScheduleCreate(id string, opt ScheduleOptions) error {
	if err := validateRequiredText("id", id); err != nil {
		return err
	}
	kind, err := scheduleKind(opt)
	if err != nil {
		return err
	}
	recurring := kind == "interval" || kind == "cron"
	if err := validateScheduleTarget(opt.Target, recurring); err != nil {
		return err
	}
	if err := validateOptionalNonNegativeInt64("deadline_ms", opt.DeadlineMS); err != nil {
		return err
	}
	for _, field := range []struct {
		name  string
		value *int64
	}{
		{name: "at_ms", value: opt.AtMS},
		{name: "delay_ms", value: opt.DelayMS},
		{name: "start_at_ms", value: opt.StartAtMS},
		{name: "end_at_ms", value: opt.EndAtMS},
		{name: "now_ms", value: opt.NowMS},
	} {
		if err := validateOptionalFlowExactNonNegative(field.name, field.value); err != nil {
			return err
		}
	}
	for _, field := range []struct {
		name  string
		value *int64
	}{
		{name: "every_ms", value: opt.EveryMS},
		{name: "overlap_retry_ms", value: opt.OverlapRetryMS},
		{name: "max_fires", value: opt.MaxFires},
	} {
		if err := validateOptionalFlowExactPositive(field.name, field.value); err != nil {
			return err
		}
	}

	switch kind {
	case "delay":
		if opt.DelayMS == nil {
			return errors.New("delay_ms is required for delay schedules")
		}
		if opt.NowMS != nil && *opt.NowMS > maxFlowExactIntegerV080-*opt.DelayMS {
			return fmt.Errorf("now_ms plus delay_ms exceeds maximum %d", maxFlowExactIntegerV080)
		}
	case "interval":
		if opt.EveryMS == nil {
			return errors.New("every_ms is required for interval schedules")
		}
	case "cron":
		if err := validateRequiredText("cron", opt.Cron); err != nil {
			return err
		}
	}
	if kind != "cron" && opt.Timezone != "" {
		return errors.New("timezone is only supported for cron schedules")
	}
	if opt.Timezone != "" {
		if err := validateRequiredText("timezone", opt.Timezone); err != nil {
			return err
		}
	}
	if recurring {
		if opt.OverlapPolicy != "" && !validScheduleOverlapPolicy(opt.OverlapPolicy) {
			return errors.New("overlap_policy must be allow, skip, queue_after_previous, or fail_schedule")
		}
		if firstRun, known := knownFirstScheduleRun(kind, opt); known && opt.EndAtMS != nil && *opt.EndAtMS < firstRun {
			return errors.New("end_at_ms must be at or after first run")
		}
	} else {
		if opt.OverlapPolicy != "" {
			return errors.New("overlap_policy is only supported for recurring schedules")
		}
		if opt.MaxFires != nil {
			return errors.New("max_fires is only supported for recurring schedules")
		}
		if opt.EndAtMS != nil {
			return errors.New("end_at_ms is only supported for recurring schedules")
		}
	}
	return nil
}

func scheduleKind(opt ScheduleOptions) (string, error) {
	kind := strings.ToLower(strings.TrimSpace(opt.Kind))
	if kind == "" {
		switch {
		case opt.Cron != "":
			kind = "cron"
		case opt.EveryMS != nil:
			kind = "interval"
		case opt.DelayMS != nil:
			kind = "delay"
		default:
			kind = "one_shot"
		}
	}
	switch kind {
	case "one_shot", "delay", "interval", "cron":
		return kind, nil
	default:
		return "", errors.New("kind must be one_shot, delay, interval, or cron")
	}
}

func validateScheduleTarget(target map[string]any, recurring bool) error {
	if target == nil {
		return errors.New("target must be a mapping with a non-empty type")
	}
	for key := range target {
		if !validScheduleTargetField(key) {
			return fmt.Errorf("target contains unsupported field %q", key)
		}
	}
	if err := validateRequiredAnyText("target type", target["type"]); err != nil {
		return err
	}
	if asString(target["type"]) == reservedScheduleFlowTypeV080 {
		return errors.New("target type is reserved for internal use")
	}
	for _, key := range []string{"state", "id", "id_prefix", "partition_key", "correlation_id", "parent_flow_id", "root_flow_id"} {
		value, exists := target[key]
		if !exists || value == nil {
			continue
		}
		if err := validateRequiredAnyText("target "+key, value); err != nil {
			return err
		}
		switch key {
		case "parent_flow_id", "root_flow_id":
			if err := validateOptionalPublicFlowIDReference("target "+key, asString(value)); err != nil {
				return err
			}
		case "correlation_id":
			if err := validateFlowReference("target "+key, asString(value)); err != nil {
				return err
			}
		}
	}
	if recurring {
		if value, exists := target["id"]; exists && value != nil {
			return errors.New("target id is not supported for recurring schedules; use id_prefix")
		}
	}
	if value, exists := target["run_at_ms"]; exists && value != nil {
		parsed, err := responseInt64(value, nil)
		if err != nil {
			return errors.New("target run_at_ms must be a non-negative integer")
		}
		if err := validateFlowExactNonNegative("target run_at_ms", parsed); err != nil {
			return err
		}
	}
	if value, exists := target["priority"]; exists && value != nil {
		priority, err := responseInt64(value, nil)
		if err != nil || priority < 0 || priority > 2 {
			return errors.New("target priority must be between 0 and 2")
		}
	}
	for _, key := range []string{"payload_ref"} {
		value, exists := target[key]
		if exists && value != nil {
			if err := validateRequiredAnyText("target "+key, value); err != nil {
				return err
			}
		}
	}
	for _, key := range []string{"id", "id_prefix"} {
		value, exists := target[key]
		if exists && value != nil && strings.HasPrefix(asString(value), reservedScheduleFlowIDPrefixV080) {
			return fmt.Errorf("target %s is reserved for internal use", key)
		}
	}
	if err := validateScheduleTargetNamedValues(target); err != nil {
		return err
	}
	return nil
}

func validScheduleTargetField(key string) bool {
	switch key {
	case "type", "state", "id", "id_prefix", "partition_key", "correlation_id",
		"parent_flow_id", "root_flow_id", "run_at_ms", "priority", "payload",
		"payload_ref", "values", "value_refs":
		return true
	default:
		return false
	}
}

func validateScheduleTargetNamedValues(target map[string]any) error {
	if value, exists := target["values"]; exists && value != nil {
		values, ok := value.(map[string]any)
		if !ok {
			return errors.New("target values must be a string-keyed mapping")
		}
		if err := validateNamedValues(NamedValues{Values: values}); err != nil {
			return err
		}
	}
	if value, exists := target["value_refs"]; exists && value != nil {
		refs, ok := value.(map[string]string)
		if !ok {
			return errors.New("target value_refs must be a string mapping")
		}
		if err := validateNamedValues(NamedValues{ValueRefs: refs}); err != nil {
			return err
		}
	}
	return nil
}

func validScheduleOverlapPolicy(value string) bool {
	switch canonicalAdminEnum(value) {
	case "allow", "skip", "queue_after_previous", "fail_schedule":
		return true
	default:
		return false
	}
}

func knownFirstScheduleRun(kind string, opt ScheduleOptions) (int64, bool) {
	if kind == "delay" {
		if opt.NowMS == nil || opt.DelayMS == nil || *opt.NowMS > math.MaxInt64-*opt.DelayMS {
			return 0, false
		}
		return *opt.NowMS + *opt.DelayMS, true
	}
	if kind == "cron" {
		if opt.StartAtMS != nil {
			return *opt.StartAtMS, true
		}
		if opt.AtMS != nil {
			return *opt.AtMS, true
		}
	} else {
		if opt.AtMS != nil {
			return *opt.AtMS, true
		}
		if opt.StartAtMS != nil {
			return *opt.StartAtMS, true
		}
	}
	if opt.NowMS != nil {
		return *opt.NowMS, true
	}
	return 0, false
}

func canonicalAdminEnum(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func validateScheduleGet(id string, deadlineMS *int64) error {
	if err := validateRequiredText("id", id); err != nil {
		return err
	}
	return validateOptionalNonNegativeInt64("deadline_ms", deadlineMS)
}

func validateScheduleStatus(id string, opt ScheduleStatusOptions) error {
	if err := validateRequiredText("id", id); err != nil {
		return err
	}
	if err := validateOptionalFlowExactNonNegative("now_ms", opt.NowMS); err != nil {
		return err
	}
	return validateOptionalNonNegativeInt64("deadline_ms", opt.DeadlineMS)
}

func validateScheduleFireDueOptions(opt ScheduleFireDueOptions) error {
	if err := validateOptionalFlowExactNonNegative("now_ms", opt.NowMS); err != nil {
		return err
	}
	if opt.Worker != "" {
		if err := validateRequiredText("worker", opt.Worker); err != nil {
			return err
		}
	}
	leaseMS := int64(30_000)
	if opt.LeaseMS != nil {
		leaseMS = *opt.LeaseMS
	}
	now := nowMS()
	if opt.NowMS != nil {
		now = *opt.NowMS
	}
	if err := validateFlowExactPositive("lease_ms", leaseMS); err != nil {
		return err
	}
	if now > maxFlowExactIntegerV080-leaseMS {
		return errors.New("now_ms + lease_ms exceeds the exact integer range")
	}
	if err := validateOptionalBlockingTimeoutMS("FLOW.SCHEDULE.FIRE_DUE", opt.BlockMS); err != nil {
		return err
	}
	if err := validateOptionalPositiveInt("limit", opt.Limit); err != nil {
		return err
	}
	return validateOptionalNonNegativeInt64("deadline_ms", opt.DeadlineMS)
}

func validateScheduleFireOptions(id string, opt ScheduleFireOptions) error {
	if err := validateRequiredText("id", id); err != nil {
		return err
	}
	if err := validateOptionalFlowExactNonNegative("now_ms", opt.NowMS); err != nil {
		return err
	}
	if err := validateOptionalFlowExactNonNegative("fire_at_ms", opt.FireAtMS); err != nil {
		return err
	}
	return validateOptionalNonNegativeInt64("deadline_ms", opt.DeadlineMS)
}

func validateScheduleList(opt ScheduleListOptions) error {
	if opt.Kind != "" {
		if _, err := scheduleKind(ScheduleOptions{Kind: opt.Kind}); err != nil {
			return err
		}
	}
	for _, field := range []struct {
		name  string
		value *int64
	}{
		{name: "from_ms", value: opt.FromMS},
		{name: "to_ms", value: opt.ToMS},
	} {
		if err := validateOptionalFlowExactNonNegative(field.name, field.value); err != nil {
			return err
		}
	}
	if err := validateOptionalPositiveInt("count", opt.Count); err != nil {
		return err
	}
	return validateOptionalNonNegativeInt64("deadline_ms", opt.DeadlineMS)
}

func validateFlowReadOptions(opt ReadOptions) error {
	if err := validateFlowReadRange(opt.Count, opt.FromMS, opt.ToMS); err != nil {
		return err
	}
	return validateFlowAttributes(opt.Attributes)
}

func validateRequiredText(name, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s must be a non-empty string", name)
	}
	return nil
}

func validateRequiredAnyText(name string, value any) error {
	switch text := value.(type) {
	case string:
		return validateRequiredText(name, text)
	case []byte:
		return validateRequiredText(name, string(text))
	default:
		return fmt.Errorf("%s must be a non-empty string", name)
	}
}

func validateOptionalNonNegativeInt64(name string, value *int64) error {
	if value != nil && *value < 0 {
		return fmt.Errorf("%s must be non-negative", name)
	}
	return nil
}

func validateOptionalPositiveInt64(name string, value *int64) error {
	if value != nil && *value <= 0 {
		return fmt.Errorf("%s must be positive", name)
	}
	return nil
}

func validateOptionalPositiveInt(name string, value *int) error {
	if value != nil && *value <= 0 {
		return fmt.Errorf("%s must be positive", name)
	}
	return nil
}
