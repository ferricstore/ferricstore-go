package ferricstore

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	maxFlowExactIntegerV080          int64 = 9_007_199_254_740_991
	maxFlowReferenceBytesV080              = 4_096
	reservedScheduleFlowTypeV080           = "__ferricstore_schedule"
	reservedScheduleFlowIDPrefixV080       = "__ferricstore_schedule__:"
)

func validatePublicFlowID(name, value string) error {
	if err := validateFlowMutationText(name, value); err != nil {
		return err
	}
	if strings.HasPrefix(value, reservedScheduleFlowIDPrefixV080) {
		return fmt.Errorf("%s is reserved for internal use", name)
	}
	return nil
}

func validatePublicFlowType(name, value string) error {
	if err := validateFlowMutationText(name, value); err != nil {
		return err
	}
	if value == reservedScheduleFlowTypeV080 {
		return fmt.Errorf("%s is reserved for internal use", name)
	}
	return nil
}

func validateFlowReference(name, value string) error {
	if len(value) > maxFlowReferenceBytesV080 {
		return fmt.Errorf("%s is too large (maximum %d bytes)", name, maxFlowReferenceBytesV080)
	}
	return nil
}

func validateOptionalPublicFlowIDReference(name, value string) error {
	if value == "" {
		return nil
	}
	if err := validatePublicFlowID(name, value); err != nil {
		return err
	}
	return validateFlowReference(name, value)
}

func validateFlowExactNonNegative(name string, value int64) error {
	if value < 0 {
		return fmt.Errorf("%s must be non-negative", name)
	}
	if value > maxFlowExactIntegerV080 {
		return fmt.Errorf("%s exceeds maximum %d", name, maxFlowExactIntegerV080)
	}
	return nil
}

func validateOptionalFlowExactNonNegative(name string, value *int64) error {
	if value == nil {
		return nil
	}
	return validateFlowExactNonNegative(name, *value)
}

func validateFlowExactPositive(name string, value int64) error {
	if value <= 0 {
		return fmt.Errorf("%s must be positive", name)
	}
	if value > maxFlowExactIntegerV080 {
		return fmt.Errorf("%s exceeds maximum %d", name, maxFlowExactIntegerV080)
	}
	return nil
}

func validateOptionalFlowExactPositive(name string, value *int64) error {
	if value == nil {
		return nil
	}
	return validateFlowExactPositive(name, *value)
}

func validateFlowDeadline(name string, suppliedNowMS, durationMS int64) error {
	now := suppliedNowMS
	if now == 0 {
		now = nowMS()
	}
	if err := validateFlowExactNonNegative("flow now milliseconds", now); err != nil {
		return err
	}
	if err := validateFlowExactPositive(name, durationMS); err != nil {
		return err
	}
	if now > maxFlowExactIntegerV080-durationMS {
		return fmt.Errorf("%s deadline exceeds maximum %d", name, maxFlowExactIntegerV080)
	}
	return nil
}

func validateOptionalFlowDeadline(name string, nowMS int64, durationMS *int64) error {
	if durationMS == nil {
		return nil
	}
	return validateFlowDeadline(name, nowMS, *durationMS)
}

func validateFlowHistoryEvent(name, value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	milliseconds, version, ok := strings.Cut(value, "-")
	if !ok || strings.Contains(version, "-") ||
		!canonicalFlowEventInteger(milliseconds) || !canonicalFlowEventInteger(version) {
		return 0, fmt.Errorf("%s must be a history event id", name)
	}
	parsed, _ := strconv.ParseInt(milliseconds, 10, 64)
	return parsed, nil
}

func canonicalFlowEventInteger(value string) bool {
	parsed, err := strconv.ParseInt(value, 10, 64)
	return err == nil && parsed >= 0 && parsed <= maxFlowExactIntegerV080 &&
		value == strconv.FormatInt(parsed, 10)
}
