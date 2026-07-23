package ferricstore

import (
	"errors"
	"strings"
)

func validateFlowGet(id string, values []string) error {
	if err := validatePublicFlowID("flow id", id); err != nil {
		return err
	}
	if err := validateClaimStrings("value names", values); err != nil {
		return err
	}
	return nil
}

func validateFlowSearch(opt SearchOptions) error {
	if opt.Type != "" {
		if err := validatePublicFlowType("flow type", opt.Type); err != nil {
			return err
		}
	}
	if err := validateFlowReadRange(opt.Count, opt.FromMS, opt.ToMS); err != nil {
		return err
	}
	if err := validateFlowAttributes(opt.Attributes); err != nil {
		return err
	}
	return validateFlowStateMetaQuery(opt.StateMeta)
}

func validateFlowStateMetaQuery(stateMeta map[string]map[string]any) error {
	if len(stateMeta) > maxFlowStateMetaStates {
		return errors.New("too many flow state_meta states")
	}
	total := 0
	var normalized map[string]struct{}
	if len(stateMeta) > 1 {
		normalized = make(map[string]struct{}, len(stateMeta))
	}
	for state, meta := range stateMeta {
		state = strings.TrimSpace(state)
		if state == "" {
			return errors.New("flow state_meta state must not be empty")
		}
		if len(state) > maxFlowStateMetaStateBytes {
			return errors.New("flow state_meta state is too large")
		}
		if _, exists := normalized[state]; exists {
			return errors.New("flow state_meta state is duplicated after normalization")
		}
		if normalized != nil {
			normalized[state] = struct{}{}
		}
		size, err := flowStateMetaEntrySize(meta)
		if err != nil {
			return err
		}
		total += len(state) + size
	}
	if total > maxFlowStateMetaTotalBytes {
		return errors.New("flow state_meta is too large")
	}
	return nil
}

func validateFlowStuck(flowType string, count *int, olderThanMS, nowMS *int64) error {
	if err := validatePublicFlowType("flow type", flowType); err != nil {
		return err
	}
	if err := validateOptionalPositiveInt("count", count); err != nil {
		return err
	}
	if err := validateOptionalFlowExactNonNegative("older_than_ms", olderThanMS); err != nil {
		return err
	}
	return validateOptionalFlowExactNonNegative("now_ms", nowMS)
}

func validateFlowHistory(opt HistoryOptions) error {
	if err := validatePublicFlowID("flow id", opt.ID); err != nil {
		return err
	}
	if opt.Count < 0 {
		return errors.New("count must be positive")
	}
	if err := validateFlowReadRange(nil, opt.FromMS, opt.ToMS); err != nil {
		return err
	}
	if err := validateOptionalFlowExactNonNegative("from_version", opt.FromVersion); err != nil {
		return err
	}
	if err := validateOptionalFlowExactNonNegative("to_version", opt.ToVersion); err != nil {
		return err
	}
	if opt.FromVersion != nil && opt.ToVersion != nil && *opt.FromVersion > *opt.ToVersion {
		return errors.New("from_version must not exceed to_version")
	}
	fromEventMS, err := validateFlowHistoryEvent("from_event", opt.FromEvent)
	if err != nil {
		return err
	}
	toEventMS, err := validateFlowHistoryEvent("to_event", opt.ToEvent)
	if err != nil {
		return err
	}
	if opt.FromEvent != "" && opt.ToEvent != "" &&
		(fromEventMS > toEventMS || fromEventMS == toEventMS && opt.FromEvent > opt.ToEvent) {
		return errors.New("from_event must not exceed to_event")
	}
	return validateOptionalFlowExactNonNegative("payload_max_bytes", opt.PayloadMaxBytes)
}

func validateFlowReadRange(count *int, fromMS, toMS *int64) error {
	if err := validateOptionalPositiveInt("count", count); err != nil {
		return err
	}
	if err := validateOptionalFlowExactNonNegative("from_ms", fromMS); err != nil {
		return err
	}
	if err := validateOptionalFlowExactNonNegative("to_ms", toMS); err != nil {
		return err
	}
	if fromMS != nil && toMS != nil && *fromMS > *toMS {
		return errors.New("from_ms must not exceed to_ms")
	}
	return nil
}
