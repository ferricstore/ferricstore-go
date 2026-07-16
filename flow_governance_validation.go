package ferricstore

import (
	"errors"
	"fmt"
	"math"
)

const maxLimitMutationAmount int64 = 1_000
const maxLimitReservationIDBytes = 256

func validateEffectReserve(id, effectKey, effectType string, opt EffectReserveOptions) error {
	for _, field := range []struct{ name, value string }{
		{name: "id", value: id},
		{name: "effect_key", value: effectKey},
		{name: "effect_type", value: effectType},
		{name: "lease_token", value: opt.LeaseToken},
		{name: "operation_digest", value: opt.OperationDigest},
	} {
		if err := validateRequiredText(field.name, field.value); err != nil {
			return err
		}
	}
	if opt.FencingToken == nil || *opt.FencingToken < 0 {
		return errors.New("fencing_token must be a non-negative integer")
	}
	return validateOptionalNonNegativeInt64("now_ms", opt.NowMS)
}

func validateEffectStatus(id, effectKey string, opt EffectStatusOptions) error {
	for _, field := range []struct{ name, value string }{
		{name: "id", value: id},
		{name: "effect_key", value: effectKey},
		{name: "lease_token", value: opt.LeaseToken},
	} {
		if err := validateRequiredText(field.name, field.value); err != nil {
			return err
		}
	}
	if opt.FencingToken == nil || *opt.FencingToken < 0 {
		return errors.New("fencing_token must be a non-negative integer")
	}
	if err := validateOptionalNonNegativeInt64("latency_ms", opt.LatencyMS); err != nil {
		return err
	}
	return validateOptionalNonNegativeInt64("now_ms", opt.NowMS)
}

func validateEffectGet(id, effectKey string) error {
	if err := validateRequiredText("id", id); err != nil {
		return err
	}
	return validateRequiredText("effect_key", effectKey)
}

func validateApprovalRequest(id string, opt ApprovalRequestOptions) error {
	for _, field := range []struct{ name, value string }{
		{name: "id", value: id},
		{name: "flow_id", value: opt.FlowID},
		{name: "scope", value: opt.Scope},
	} {
		if err := validateRequiredText(field.name, field.value); err != nil {
			return err
		}
	}
	if opt.PolicyHash != "" {
		if err := validateRequiredText("policy_hash", opt.PolicyHash); err != nil {
			return err
		}
	}
	if err := validatePolicyVersion(opt.PolicyVersion); err != nil {
		return err
	}
	if err := validateOptionalPositiveInt64("timeout_ms", opt.TimeoutMS); err != nil {
		return err
	}
	if opt.TimeoutMS != nil && opt.ExpiresAtMS != nil {
		return errors.New("timeout_ms and expires_at_ms are mutually exclusive")
	}
	if err := validateOptionalNonNegativeInt64("expires_at_ms", opt.ExpiresAtMS); err != nil {
		return err
	}
	if err := validateOptionalNonNegativeInt64("now_ms", opt.NowMS); err != nil {
		return err
	}
	for _, assignee := range opt.Assignees {
		if err := validateRequiredText("assignee", assignee); err != nil {
			return err
		}
	}
	return nil
}

func validatePolicyVersion(value any) error {
	if value == nil {
		return nil
	}
	switch version := value.(type) {
	case string:
		return validateRequiredText("policy_version", version)
	case []byte:
		return validateRequiredText("policy_version", string(version))
	default:
		parsed, err := responseInt64(value, nil)
		if err != nil || parsed < 0 {
			return errors.New("policy_version must be a non-empty string or non-negative integer")
		}
		return nil
	}
}

func validateApprovalDecision(id, approver string, nowMS *int64) error {
	if err := validateRequiredText("id", id); err != nil {
		return err
	}
	if err := validateRequiredText("approver", approver); err != nil {
		return err
	}
	return validateOptionalNonNegativeInt64("now_ms", nowMS)
}

func validateApprovalList(opt ApprovalListOptions) error {
	if opt.Status != "" {
		switch canonicalAdminEnum(opt.Status) {
		case "pending", "approved", "rejected":
		default:
			return errors.New("status must be pending, approved, or rejected")
		}
	}
	if err := validateAdminListFilters(opt.Scope, opt.PartitionKey, opt.FlowID); err != nil {
		return err
	}
	return validateOptionalPositiveInt("limit", opt.Limit)
}

func validateAdminListFilters(scope, partitionKey string, additional ...string) error {
	for index, field := range append([]string{scope, partitionKey}, additional...) {
		if field == "" {
			continue
		}
		name := "scope"
		if index == 1 {
			name = "partition_key"
		} else if index > 1 {
			name = "filter"
		}
		if err := validateRequiredText(name, field); err != nil {
			return err
		}
	}
	if scope != "" && partitionKey != "" {
		return errors.New("scope and partition_key are mutually exclusive")
	}
	return nil
}

func validateCircuitOperation(scope string, openMS, failureThreshold, nowMS *int64) error {
	if err := validateRequiredText("scope", scope); err != nil {
		return err
	}
	if err := validateOptionalPositiveInt64("open_ms", openMS); err != nil {
		return err
	}
	if err := validateOptionalPositiveInt64("failure_threshold", failureThreshold); err != nil {
		return err
	}
	return validateOptionalNonNegativeInt64("now_ms", nowMS)
}

func validateCircuitOpenOptions(scope string, opt CircuitOpenOptions) ([]string, error) {
	if err := validateCircuitOperation(scope, opt.OpenMS, opt.FailureThreshold, opt.NowMS); err != nil {
		return nil, err
	}
	for _, field := range []struct {
		name  string
		value *int64
	}{
		{name: "window_ms", value: opt.WindowMS},
		{name: "min_calls", value: opt.MinCalls},
		{name: "failure_rate_pct", value: opt.FailureRatePct},
		{name: "latency_threshold_ms", value: opt.LatencyThresholdMS},
		{name: "half_open_max_probes", value: opt.HalfOpenMaxProbes},
		{name: "half_open_success_threshold", value: opt.HalfOpenSuccessThreshold},
	} {
		if err := validateOptionalPositiveInt64(field.name, field.value); err != nil {
			return nil, err
		}
	}
	if opt.FailureRatePct != nil && *opt.FailureRatePct > 100 {
		return nil, errors.New("failure_rate_pct must be between 1 and 100")
	}
	if opt.MinCalls != nil && *opt.MinCalls > 64 {
		return nil, errors.New("min_calls cannot exceed 64")
	}
	threshold := int64(5)
	if opt.FailureThreshold != nil {
		threshold = *opt.FailureThreshold
	}
	if threshold > 64 {
		if opt.FailureRatePct == nil {
			return nil, errors.New("failure_threshold cannot exceed 64 without failure_rate_pct")
		}
		if opt.MinCalls == nil {
			return nil, errors.New("min_calls is required when failure_threshold exceeds 64")
		}
	}
	if err := validateOptionalNonNegativeInt64("deadline_ms", opt.DeadlineMS); err != nil {
		return nil, err
	}
	if opt.ErrorClasses == nil {
		return nil, nil
	}
	unique := make([]string, 0, len(opt.ErrorClasses))
	for _, value := range opt.ErrorClasses {
		if err := validateRequiredText("error_classes item", value); err != nil {
			return nil, err
		}
		duplicate := false
		for _, existing := range unique {
			if existing == value {
				duplicate = true
				break
			}
		}
		if !duplicate {
			unique = append(unique, value)
		}
	}
	return unique, nil
}

func validateBudgetReserve(scope string, amount int64, limit, windowMS *int64, reservationID string, nowMS *int64) error {
	if err := validateRequiredText("scope", scope); err != nil {
		return err
	}
	if amount <= 0 {
		return errors.New("amount must be positive")
	}
	if err := validateOptionalPositiveInt64("limit", limit); err != nil {
		return err
	}
	if err := validateOptionalPositiveInt64("window_ms", windowMS); err != nil {
		return err
	}
	if reservationID != "" {
		if err := validateRequiredText("reservation_id", reservationID); err != nil {
			return err
		}
	}
	return validateOptionalNonNegativeInt64("now_ms", nowMS)
}

func validateBudgetSettlement(scope, reservationID string, actualAmount *int64, nowMS *int64) error {
	if err := validateRequiredText("scope", scope); err != nil {
		return err
	}
	if err := validateRequiredText("reservation_id", reservationID); err != nil {
		return err
	}
	if actualAmount != nil && *actualAmount < 0 {
		return errors.New("actual_amount must be non-negative")
	}
	return validateOptionalNonNegativeInt64("now_ms", nowMS)
}

func validateAdminList(scope, partitionKey string, limit *int) error {
	if err := validateAdminListFilters(scope, partitionKey); err != nil {
		return err
	}
	return validateOptionalPositiveInt("limit", limit)
}

func validateLimitMutation(scope string, shardID, amount int64, nowMS *int64) error {
	if err := validateRequiredText("scope", scope); err != nil {
		return err
	}
	if shardID < 0 {
		return errors.New("shard_id must be non-negative")
	}
	if amount <= 0 || amount > maxLimitMutationAmount {
		return fmt.Errorf("amount must be between 1 and %d", maxLimitMutationAmount)
	}
	return validateOptionalNonNegativeInt64("now_ms", nowMS)
}

func validateLimitReleaseOptions(scope string, opt LimitReleaseOptions) ([]string, error) {
	if err := validateRequiredText("scope", scope); err != nil {
		return nil, err
	}
	if opt.ShardID < 0 {
		return nil, errors.New("shard_id must be non-negative")
	}
	if opt.ReservationIDs == nil {
		return nil, errors.New("reservation_ids must be provided for an exact release")
	}
	if opt.Amount != nil && (*opt.Amount <= 0 || *opt.Amount > maxLimitMutationAmount) {
		return nil, fmt.Errorf("amount must be between 1 and %d", maxLimitMutationAmount)
	}
	var reservationIDs []string
	if len(opt.ReservationIDs) == 0 {
		return nil, errors.New("reservation_ids must not be empty")
	}
	if opt.ReservationIDs != nil {
		if int64(len(opt.ReservationIDs)) > maxLimitMutationAmount {
			return nil, fmt.Errorf("reservation_ids cannot contain more than %d items", maxLimitMutationAmount)
		}
		reservationIDs = make([]string, 0, len(opt.ReservationIDs))
		seen := make(map[string]struct{}, len(opt.ReservationIDs))
		for _, id := range opt.ReservationIDs {
			if err := validateRequiredText("reservation_ids item", id); err != nil {
				return nil, err
			}
			if len(id) > maxLimitReservationIDBytes {
				return nil, fmt.Errorf("reservation_ids values cannot exceed %d bytes", maxLimitReservationIDBytes)
			}
			if _, duplicate := seen[id]; duplicate {
				return nil, errors.New("reservation_ids must contain unique values")
			}
			seen[id] = struct{}{}
			reservationIDs = append(reservationIDs, id)
		}
		if opt.Amount != nil && *opt.Amount != int64(len(reservationIDs)) {
			return nil, errors.New("amount must match the number of reservation_ids")
		}
	}
	if err := validateOptionalNonNegativeInt64("now_ms", opt.NowMS); err != nil {
		return nil, err
	}
	if err := validateOptionalNonNegativeInt64("deadline_ms", opt.DeadlineMS); err != nil {
		return nil, err
	}
	return reservationIDs, nil
}

func validateLimitLease(scope string, shardID, amount, ttlMS int64, limit, nowMS *int64) error {
	if err := validateLimitMutation(scope, shardID, amount, nowMS); err != nil {
		return err
	}
	if ttlMS <= 0 {
		return errors.New("ttl_ms must be positive")
	}
	if limit != nil && *limit < 0 {
		return errors.New("limit must be non-negative")
	}
	if nowMS != nil && *nowMS > math.MaxInt64-ttlMS {
		return errors.New("now_ms plus ttl_ms overflows int64")
	}
	return nil
}

func validateLimitGet(scope string, nowMS *int64) error {
	if err := validateRequiredText("scope", scope); err != nil {
		return err
	}
	return validateOptionalNonNegativeInt64("now_ms", nowMS)
}

func validateLimitList(scope, partitionKey string, limit *int, nowMS *int64) error {
	if err := validateAdminList(scope, partitionKey, limit); err != nil {
		return err
	}
	return validateOptionalNonNegativeInt64("now_ms", nowMS)
}
