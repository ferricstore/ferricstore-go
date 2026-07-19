package ferricstore

import (
	"errors"
	"fmt"
)

const maxLimitMutationAmount int64 = 1_000
const maxLimitReservationIDBytes = maxGovernanceReservationIDBytesV080

func validateEffectReserve(id, effectKey, effectType string, opt EffectReserveOptions) error {
	if err := validatePublicFlowID("id", id); err != nil {
		return err
	}
	for _, field := range []struct {
		name, value string
		maximum     int
	}{
		{name: "id", value: id, maximum: maxGovernanceDimensionBytesV080},
		{name: "effect_key", value: effectKey, maximum: maxGovernanceDimensionBytesV080},
		{name: "effect_type", value: effectType, maximum: maxGovernanceDimensionBytesV080},
		{name: "operation_digest", value: opt.OperationDigest, maximum: maxGovernanceFieldBytesV080},
	} {
		if err := validateGovernanceRequiredText(field.name, field.value, field.maximum); err != nil {
			return err
		}
	}
	if err := validateRequiredText("lease_token", opt.LeaseToken); err != nil {
		return err
	}
	for _, field := range []struct{ name, value string }{
		{name: "partition_key", value: opt.PartitionKey},
		{name: "governance_scope", value: opt.GovernanceScope},
	} {
		if err := validateGovernanceOptionalText(field.name, field.value, maxGovernanceDimensionBytesV080); err != nil {
			return err
		}
	}
	if err := validateGovernanceOptionalText("idempotency_key", opt.IdempotencyKey, maxGovernanceFieldBytesV080); err != nil {
		return err
	}
	if opt.FencingToken == nil {
		return errors.New("fencing_token must be a non-negative integer")
	}
	if err := validateFlowExactNonNegative("fencing_token", *opt.FencingToken); err != nil {
		return err
	}
	return validateOptionalGovernanceExactNonNegative("now_ms", opt.NowMS)
}

func validateEffectStatus(id, effectKey string, opt EffectStatusOptions) error {
	if err := validatePublicFlowID("id", id); err != nil {
		return err
	}
	for _, field := range []struct {
		name, value string
		maximum     int
	}{
		{name: "id", value: id, maximum: maxGovernanceDimensionBytesV080},
		{name: "effect_key", value: effectKey, maximum: maxGovernanceDimensionBytesV080},
	} {
		if err := validateGovernanceRequiredText(field.name, field.value, field.maximum); err != nil {
			return err
		}
	}
	if err := validateRequiredText("lease_token", opt.LeaseToken); err != nil {
		return err
	}
	if err := validateGovernanceOptionalText("partition_key", opt.PartitionKey, maxGovernanceDimensionBytesV080); err != nil {
		return err
	}
	for _, field := range []struct{ name, value string }{
		{name: "external_id", value: opt.ExternalID},
		{name: "error", value: opt.Error},
		{name: "reason", value: opt.Reason},
	} {
		if err := validateGovernanceOptionalText(field.name, field.value, maxGovernanceFieldBytesV080); err != nil {
			return err
		}
	}
	if opt.FencingToken == nil {
		return errors.New("fencing_token must be a non-negative integer")
	}
	if err := validateFlowExactNonNegative("fencing_token", *opt.FencingToken); err != nil {
		return err
	}
	if err := validateOptionalGovernanceExactNonNegative("latency_ms", opt.LatencyMS); err != nil {
		return err
	}
	return validateOptionalGovernanceExactNonNegative("now_ms", opt.NowMS)
}

func validateEffectGet(id, effectKey, partitionKey string) error {
	if err := validatePublicFlowID("id", id); err != nil {
		return err
	}
	if err := validateGovernanceRequiredText("id", id, maxGovernanceDimensionBytesV080); err != nil {
		return err
	}
	if err := validateGovernanceRequiredText("effect_key", effectKey, maxGovernanceDimensionBytesV080); err != nil {
		return err
	}
	return validateGovernanceOptionalText("partition_key", partitionKey, maxGovernanceDimensionBytesV080)
}

func validateApprovalRequest(id string, opt ApprovalRequestOptions) error {
	if err := validatePublicFlowID("flow_id", opt.FlowID); err != nil {
		return err
	}
	for _, field := range []struct{ name, value string }{
		{name: "id", value: id},
		{name: "flow_id", value: opt.FlowID},
		{name: "scope", value: opt.Scope},
	} {
		if err := validateGovernanceRequiredText(field.name, field.value, maxGovernanceDimensionBytesV080); err != nil {
			return err
		}
	}
	for _, field := range []struct{ name, value string }{
		{name: "reason", value: opt.Reason},
		{name: "requested_by", value: opt.RequestedBy},
		{name: "policy_hash", value: opt.PolicyHash},
	} {
		if err := validateGovernanceOptionalText(field.name, field.value, maxGovernanceFieldBytesV080); err != nil {
			return err
		}
	}
	if err := validatePolicyVersion(opt.PolicyVersion); err != nil {
		return err
	}
	if opt.TimeoutMS != nil && opt.ExpiresAtMS != nil {
		return errors.New("timeout_ms and expires_at_ms are mutually exclusive")
	}
	if err := validateOptionalGovernanceExactNonNegative("now_ms", opt.NowMS); err != nil {
		return err
	}
	now := nowMS()
	if opt.NowMS != nil {
		now = *opt.NowMS
	}
	if opt.TimeoutMS != nil {
		if err := validateFlowExactPositive("timeout_ms", *opt.TimeoutMS); err != nil {
			return err
		}
		if now > maxFlowExactIntegerV080-*opt.TimeoutMS {
			return errors.New("now_ms + timeout_ms exceeds the exact integer range")
		}
	}
	if opt.ExpiresAtMS != nil && (*opt.ExpiresAtMS <= now || *opt.ExpiresAtMS > maxFlowExactIntegerV080) {
		return errors.New("expires_at_ms must be greater than now_ms and within the exact integer range")
	}
	if len(opt.Assignees) > maxApprovalAssigneesV080 {
		return fmt.Errorf("assignees cannot contain more than %d items", maxApprovalAssigneesV080)
	}
	for _, assignee := range opt.Assignees {
		if err := validateGovernanceRequiredText("assignee", assignee, maxGovernanceDimensionBytesV080); err != nil {
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
		return validateGovernanceRequiredText("policy_version", version, maxGovernanceFieldBytesV080)
	case []byte:
		return validateGovernanceRequiredText("policy_version", string(version), maxGovernanceFieldBytesV080)
	default:
		parsed, err := responseInt64(value, nil)
		if err != nil {
			return errors.New("policy_version must be a non-empty string or non-negative integer")
		}
		return validateFlowExactNonNegative("policy_version", parsed)
	}
}

func validateApprovalDecision(id, approver, reason string, nowMS *int64) error {
	if err := validateGovernanceRequiredText("id", id, maxGovernanceDimensionBytesV080); err != nil {
		return err
	}
	if err := validateGovernanceRequiredText("approver", approver, maxGovernanceFieldBytesV080); err != nil {
		return err
	}
	if err := validateGovernanceOptionalText("reason", reason, maxGovernanceFieldBytesV080); err != nil {
		return err
	}
	return validateOptionalGovernanceExactNonNegative("now_ms", nowMS)
}

func validateApprovalList(opt ApprovalListOptions) error {
	if opt.Status != "" {
		switch canonicalAdminEnum(opt.Status) {
		case "pending", "approved", "rejected", "expired":
		default:
			return errors.New("status must be pending, approved, rejected, or expired")
		}
	}
	if opt.FlowID != "" {
		if err := validatePublicFlowID("flow_id", opt.FlowID); err != nil {
			return err
		}
	}
	if err := validateAdminListFilters(opt.Scope, opt.PartitionKey, opt.FlowID); err != nil {
		return err
	}
	return validateOptionalPositiveInt("limit", opt.Limit)
}

func validateGovernanceLedger(id string, opt GovernanceLedgerOptions) error {
	if err := validatePublicFlowID("id", id); err != nil {
		return err
	}
	if err := validateGovernanceRequiredText("id", id, maxGovernanceDimensionBytesV080); err != nil {
		return err
	}
	if err := validateGovernanceOptionalText("partition_key", opt.PartitionKey, maxGovernanceDimensionBytesV080); err != nil {
		return err
	}
	if err := validateOptionalPositiveInt("limit", opt.Limit); err != nil {
		return err
	}
	if err := validateOptionalGovernanceExactNonNegative("from_ms", opt.FromMS); err != nil {
		return err
	}
	if err := validateOptionalGovernanceExactNonNegative("to_ms", opt.ToMS); err != nil {
		return err
	}
	if opt.FromMS != nil && opt.ToMS != nil && *opt.FromMS > *opt.ToMS {
		return errors.New("from_ms cannot exceed to_ms")
	}
	return nil
}

func validateAdminListFilters(scope, partitionKey string, additional ...string) error {
	name, value := "scope", scope
	if value == "" {
		name, value = "partition_key", partitionKey
	}
	if value != "" {
		if err := validateGovernanceRequiredText(name, value, maxGovernanceDimensionBytesV080); err != nil {
			return err
		}
	}
	for _, field := range additional {
		if field != "" {
			if err := validateGovernanceRequiredText("filter", field, maxGovernanceDimensionBytesV080); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateCircuitOperation(scope string, openMS, failureThreshold, nowMS *int64) error {
	if err := validateGovernanceScope("scope", scope); err != nil {
		return err
	}
	if err := validateOptionalGovernanceExactPositive("open_ms", openMS); err != nil {
		return err
	}
	if err := validateOptionalGovernanceExactPositive("failure_threshold", failureThreshold); err != nil {
		return err
	}
	return validateOptionalGovernanceExactNonNegative("now_ms", nowMS)
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
		if err := validateOptionalGovernanceExactPositive(field.name, field.value); err != nil {
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
	if len(opt.ErrorClasses) > maxCircuitErrorClassesV080 {
		return nil, fmt.Errorf("error_classes cannot contain more than %d items", maxCircuitErrorClassesV080)
	}
	unique := make([]string, 0, len(opt.ErrorClasses))
	seen := make(map[string]struct{}, len(opt.ErrorClasses))
	for _, value := range opt.ErrorClasses {
		if err := validateGovernanceRequiredText("error_classes item", value, maxCircuitErrorClassBytesV080); err != nil {
			return nil, err
		}
		if _, duplicate := seen[value]; !duplicate {
			seen[value] = struct{}{}
			unique = append(unique, value)
		}
	}
	return unique, nil
}

func validateBudgetReserve(scope string, amount int64, limit, windowMS *int64, reservationID string, nowMS *int64) error {
	if err := validateGovernanceScope("scope", scope); err != nil {
		return err
	}
	if err := validateFlowExactPositive("amount", amount); err != nil {
		return err
	}
	if err := validateOptionalGovernanceExactPositive("limit", limit); err != nil {
		return err
	}
	if err := validateOptionalGovernanceExactPositive("window_ms", windowMS); err != nil {
		return err
	}
	if reservationID != "" {
		if err := validateGovernanceRequiredText("reservation_id", reservationID, maxGovernanceReservationIDBytesV080); err != nil {
			return err
		}
	}
	return validateOptionalGovernanceExactNonNegative("now_ms", nowMS)
}

func validateBudgetSettlement(scope, reservationID string, actualAmount *int64, nowMS *int64) error {
	if err := validateGovernanceScope("scope", scope); err != nil {
		return err
	}
	if err := validateGovernanceRequiredText("reservation_id", reservationID, maxGovernanceReservationIDBytesV080); err != nil {
		return err
	}
	if err := validateOptionalGovernanceExactNonNegative("actual_amount", actualAmount); err != nil {
		return err
	}
	return validateOptionalGovernanceExactNonNegative("now_ms", nowMS)
}

func validateAdminList(scope, partitionKey string, limit *int) error {
	if err := validateAdminListFilters(scope, partitionKey); err != nil {
		return err
	}
	return validateOptionalPositiveInt("limit", limit)
}

func validateLimitMutation(scope string, shardID, amount int64, nowMS *int64) error {
	if err := validateGovernanceScope("scope", scope); err != nil {
		return err
	}
	if err := validateFlowExactNonNegative("shard_id", shardID); err != nil {
		return err
	}
	if amount <= 0 || amount > maxLimitMutationAmount {
		return fmt.Errorf("amount must be between 1 and %d", maxLimitMutationAmount)
	}
	return validateOptionalGovernanceExactNonNegative("now_ms", nowMS)
}

func validateLimitReleaseOptions(scope string, opt LimitReleaseOptions) ([]string, error) {
	if err := validateGovernanceScope("scope", scope); err != nil {
		return nil, err
	}
	if err := validateFlowExactNonNegative("shard_id", opt.ShardID); err != nil {
		return nil, err
	}
	if len(opt.ReservationIDs) == 0 {
		return nil, errors.New("reservation_ids must contain at least one reservation ID")
	}
	if int64(len(opt.ReservationIDs)) > maxLimitMutationAmount {
		return nil, fmt.Errorf("reservation_ids cannot contain more than %d items", maxLimitMutationAmount)
	}
	reservationIDs := make([]string, 0, len(opt.ReservationIDs))
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
	if err := validateOptionalGovernanceExactNonNegative("now_ms", opt.NowMS); err != nil {
		return nil, err
	}
	if err := validateOptionalNonNegativeInt64("deadline_ms", opt.DeadlineMS); err != nil {
		return nil, err
	}
	return reservationIDs, nil
}

func validateLimitLease(scope string, shardID, amount, ttlMS int64, limit, nowValue *int64) error {
	if err := validateLimitMutation(scope, shardID, amount, nowValue); err != nil {
		return err
	}
	if err := validateFlowExactPositive("ttl_ms", ttlMS); err != nil {
		return err
	}
	if err := validateOptionalGovernanceExactNonNegative("limit", limit); err != nil {
		return err
	}
	now := nowMS()
	if nowValue != nil {
		now = *nowValue
	}
	if now > maxFlowExactIntegerV080-ttlMS {
		return errors.New("now_ms + ttl_ms exceeds the exact integer range")
	}
	return nil
}

func validateLimitGet(scope string, nowMS *int64) error {
	if err := validateGovernanceScope("scope", scope); err != nil {
		return err
	}
	return validateOptionalGovernanceExactNonNegative("now_ms", nowMS)
}

func validateLimitList(scope, partitionKey string, limit *int, nowMS *int64) error {
	if err := validateAdminList(scope, partitionKey, limit); err != nil {
		return err
	}
	return validateOptionalGovernanceExactNonNegative("now_ms", nowMS)
}
