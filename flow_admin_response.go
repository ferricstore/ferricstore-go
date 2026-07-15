package ferricstore

import (
	"errors"
	"fmt"
	"strconv"
)

func mapResult(value any, err error) (map[string]any, error) {
	if err != nil {
		return nil, err
	}
	return nativeMap(value)
}

func statsCount(stats map[string]any) (int64, error) {
	value, ok := stats["count"]
	if !ok {
		return 0, errors.New("FLOW.STATS response missing count")
	}
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case uint64:
		if v > uint64(^uint64(0)>>1) {
			return 0, errors.New("FLOW.STATS response count overflows int64")
		}
		return int64(v), nil
	case uint:
		if uint64(v) > uint64(^uint64(0)>>1) {
			return 0, errors.New("FLOW.STATS response count overflows int64")
		}
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case string:
		count, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, errors.New("FLOW.STATS response count is not numeric")
		}
		return count, nil
	case []byte:
		count, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return 0, errors.New("FLOW.STATS response count is not numeric")
		}
		return count, nil
	default:
		return 0, errors.New("FLOW.STATS response count is not numeric")
	}
}

func mapList(value any, err error) ([]map[string]any, error) {
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, errors.New("expected array response")
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		m, err := nativeMap(item)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

func adminInt64(m map[string]any, field string) (int64, error) {
	value, exists := m[field]
	if !exists || value == nil {
		return 0, nil
	}
	parsed, err := responseInt64(value, nil)
	if err != nil {
		return 0, fmt.Errorf("decode admin field %s: %w", field, err)
	}
	return parsed, nil
}

func adminFloat64(m map[string]any, field string) (float64, error) {
	value, exists := m[field]
	if !exists || value == nil {
		return 0, nil
	}
	parsed, err := responseFloat64(value, nil)
	if err != nil {
		return 0, fmt.Errorf("decode admin field %s: %w", field, err)
	}
	return parsed, nil
}

func adminStringList(value any, field string) ([]string, error) {
	if value == nil {
		return []string{}, nil
	}
	if values, ok := value.([]string); ok {
		return append([]string(nil), values...), nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("decode %s: expected array, got %T", field, value)
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		text, err := responseString(item, nil)
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", field, err)
		}
		out = append(out, text)
	}
	return out, nil
}

func scheduleResult(value any, err error) (ScheduleResult, error) {
	if err != nil {
		return ScheduleResult{}, err
	}
	m, err := nativeMap(value)
	if err != nil {
		return ScheduleResult{}, err
	}
	return scheduleResultFromMap(m)
}

func scheduleResultFromMap(m map[string]any) (ScheduleResult, error) {
	target, err := optionalNativeMap(m["target"], "schedule target")
	if err != nil {
		return ScheduleResult{}, err
	}
	nextFireAtMS, err := adminInt64(m, "next_fire_at_ms")
	if err != nil {
		return ScheduleResult{}, err
	}
	lastFireAtMS, err := adminInt64(m, "last_fire_at_ms")
	if err != nil {
		return ScheduleResult{}, err
	}
	fires, err := adminInt64(m, "fires")
	if err != nil {
		return ScheduleResult{}, err
	}
	maxFires, err := adminInt64(m, "max_fires")
	if err != nil {
		return ScheduleResult{}, err
	}
	endAtMS, err := adminInt64(m, "end_at_ms")
	if err != nil {
		return ScheduleResult{}, err
	}
	return ScheduleResult{
		ID:            asString(m["id"]),
		Kind:          asString(m["kind"]),
		Status:        asString(m["status"]),
		Target:        target,
		Timezone:      asString(m["timezone"]),
		Cron:          asString(m["cron"]),
		OverlapPolicy: asString(m["overlap_policy"]),
		NextFireAtMS:  nextFireAtMS,
		LastFireAtMS:  lastFireAtMS,
		Fires:         fires,
		MaxFires:      maxFires,
		EndAtMS:       endAtMS,
		Raw:           m,
	}, nil
}

func effectResult(value any, err error) (EffectResult, error) {
	if err != nil {
		return EffectResult{}, err
	}
	m, err := nativeMap(value)
	if err != nil {
		return EffectResult{}, err
	}
	return EffectResult{
		ID:              asString(m["id"]),
		EffectKey:       asString(m["effect_key"]),
		EffectType:      asString(m["effect_type"]),
		Status:          asString(m["status"]),
		ExternalID:      asString(m["external_id"]),
		Error:           asString(m["error"]),
		Reason:          asString(m["reason"]),
		OperationDigest: asString(m["operation_digest"]),
		IdempotencyKey:  asString(m["idempotency_key"]),
		Raw:             m,
	}, nil
}

func approvalResult(value any, err error) (ApprovalResult, error) {
	if err != nil {
		return ApprovalResult{}, err
	}
	m, err := nativeMap(value)
	if err != nil {
		return ApprovalResult{}, err
	}
	return approvalResultFromMap(m)
}

func approvalResultFromMap(m map[string]any) (ApprovalResult, error) {
	assignees, err := adminStringList(m["assignees"], "approval assignees")
	if err != nil {
		return ApprovalResult{}, err
	}
	return ApprovalResult{
		ID:            asString(m["id"]),
		FlowID:        asString(m["flow_id"]),
		Scope:         asString(m["scope"]),
		Status:        asString(m["status"]),
		Reason:        asString(m["reason"]),
		RequestedBy:   asString(m["requested_by"]),
		Approver:      asString(m["approver"]),
		Assignees:     assignees,
		PolicyHash:    asString(m["policy_hash"]),
		PolicyVersion: asString(m["policy_version"]),
		Raw:           m,
	}, nil
}

func governanceOverviewFromMap(m map[string]any) (GovernanceOverview, error) {
	counts, err := optionalNativeMap(m["counts"], "governance counts")
	if err != nil {
		return GovernanceOverview{}, err
	}
	overview := GovernanceOverview{Counts: counts, Limits: []LimitResult{}, Raw: m}
	approvals, err := mapList(m["approvals"], nil)
	if err != nil {
		return GovernanceOverview{}, fmt.Errorf("decode governance approvals: %w", err)
	}
	for _, item := range approvals {
		result, err := approvalResultFromMap(item)
		if err != nil {
			return GovernanceOverview{}, err
		}
		overview.Approvals = append(overview.Approvals, result)
	}
	budgets, err := mapList(m["budgets"], nil)
	if err != nil {
		return GovernanceOverview{}, fmt.Errorf("decode governance budgets: %w", err)
	}
	for _, item := range budgets {
		result, err := budgetResultFromMap(item)
		if err != nil {
			return GovernanceOverview{}, err
		}
		overview.Budgets = append(overview.Budgets, result)
	}
	limits, err := mapList(m["limits"], nil)
	if err != nil {
		return GovernanceOverview{}, fmt.Errorf("decode governance limits: %w", err)
	}
	for _, item := range limits {
		result, err := limitResultFromMap(item)
		if err != nil {
			return GovernanceOverview{}, err
		}
		overview.Limits = append(overview.Limits, result)
	}
	effects, err := mapList(m["effects"], nil)
	if err != nil {
		return GovernanceOverview{}, fmt.Errorf("decode governance effects: %w", err)
	}
	for _, item := range effects {
		result, err := effectResult(item, nil)
		if err != nil {
			return GovernanceOverview{}, err
		}
		overview.Effects = append(overview.Effects, result)
	}
	return overview, nil
}

func circuitResult(value any, err error) (CircuitBreakerStatus, error) {
	if err != nil {
		return CircuitBreakerStatus{}, err
	}
	m, err := nativeMap(value)
	if err != nil {
		return CircuitBreakerStatus{}, err
	}
	retryAfterMS, err := adminInt64(m, "retry_after_ms")
	if err != nil {
		return CircuitBreakerStatus{}, err
	}
	return CircuitBreakerStatus{
		Scope:        asString(m["scope"]),
		Status:       asString(m["status"]),
		RetryAfterMS: retryAfterMS,
		Raw:          m,
	}, nil
}

func budgetResult(value any, err error) (BudgetResult, error) {
	if err != nil {
		return BudgetResult{}, err
	}
	m, err := nativeMap(value)
	if err != nil {
		return BudgetResult{}, err
	}
	return budgetResultFromMap(m)
}

func budgetResultFromMap(m map[string]any) (BudgetResult, error) {
	limit, err := adminInt64(m, "limit")
	if err != nil {
		return BudgetResult{}, err
	}
	used, err := adminInt64(m, "used")
	if err != nil {
		return BudgetResult{}, err
	}
	remaining, err := adminInt64(m, "remaining")
	if err != nil {
		return BudgetResult{}, err
	}
	return BudgetResult{
		Scope:         asString(m["scope"]),
		Status:        asString(m["status"]),
		Limit:         limit,
		Used:          used,
		Remaining:     remaining,
		ReservationID: asString(m["reservation_id"]),
		Raw:           m,
	}, nil
}

func limitResult(value any, err error) (LimitResult, error) {
	if err != nil {
		return LimitResult{}, err
	}
	m, err := nativeMap(value)
	if err != nil {
		return LimitResult{}, err
	}
	return limitResultFromMap(m)
}

func limitResultFromMap(m map[string]any) (LimitResult, error) {
	owner := m
	if raw, exists := m["owner"]; exists && raw != nil {
		nested, err := optionalNativeMap(raw, "limit owner")
		if err != nil {
			return LimitResult{}, err
		}
		if len(nested) > 0 {
			owner = nested
		}
	}
	result, err := limitOwnerFromMap(owner)
	if err != nil {
		return LimitResult{}, err
	}
	if raw, exists := m["lease"]; exists && raw != nil {
		lease, err := optionalNativeMap(raw, "limit lease")
		if err != nil {
			return LimitResult{}, err
		}
		if len(lease) > 0 {
			parsed, err := limitLeaseFromMap(lease, 0, false)
			if err != nil {
				return LimitResult{}, err
			}
			result.Lease = &parsed
		}
	}
	result.Raw = m
	return result, nil
}

func limitOwnerFromMap(m map[string]any) (LimitResult, error) {
	limit, err := adminInt64(m, "limit")
	if err != nil {
		return LimitResult{}, err
	}
	free, err := adminInt64(m, "free")
	if err != nil {
		return LimitResult{}, err
	}
	epoch, err := adminInt64(m, "epoch")
	if err != nil {
		return LimitResult{}, err
	}
	leases, err := limitLeasesFromMap(m["leases"])
	if err != nil {
		return LimitResult{}, err
	}
	return LimitResult{
		Scope:  asString(m["scope"]),
		Limit:  limit,
		Free:   free,
		Epoch:  epoch,
		Leases: leases,
		Raw:    m,
	}, nil
}

func limitLeasesFromMap(value any) (map[int64]LimitLeaseState, error) {
	items, err := optionalNativeMap(value, "limit leases")
	if err != nil {
		return nil, err
	}
	leases := make(map[int64]LimitLeaseState, len(items))
	for key, raw := range items {
		shardID, err := strconv.ParseInt(key, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("decode limit lease shard %q: %w", key, err)
		}
		leaseMap, err := optionalNativeMap(raw, "limit lease")
		if err != nil {
			return nil, err
		}
		lease, err := limitLeaseFromMap(leaseMap, shardID, true)
		if err != nil {
			return nil, err
		}
		leases[lease.ShardID] = lease
	}
	return leases, nil
}

func limitLeaseFromMap(m map[string]any, fallbackShardID int64, hasFallback bool) (LimitLeaseState, error) {
	shardID, err := adminInt64(m, "shard_id")
	if err != nil {
		return LimitLeaseState{}, err
	}
	if shardID == 0 && hasFallback {
		shardID = fallbackShardID
	}
	epoch, err := adminInt64(m, "epoch")
	if err != nil {
		return LimitLeaseState{}, err
	}
	expiresAtMS, err := adminInt64(m, "expires_at_ms")
	if err != nil {
		return LimitLeaseState{}, err
	}
	available, err := adminInt64(m, "available")
	if err != nil {
		return LimitLeaseState{}, err
	}
	inUse, err := adminInt64(m, "in_use")
	if err != nil {
		return LimitLeaseState{}, err
	}
	pendingReclaim, err := adminInt64(m, "pending_reclaim")
	if err != nil {
		return LimitLeaseState{}, err
	}
	drainRate, err := adminFloat64(m, "drain_rate")
	if err != nil {
		return LimitLeaseState{}, err
	}
	lastSpendAtMS, err := adminInt64(m, "last_spend_at_ms")
	if err != nil {
		return LimitLeaseState{}, err
	}
	return LimitLeaseState{
		ShardID:        shardID,
		Epoch:          epoch,
		ExpiresAtMS:    expiresAtMS,
		Available:      available,
		InUse:          inUse,
		PendingReclaim: pendingReclaim,
		DrainRate:      drainRate,
		LastSpendAtMS:  lastSpendAtMS,
		Raw:            m,
	}, nil
}
