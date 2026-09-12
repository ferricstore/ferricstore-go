package ferricstore

import (
	"fmt"
	"strconv"
)

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
	strings := make([]string, 3)
	for index, field := range []string{"scope", "status", "reservation_id"} {
		value, err := adminString(m, field)
		if err != nil {
			return BudgetResult{}, err
		}
		strings[index] = value
	}

	fields := []string{
		"limit", "window_ms", "window_start_ms", "used", "remaining", "reservations_count",
		"reserved_amount", "actual_amount", "overage_amount", "reserved_at_ms", "settled_at_ms",
	}
	numbers := make([]int64, len(fields))
	for index, field := range fields {
		value, err := adminNonNegativeInt64(m, field)
		if err != nil {
			return BudgetResult{}, err
		}
		numbers[index] = value
	}

	overBudget, err := adminBool(m, "over_budget")
	if err != nil {
		return BudgetResult{}, err
	}
	usage, err := optionalNativeMap(m["usage"], "budget usage")
	if err != nil {
		return BudgetResult{}, err
	}
	if _, present := m["usage"]; !present || m["usage"] == nil {
		usage = nil
	}

	limitPresent := presentAdminField(m, "limit")
	usedPresent := presentAdminField(m, "used")
	if limitPresent && usedPresent {
		expectedRemaining := int64(0)
		if numbers[3] <= numbers[0] {
			expectedRemaining = numbers[0] - numbers[3]
		}
		if presentAdminField(m, "remaining") && numbers[4] != expectedRemaining {
			return BudgetResult{}, fmt.Errorf("budget response remaining is %d, want %d", numbers[4], expectedRemaining)
		}
		if presentAdminField(m, "over_budget") && overBudget != (numbers[3] > numbers[0]) {
			return BudgetResult{}, fmt.Errorf("budget response over_budget conflicts with limit and used")
		}
	}

	return BudgetResult{
		Scope: strings[0], Status: strings[1], ReservationID: strings[2],
		Limit: numbers[0], WindowMS: numbers[1], WindowStartMS: numbers[2], Used: numbers[3],
		Remaining: numbers[4], OverBudget: overBudget, ReservationsCount: numbers[5],
		ReservedAmount: numbers[6], ActualAmount: numbers[7], Usage: usage, OverageAmount: numbers[8],
		ReservedAtMS: numbers[9], SettledAtMS: numbers[10], Raw: m,
	}, nil
}

func adminBool(m map[string]any, field string) (bool, error) {
	value, exists := m[field]
	if !exists || value == nil {
		return false, nil
	}
	parsed, err := responseBool(value, nil)
	if err != nil {
		return false, fmt.Errorf("decode admin field %s: %w", field, err)
	}
	return parsed, nil
}

func presentAdminField(m map[string]any, field string) bool {
	value, present := m[field]
	return present && value != nil
}

func limitResult(value any, err error) (LimitResult, error) {
	if err != nil {
		return LimitResult{}, err
	}
	value, err = normalizeLimitLeaseKeys(value)
	if err != nil {
		return LimitResult{}, err
	}
	m, err := nativeMap(value)
	if err != nil {
		return LimitResult{}, err
	}
	return limitResultFromMap(m)
}

// The HTTP envelope preserves Erlang map-key types. Limit lease maps are the
// one public response shape whose keys are shard integers, while the typed Go
// decoder intentionally models those keys as decimal strings before parsing.
// Normalize only that documented field so arbitrary generic response maps
// continue to preserve their key types.
func normalizeLimitLeaseKeys(value any) (any, error) {
	root, ok := value.(map[string]any)
	if !ok {
		return value, nil
	}
	normalizedRoot := cloneStringAnyMap(root)
	if leases, exists := normalizedRoot["leases"]; exists {
		normalized, err := normalizeLimitLeaseMap(leases)
		if err != nil {
			return nil, err
		}
		normalizedRoot["leases"] = normalized
	}
	if owner, exists := normalizedRoot["owner"].(map[string]any); exists {
		normalizedOwner := cloneStringAnyMap(owner)
		if leases, present := normalizedOwner["leases"]; present {
			normalized, err := normalizeLimitLeaseMap(leases)
			if err != nil {
				return nil, err
			}
			normalizedOwner["leases"] = normalized
		}
		normalizedRoot["owner"] = normalizedOwner
	}
	return normalizedRoot, nil
}

func normalizeLimitListLeaseKeys(value any) (any, error) {
	items, ok := value.([]any)
	if !ok {
		return value, nil
	}
	normalized := make([]any, len(items))
	for index, item := range items {
		converted, err := normalizeLimitLeaseKeys(item)
		if err != nil {
			return nil, err
		}
		normalized[index] = converted
	}
	return normalized, nil
}

func normalizeGovernanceLimitLeaseKeys(value any) (any, error) {
	root, ok := value.(map[string]any)
	if !ok {
		return value, nil
	}
	normalized := cloneStringAnyMap(root)
	if limits, exists := normalized["limits"]; exists {
		converted, err := normalizeLimitListLeaseKeys(limits)
		if err != nil {
			return nil, err
		}
		normalized["limits"] = converted
	}
	return normalized, nil
}

func normalizeLimitLeaseMap(value any) (any, error) {
	leases, ok := value.(map[any]any)
	if !ok {
		return value, nil
	}
	normalized := make(map[string]any, len(leases))
	for key, lease := range leases {
		name, err := limitLeaseMapKey(key)
		if err != nil {
			return nil, err
		}
		if _, duplicate := normalized[name]; duplicate {
			return nil, fmt.Errorf("decode limit leases: duplicate shard key %q", name)
		}
		normalized[name] = lease
	}
	return normalized, nil
}

func limitLeaseMapKey(value any) (string, error) {
	switch key := value.(type) {
	case string:
		return key, nil
	case []byte:
		return string(key), nil
	case int:
		return strconv.FormatInt(int64(key), 10), nil
	case int8:
		return strconv.FormatInt(int64(key), 10), nil
	case int16:
		return strconv.FormatInt(int64(key), 10), nil
	case int32:
		return strconv.FormatInt(int64(key), 10), nil
	case int64:
		return strconv.FormatInt(key, 10), nil
	case uint:
		return strconv.FormatUint(uint64(key), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(key), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(key), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(key), 10), nil
	case uint64:
		return strconv.FormatUint(key, 10), nil
	default:
		return "", fmt.Errorf("decode limit leases: invalid shard key %T", value)
	}
}

func cloneStringAnyMap(value map[string]any) map[string]any {
	cloned := make(map[string]any, len(value))
	for key, item := range value {
		cloned[key] = item
	}
	return cloned
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
	if raw, exists := m["reservation_ids"]; exists && raw != nil {
		reservationIDs, err := adminStringList(raw, "limit reservation_ids")
		if err != nil {
			return LimitResult{}, err
		}
		seen := make(map[string]struct{}, len(reservationIDs))
		for _, id := range reservationIDs {
			if id == "" {
				return LimitResult{}, fmt.Errorf("decode limit reservation_ids: values must be non-empty")
			}
			if _, duplicate := seen[id]; duplicate {
				return LimitResult{}, fmt.Errorf("decode limit reservation_ids: duplicate value %q", id)
			}
			seen[id] = struct{}{}
		}
		result.ReservationIDs = reservationIDs
	}
	result.Raw = m
	return result, nil
}

func limitOwnerFromMap(m map[string]any) (LimitResult, error) {
	scope, err := adminString(m, "scope")
	if err != nil {
		return LimitResult{}, err
	}
	policyVersionHash, err := adminString(m, "policy_version_hash")
	if err != nil {
		return LimitResult{}, err
	}
	fields := []string{"limit", "free", "epoch", "config_version"}
	values := make([]int64, len(fields))
	for index, field := range fields {
		values[index], err = adminNonNegativeInt64(m, field)
		if err != nil {
			return LimitResult{}, err
		}
	}
	leases, err := limitLeasesFromMap(m["leases"])
	if err != nil {
		return LimitResult{}, err
	}
	return LimitResult{
		Scope: scope, Limit: values[0], Free: values[1], Epoch: values[2], ConfigVersion: values[3],
		PolicyVersionHash: policyVersionHash, Leases: leases, Raw: m,
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
		if err != nil || shardID < 0 {
			return nil, fmt.Errorf("decode limit lease shard %q: must be a non-negative integer", key)
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
	shardID, shardPresent, err := presentAdminNonNegativeInt64(m, "shard_id")
	if err != nil {
		return LimitLeaseState{}, err
	}
	if hasFallback {
		if shardPresent && shardID != fallbackShardID {
			return LimitLeaseState{}, fmt.Errorf("decode limit lease shard_id %d conflicts with map key %d", shardID, fallbackShardID)
		}
		shardID = fallbackShardID
	}
	fields := []string{"epoch", "expires_at_ms", "available", "in_use", "pending_reclaim", "last_spend_at_ms"}
	values := make([]int64, len(fields))
	for index, field := range fields {
		values[index], err = adminNonNegativeInt64(m, field)
		if err != nil {
			return LimitLeaseState{}, err
		}
	}
	drainRate, err := adminFloat64(m, "drain_rate")
	if err != nil {
		return LimitLeaseState{}, err
	}
	if drainRate < 0 {
		return LimitLeaseState{}, fmt.Errorf("decode admin field drain_rate: must be non-negative")
	}
	return LimitLeaseState{
		ShardID: shardID, Epoch: values[0], ExpiresAtMS: values[1], Available: values[2],
		InUse: values[3], PendingReclaim: values[4], DrainRate: drainRate, LastSpendAtMS: values[5], Raw: m,
	}, nil
}

func presentAdminNonNegativeInt64(m map[string]any, field string) (int64, bool, error) {
	if !presentAdminField(m, field) {
		return 0, false, nil
	}
	value, err := adminNonNegativeInt64(m, field)
	return value, true, err
}
