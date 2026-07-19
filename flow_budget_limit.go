package ferricstore

import (
	"context"
)

type BudgetResult struct {
	Scope             string
	Status            string
	Limit             int64
	WindowMS          int64
	WindowStartMS     int64
	Used              int64
	Remaining         int64
	OverBudget        bool
	ReservationsCount int64
	ReservationID     string
	ReservedAmount    int64
	ActualAmount      int64
	Usage             map[string]any
	OverageAmount     int64
	ReservedAtMS      int64
	SettledAtMS       int64
	Raw               map[string]any
}

type LimitLeaseState struct {
	ShardID        int64
	Epoch          int64
	ExpiresAtMS    int64
	Available      int64
	InUse          int64
	PendingReclaim int64
	DrainRate      float64
	LastSpendAtMS  int64
	Raw            map[string]any
}

type LimitResult struct {
	Scope             string
	Limit             int64
	Free              int64
	Epoch             int64
	ConfigVersion     int64
	PolicyVersionHash string
	Leases            map[int64]LimitLeaseState
	Lease             *LimitLeaseState
	ReservationIDs    []string
	Raw               map[string]any
}

type LimitReleaseOptions struct {
	ShardID        int64
	ReservationIDs []string
	NowMS          *int64
	DeadlineMS     *int64
}

func (c *Client) BudgetReserve(ctx context.Context, scope string, amount int64, limit, windowMS *int64, reservationID string, nowMS *int64) (BudgetResult, error) {
	if err := validateBudgetReserve(scope, amount, limit, windowMS, reservationID, nowMS); err != nil {
		return BudgetResult{}, err
	}
	args := []any{"FLOW.BUDGET.RESERVE", scope, "AMOUNT", amount}
	appendInt64Ptr(&args, "LIMIT", limit)
	appendInt64Ptr(&args, "WINDOW_MS", windowMS)
	appendOpt(&args, "RESERVATION_ID", reservationID)
	appendInt64Ptr(&args, "NOW", nowMS)
	return budgetResult(c.typedReply(ctx, args...))
}

func (c *Client) BudgetCommit(ctx context.Context, scope, reservationID string, actualAmount int64, usage map[string]any, nowMS *int64) (BudgetResult, error) {
	if err := validateBudgetSettlement(scope, reservationID, &actualAmount, nowMS); err != nil {
		return BudgetResult{}, err
	}
	if err := validateGovernanceUsage(usage); err != nil {
		return BudgetResult{}, err
	}
	args := []any{"FLOW.BUDGET.COMMIT", scope, "RESERVATION_ID", reservationID, "ACTUAL_AMOUNT", actualAmount}
	if usage != nil {
		args = append(args, "USAGE", usage)
	}
	appendInt64Ptr(&args, "NOW", nowMS)
	return budgetResult(c.typedReply(ctx, args...))
}

func (c *Client) BudgetRelease(ctx context.Context, scope, reservationID string, nowMS *int64) (BudgetResult, error) {
	if err := validateBudgetSettlement(scope, reservationID, nil, nowMS); err != nil {
		return BudgetResult{}, err
	}
	args := []any{"FLOW.BUDGET.RELEASE", scope, "RESERVATION_ID", reservationID}
	appendInt64Ptr(&args, "NOW", nowMS)
	return budgetResult(c.typedReply(ctx, args...))
}

func (c *Client) BudgetGet(ctx context.Context, scope string) (*BudgetResult, error) {
	if err := validateGovernanceScope("scope", scope); err != nil {
		return nil, err
	}
	value, err := c.typedReply(ctx, "FLOW.BUDGET.GET", scope)
	if err != nil || value == nil {
		return nil, err
	}
	result, err := budgetResult(value, nil)
	return &result, err
}

func (c *Client) BudgetList(ctx context.Context, scope, partitionKey string, limit *int) ([]BudgetResult, error) {
	if err := validateAdminList(scope, partitionKey, limit); err != nil {
		return nil, err
	}
	args := []any{"FLOW.BUDGET.LIST"}
	appendGovernanceScopeFilter(&args, scope, partitionKey)
	appendIntPtr(&args, "LIMIT", limit)
	value, err := c.typedReply(ctx, args...)
	maps, err := mapListWithLimit(
		"FLOW.BUDGET.LIST", limit,
		defaultFlowResponseLimitV080, maxClampedFlowListItemsV080,
		value, err,
	)
	if err != nil {
		return nil, err
	}
	out := make([]BudgetResult, 0, len(maps))
	for _, item := range maps {
		result, err := budgetResultFromMap(item)
		if err != nil {
			return nil, err
		}
		out = append(out, result)
	}
	return out, nil
}

func (c *Client) LimitLease(ctx context.Context, scope string, shardID, amount, ttlMS int64, limit, nowMS *int64) (LimitResult, error) {
	if err := validateLimitLease(scope, shardID, amount, ttlMS, limit, nowMS); err != nil {
		return LimitResult{}, err
	}
	args := []any{"FLOW.LIMIT.LEASE", scope, "SHARD_ID", shardID, "AMOUNT", amount, "TTL_MS", ttlMS}
	appendInt64Ptr(&args, "LIMIT", limit)
	appendInt64Ptr(&args, "NOW", nowMS)
	return limitResult(c.typedReply(ctx, args...))
}

func (c *Client) LimitSpend(ctx context.Context, scope string, shardID, amount int64, nowMS *int64) (LimitResult, error) {
	if err := validateLimitMutation(scope, shardID, amount, nowMS); err != nil {
		return LimitResult{}, err
	}
	args := []any{"FLOW.LIMIT.SPEND", scope, "SHARD_ID", shardID, "AMOUNT", amount}
	appendInt64Ptr(&args, "NOW", nowMS)
	return limitResult(c.typedReply(ctx, args...))
}

// LimitRelease releases exact reservation IDs returned by LimitSpend.
func (c *Client) LimitRelease(ctx context.Context, scope string, opt LimitReleaseOptions) (LimitResult, error) {
	reservationIDs, err := validateLimitReleaseOptions(scope, opt)
	if err != nil {
		return LimitResult{}, err
	}
	args := []any{"FLOW.LIMIT.RELEASE", scope, "SHARD_ID", opt.ShardID}
	args = append(args, "RESERVATION_IDS", reservationIDs)
	appendInt64Ptr(&args, "NOW", opt.NowMS)
	appendInt64Ptr(&args, "DEADLINE_MS", opt.DeadlineMS)
	return limitResult(c.typedReply(ctx, args...))
}

func (c *Client) LimitGet(ctx context.Context, scope string, nowMS *int64) (*LimitResult, error) {
	if err := validateLimitGet(scope, nowMS); err != nil {
		return nil, err
	}
	args := []any{"FLOW.LIMIT.GET", scope}
	appendInt64Ptr(&args, "NOW", nowMS)
	value, err := c.typedReply(ctx, args...)
	if err != nil || value == nil {
		return nil, err
	}
	result, err := limitResult(value, nil)
	return &result, err
}

func (c *Client) LimitList(ctx context.Context, scope, partitionKey string, limit *int, nowMS *int64) ([]LimitResult, error) {
	if err := validateLimitList(scope, partitionKey, limit, nowMS); err != nil {
		return nil, err
	}
	args := []any{"FLOW.LIMIT.LIST"}
	appendGovernanceScopeFilter(&args, scope, partitionKey)
	appendIntPtr(&args, "LIMIT", limit)
	appendInt64Ptr(&args, "NOW", nowMS)
	value, err := c.typedReply(ctx, args...)
	maps, err := mapListWithLimit(
		"FLOW.LIMIT.LIST", limit,
		defaultFlowResponseLimitV080, maxClampedFlowListItemsV080,
		value, err,
	)
	if err != nil {
		return nil, err
	}
	out := make([]LimitResult, 0, len(maps))
	for _, item := range maps {
		result, err := limitResultFromMap(item)
		if err != nil {
			return nil, err
		}
		out = append(out, result)
	}
	return out, nil
}
