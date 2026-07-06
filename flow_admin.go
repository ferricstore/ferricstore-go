package ferricstore

import (
	"context"
	"errors"
	"reflect"
	"strconv"
)

type ScheduleOptions struct {
	Target         map[string]any
	Kind           string
	AtMS           *int64
	DelayMS        *int64
	StartAtMS      *int64
	EveryMS        *int64
	Cron           string
	Timezone       string
	OverlapPolicy  string
	OverlapRetryMS *int64
	MaxFires       *int64
	EndAtMS        *int64
	Overwrite      *bool
	NowMS          *int64
	ExtraOptions   map[string]any
}

type ScheduleListOptions struct {
	Kind       string
	State      string
	Timezone   string
	TargetType string
	FromMS     *int64
	ToMS       *int64
	Count      *int
	Rev        *bool
}

type ScheduleResult struct {
	ID            string
	Kind          string
	Status        string
	Target        map[string]any
	Timezone      string
	Cron          string
	OverlapPolicy string
	NextFireAtMS  int64
	LastFireAtMS  int64
	Fires         int64
	MaxFires      int64
	EndAtMS       int64
	Raw           map[string]any
}

func (c *Client) Stats(ctx context.Context, flowType string, opt ReadOptions) (map[string]any, error) {
	args := []any{"FLOW.STATS", flowType}
	appendReadOptions(&args, opt)
	return mapResult(c.Command(ctx, args...))
}

func (c *Client) CountByState(ctx context.Context, flowType, state string, opt ReadOptions) (int64, error) {
	opt.State = state
	opt.Count = nil

	stats, err := c.Stats(ctx, flowType, opt)
	if err != nil {
		return 0, err
	}
	return statsCount(stats)
}

func (c *Client) Attributes(ctx context.Context, flowType string, opt ReadOptions) ([]map[string]any, error) {
	args := []any{"FLOW.ATTRIBUTES", flowType}
	appendReadOptions(&args, opt)
	return mapList(c.Command(ctx, args...))
}

func (c *Client) AttributeValues(ctx context.Context, flowType, attribute string, opt ReadOptions) ([]map[string]any, error) {
	args := []any{"FLOW.ATTRIBUTE_VALUES", flowType, attribute}
	appendReadOptions(&args, opt)
	return mapList(c.Command(ctx, args...))
}

func (c *Client) ScheduleCreate(ctx context.Context, id string, opt ScheduleOptions) (ScheduleResult, error) {
	args := []any{"FLOW.SCHEDULE.CREATE", id}
	appendOpt(&args, "KIND", opt.Kind)
	appendInt64Ptr(&args, "AT_MS", opt.AtMS)
	appendInt64Ptr(&args, "DELAY_MS", opt.DelayMS)
	appendInt64Ptr(&args, "START_AT_MS", opt.StartAtMS)
	appendInt64Ptr(&args, "EVERY_MS", opt.EveryMS)
	appendOpt(&args, "CRON", opt.Cron)
	appendOpt(&args, "TIMEZONE", opt.Timezone)
	if opt.Target != nil {
		appendOpt(&args, "TARGET", opt.Target)
	}
	appendOpt(&args, "OVERLAP_POLICY", opt.OverlapPolicy)
	appendInt64Ptr(&args, "OVERLAP_RETRY_MS", opt.OverlapRetryMS)
	appendInt64Ptr(&args, "MAX_FIRES", opt.MaxFires)
	appendInt64Ptr(&args, "END_AT_MS", opt.EndAtMS)
	appendBoolPtr(&args, "OVERWRITE", opt.Overwrite)
	appendInt64Ptr(&args, "NOW", opt.NowMS)
	for key, value := range opt.ExtraOptions {
		args = append(args, key, value)
	}
	return scheduleResult(c.Command(ctx, args...))
}

func (c *Client) ScheduleGet(ctx context.Context, id string, nowMS *int64) (*ScheduleResult, error) {
	args := []any{"FLOW.SCHEDULE.GET", id}
	appendInt64Ptr(&args, "NOW", nowMS)
	value, err := c.Command(ctx, args...)
	if err != nil || value == nil {
		return nil, err
	}
	result, err := scheduleResult(value, nil)
	return &result, err
}

func (c *Client) ScheduleFire(ctx context.Context, id string, nowMS *int64) (ScheduleResult, error) {
	return c.scheduleStatus(ctx, "FLOW.SCHEDULE.FIRE", id, nowMS)
}

func (c *Client) SchedulePause(ctx context.Context, id string, nowMS *int64) (ScheduleResult, error) {
	return c.scheduleStatus(ctx, "FLOW.SCHEDULE.PAUSE", id, nowMS)
}

func (c *Client) ScheduleResume(ctx context.Context, id string, nowMS *int64) (ScheduleResult, error) {
	return c.scheduleStatus(ctx, "FLOW.SCHEDULE.RESUME", id, nowMS)
}

func (c *Client) ScheduleDelete(ctx context.Context, id string, nowMS *int64) (ScheduleResult, error) {
	return c.scheduleStatus(ctx, "FLOW.SCHEDULE.DELETE", id, nowMS)
}

func (c *Client) scheduleStatus(ctx context.Context, command, id string, nowMS *int64) (ScheduleResult, error) {
	args := []any{command, id}
	appendInt64Ptr(&args, "NOW", nowMS)
	value, err := c.Command(ctx, args...)
	if err != nil {
		return ScheduleResult{}, err
	}
	if isOK(value) {
		return ScheduleResult{ID: id, Status: "deleted", Raw: map[string]any{"id": id, "status": "deleted"}}, nil
	}
	return scheduleResult(value, nil)
}

func (c *Client) ScheduleFireDue(ctx context.Context, nowMS *int64, worker string, blockMS *int64, limit *int) (ScheduleResult, error) {
	args := []any{"FLOW.SCHEDULE.FIRE_DUE"}
	appendInt64Ptr(&args, "NOW", nowMS)
	appendOpt(&args, "WORKER", worker)
	appendInt64Ptr(&args, "BLOCK", blockMS)
	appendIntPtr(&args, "LIMIT", limit)
	return scheduleResult(c.Command(ctx, args...))
}

func (c *Client) ScheduleList(ctx context.Context, opt ScheduleListOptions) ([]ScheduleResult, error) {
	args := []any{"FLOW.SCHEDULE.LIST"}
	appendOpt(&args, "KIND", opt.Kind)
	appendOpt(&args, "STATE", opt.State)
	appendOpt(&args, "TIMEZONE", opt.Timezone)
	appendOpt(&args, "TARGET_TYPE", opt.TargetType)
	appendInt64Ptr(&args, "FROM_MS", opt.FromMS)
	appendInt64Ptr(&args, "TO_MS", opt.ToMS)
	appendIntPtr(&args, "COUNT", opt.Count)
	appendBoolPtr(&args, "REV", opt.Rev)
	maps, err := mapList(c.Command(ctx, args...))
	if err != nil {
		return nil, err
	}
	out := make([]ScheduleResult, 0, len(maps))
	for _, item := range maps {
		out = append(out, scheduleResultFromMap(item))
	}
	return out, nil
}

type EffectResult struct {
	ID              string
	EffectKey       string
	EffectType      string
	Status          string
	ExternalID      string
	Error           string
	Reason          string
	OperationDigest string
	IdempotencyKey  string
	Raw             map[string]any
}

type EffectReserveOptions struct {
	PartitionKey    string
	LeaseToken      string
	FencingToken    *int64
	OperationDigest string
	IdempotencyKey  string
	GovernanceScope string
	NowMS           *int64
}

type EffectStatusOptions struct {
	PartitionKey string
	LeaseToken   string
	FencingToken *int64
	ExternalID   string
	Error        string
	Reason       string
	LatencyMS    *int64
	NowMS        *int64
}

func (c *Client) EffectReserve(ctx context.Context, id, effectKey, effectType string, opt EffectReserveOptions) (EffectResult, error) {
	args := []any{"FLOW.EFFECT.RESERVE", id, "EFFECT_KEY", effectKey, "EFFECT_TYPE", effectType}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendOpt(&args, "LEASE_TOKEN", opt.LeaseToken)
	appendInt64Ptr(&args, "FENCING", opt.FencingToken)
	appendOpt(&args, "OPERATION_DIGEST", opt.OperationDigest)
	appendOpt(&args, "IDEMPOTENCY_KEY", opt.IdempotencyKey)
	appendOpt(&args, "GOVERNANCE_SCOPE", opt.GovernanceScope)
	appendInt64Ptr(&args, "NOW", opt.NowMS)
	return effectResult(c.Command(ctx, args...))
}

func (c *Client) EffectConfirm(ctx context.Context, id, effectKey string, opt EffectStatusOptions) (EffectResult, error) {
	return c.effectStatus(ctx, "FLOW.EFFECT.CONFIRM", id, effectKey, opt)
}

func (c *Client) EffectFail(ctx context.Context, id, effectKey string, opt EffectStatusOptions) (EffectResult, error) {
	return c.effectStatus(ctx, "FLOW.EFFECT.FAIL", id, effectKey, opt)
}

func (c *Client) EffectCompensate(ctx context.Context, id, effectKey string, opt EffectStatusOptions) (EffectResult, error) {
	return c.effectStatus(ctx, "FLOW.EFFECT.COMPENSATE", id, effectKey, opt)
}

func (c *Client) EffectGet(ctx context.Context, id, effectKey, partitionKey string) (*EffectResult, error) {
	args := []any{"FLOW.EFFECT.GET", id, "EFFECT_KEY", effectKey}
	appendOpt(&args, "PARTITION", partitionKey)
	value, err := c.Command(ctx, args...)
	if err != nil || value == nil {
		return nil, err
	}
	result, err := effectResult(value, nil)
	return &result, err
}

func (c *Client) effectStatus(ctx context.Context, command, id, effectKey string, opt EffectStatusOptions) (EffectResult, error) {
	args := []any{command, id, "EFFECT_KEY", effectKey}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendOpt(&args, "LEASE_TOKEN", opt.LeaseToken)
	appendInt64Ptr(&args, "FENCING", opt.FencingToken)
	appendOpt(&args, "EXTERNAL_ID", opt.ExternalID)
	appendOpt(&args, "ERROR", opt.Error)
	appendOpt(&args, "REASON", opt.Reason)
	appendInt64Ptr(&args, "LATENCY_MS", opt.LatencyMS)
	appendInt64Ptr(&args, "NOW", opt.NowMS)
	return effectResult(c.Command(ctx, args...))
}

type ApprovalResult struct {
	ID            string
	FlowID        string
	Scope         string
	Status        string
	Reason        string
	RequestedBy   string
	Approver      string
	Assignees     []string
	PolicyHash    string
	PolicyVersion string
	Raw           map[string]any
}

type ApprovalRequestOptions struct {
	FlowID        string
	Scope         string
	Reason        string
	RequestedBy   string
	Assignees     []string
	PolicyHash    string
	PolicyVersion any
	TimeoutMS     *int64
	ExpiresAtMS   *int64
	NowMS         *int64
}

type ApprovalListOptions struct {
	Status       string
	Scope        string
	PartitionKey string
	FlowID       string
	Limit        *int
}

func (c *Client) ApprovalRequest(ctx context.Context, id string, opt ApprovalRequestOptions) (ApprovalResult, error) {
	args := []any{"FLOW.APPROVAL.REQUEST", id}
	appendOpt(&args, "FLOW_ID", opt.FlowID)
	appendOpt(&args, "SCOPE", opt.Scope)
	appendOpt(&args, "REASON", opt.Reason)
	appendOpt(&args, "REQUESTED_BY", opt.RequestedBy)
	if opt.Assignees != nil {
		args = append(args, "ASSIGNEES", opt.Assignees)
	}
	appendOpt(&args, "POLICY_HASH", opt.PolicyHash)
	appendOpt(&args, "POLICY_VERSION", opt.PolicyVersion)
	appendInt64Ptr(&args, "TIMEOUT_MS", opt.TimeoutMS)
	appendInt64Ptr(&args, "EXPIRES_AT_MS", opt.ExpiresAtMS)
	appendInt64Ptr(&args, "NOW", opt.NowMS)
	return approvalResult(c.Command(ctx, args...))
}

func (c *Client) ApprovalApprove(ctx context.Context, id, approver, reason string, nowMS *int64) (ApprovalResult, error) {
	return c.approvalStatus(ctx, "FLOW.APPROVAL.APPROVE", id, approver, reason, nowMS)
}

func (c *Client) ApprovalReject(ctx context.Context, id, approver, reason string, nowMS *int64) (ApprovalResult, error) {
	return c.approvalStatus(ctx, "FLOW.APPROVAL.REJECT", id, approver, reason, nowMS)
}

func (c *Client) approvalStatus(ctx context.Context, command, id, approver, reason string, nowMS *int64) (ApprovalResult, error) {
	args := []any{command, id, "APPROVER", approver}
	appendOpt(&args, "REASON", reason)
	appendInt64Ptr(&args, "NOW", nowMS)
	return approvalResult(c.Command(ctx, args...))
}

func (c *Client) ApprovalGet(ctx context.Context, id string) (*ApprovalResult, error) {
	value, err := c.Command(ctx, "FLOW.APPROVAL.GET", id)
	if err != nil || value == nil {
		return nil, err
	}
	result, err := approvalResult(value, nil)
	return &result, err
}

func (c *Client) ApprovalList(ctx context.Context, opt ApprovalListOptions) ([]ApprovalResult, error) {
	args := []any{"FLOW.APPROVAL.LIST"}
	appendOpt(&args, "STATUS", opt.Status)
	appendOpt(&args, "SCOPE", opt.Scope)
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendOpt(&args, "FLOW_ID", opt.FlowID)
	appendIntPtr(&args, "LIMIT", opt.Limit)
	maps, err := mapList(c.Command(ctx, args...))
	if err != nil {
		return nil, err
	}
	out := make([]ApprovalResult, 0, len(maps))
	for _, item := range maps {
		out = append(out, approvalResultFromMap(item))
	}
	return out, nil
}

type GovernanceOverview struct {
	Counts    map[string]any
	Approvals []ApprovalResult
	Budgets   []BudgetResult
	Limits    []LimitResult
	Effects   []EffectResult
	Raw       map[string]any
}

func (c *Client) GovernanceOverview(ctx context.Context, opt ApprovalListOptions) (GovernanceOverview, error) {
	args := []any{"FLOW.GOVERNANCE.OVERVIEW"}
	appendOpt(&args, "SCOPE", opt.Scope)
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendOpt(&args, "STATUS", opt.Status)
	appendOpt(&args, "FLOW_ID", opt.FlowID)
	appendIntPtr(&args, "LIMIT", opt.Limit)
	m, err := mapResult(c.Command(ctx, args...))
	if err != nil {
		return GovernanceOverview{}, err
	}
	return governanceOverviewFromMap(m), nil
}

type CircuitBreakerStatus struct {
	Scope        string
	Status       string
	RetryAfterMS int64
	Raw          map[string]any
}

func (c *Client) CircuitOpen(ctx context.Context, scope string, openMS, failureThreshold, nowMS *int64) (CircuitBreakerStatus, error) {
	args := []any{"FLOW.CIRCUIT.OPEN", scope}
	appendInt64Ptr(&args, "OPEN_MS", openMS)
	appendInt64Ptr(&args, "FAILURE_THRESHOLD", failureThreshold)
	appendInt64Ptr(&args, "NOW", nowMS)
	return circuitResult(c.Command(ctx, args...))
}

func (c *Client) CircuitClose(ctx context.Context, scope string, nowMS *int64) (CircuitBreakerStatus, error) {
	args := []any{"FLOW.CIRCUIT.CLOSE", scope}
	appendInt64Ptr(&args, "NOW", nowMS)
	return circuitResult(c.Command(ctx, args...))
}

func (c *Client) CircuitGet(ctx context.Context, scope string) (*CircuitBreakerStatus, error) {
	value, err := c.Command(ctx, "FLOW.CIRCUIT.GET", scope)
	if err != nil || value == nil {
		return nil, err
	}
	result, err := circuitResult(value, nil)
	return &result, err
}

type BudgetResult struct {
	Scope         string
	Status        string
	Limit         int64
	Used          int64
	Remaining     int64
	ReservationID string
	Raw           map[string]any
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
	Scope  string
	Limit  int64
	Free   int64
	Epoch  int64
	Leases map[int64]LimitLeaseState
	Lease  *LimitLeaseState
	Raw    map[string]any
}

func (c *Client) BudgetReserve(ctx context.Context, scope string, amount int64, limit, windowMS *int64, reservationID string, nowMS *int64) (BudgetResult, error) {
	args := []any{"FLOW.BUDGET.RESERVE", scope, "AMOUNT", amount}
	appendInt64Ptr(&args, "LIMIT", limit)
	appendInt64Ptr(&args, "WINDOW_MS", windowMS)
	appendOpt(&args, "RESERVATION_ID", reservationID)
	appendInt64Ptr(&args, "NOW", nowMS)
	return budgetResult(c.Command(ctx, args...))
}

func (c *Client) BudgetCommit(ctx context.Context, scope, reservationID string, actualAmount int64, usage map[string]any, nowMS *int64) (BudgetResult, error) {
	args := []any{"FLOW.BUDGET.COMMIT", scope, "RESERVATION_ID", reservationID, "ACTUAL_AMOUNT", actualAmount}
	if usage != nil {
		args = append(args, "USAGE", usage)
	}
	appendInt64Ptr(&args, "NOW", nowMS)
	return budgetResult(c.Command(ctx, args...))
}

func (c *Client) BudgetRelease(ctx context.Context, scope, reservationID string, nowMS *int64) (BudgetResult, error) {
	args := []any{"FLOW.BUDGET.RELEASE", scope, "RESERVATION_ID", reservationID}
	appendInt64Ptr(&args, "NOW", nowMS)
	return budgetResult(c.Command(ctx, args...))
}

func (c *Client) BudgetGet(ctx context.Context, scope string) (*BudgetResult, error) {
	value, err := c.Command(ctx, "FLOW.BUDGET.GET", scope)
	if err != nil || value == nil {
		return nil, err
	}
	result, err := budgetResult(value, nil)
	return &result, err
}

func (c *Client) BudgetList(ctx context.Context, scope, partitionKey string, limit *int) ([]BudgetResult, error) {
	args := []any{"FLOW.BUDGET.LIST"}
	appendOpt(&args, "SCOPE", scope)
	appendOpt(&args, "PARTITION", partitionKey)
	appendIntPtr(&args, "LIMIT", limit)
	maps, err := mapList(c.Command(ctx, args...))
	if err != nil {
		return nil, err
	}
	out := make([]BudgetResult, 0, len(maps))
	for _, item := range maps {
		out = append(out, budgetResultFromMap(item))
	}
	return out, nil
}

func (c *Client) LimitLease(ctx context.Context, scope string, shardID, amount, ttlMS int64, limit, nowMS *int64) (LimitResult, error) {
	args := []any{"FLOW.LIMIT.LEASE", scope, "SHARD_ID", shardID, "AMOUNT", amount, "TTL_MS", ttlMS}
	appendInt64Ptr(&args, "LIMIT", limit)
	appendInt64Ptr(&args, "NOW", nowMS)
	return limitResult(c.Command(ctx, args...))
}

func (c *Client) LimitSpend(ctx context.Context, scope string, shardID, amount int64, nowMS *int64) (LimitResult, error) {
	args := []any{"FLOW.LIMIT.SPEND", scope, "SHARD_ID", shardID, "AMOUNT", amount}
	appendInt64Ptr(&args, "NOW", nowMS)
	return limitResult(c.Command(ctx, args...))
}

func (c *Client) LimitRelease(ctx context.Context, scope string, shardID, amount int64) (LimitResult, error) {
	return limitResult(c.Command(ctx, "FLOW.LIMIT.RELEASE", scope, "SHARD_ID", shardID, "AMOUNT", amount))
}

func (c *Client) LimitGet(ctx context.Context, scope string, nowMS *int64) (*LimitResult, error) {
	args := []any{"FLOW.LIMIT.GET", scope}
	appendInt64Ptr(&args, "NOW", nowMS)
	value, err := c.Command(ctx, args...)
	if err != nil || value == nil {
		return nil, err
	}
	result, err := limitResult(value, nil)
	return &result, err
}

func (c *Client) LimitList(ctx context.Context, scope, partitionKey string, limit *int, nowMS *int64) ([]LimitResult, error) {
	args := []any{"FLOW.LIMIT.LIST"}
	appendOpt(&args, "SCOPE", scope)
	appendOpt(&args, "PARTITION", partitionKey)
	appendIntPtr(&args, "LIMIT", limit)
	appendInt64Ptr(&args, "NOW", nowMS)
	maps, err := mapList(c.Command(ctx, args...))
	if err != nil {
		return nil, err
	}
	out := make([]LimitResult, 0, len(maps))
	for _, item := range maps {
		out = append(out, limitResultFromMap(item))
	}
	return out, nil
}

func (c *Client) GovernanceLedger(ctx context.Context, id string, opt ReadOptions) ([]map[string]any, error) {
	args := []any{"FLOW.GOVERNANCE.LEDGER", id}
	appendReadOptions(&args, opt)
	return mapList(c.Command(ctx, args...))
}

func appendAttributes(args *[]any, attributes, attributesMerge map[string]any, attributesDelete []string) {
	for name, value := range attributes {
		*args = append(*args, "ATTRIBUTE", name, value)
	}
	for name, value := range attributesMerge {
		*args = append(*args, "ATTRIBUTE_MERGE", name, value)
	}
	for _, name := range attributesDelete {
		*args = append(*args, "ATTRIBUTE_DELETE", name)
	}
}

func appendStateMeta(args *[]any, stateMeta map[string]any) {
	for name, value := range stateMeta {
		*args = append(*args, "STATE_META", name, value)
	}
}

func appendSearchStateMeta(args *[]any, stateMeta map[string]map[string]any) {
	for state, meta := range stateMeta {
		for name, value := range meta {
			*args = append(*args, "STATE_META", state, name, value)
		}
	}
}

func sharedCreateManyAttributes(items []CreateItem, attributes map[string]any) (map[string]any, error) {
	var first map[string]any
	for _, item := range items {
		if len(item.Attributes) == 0 {
			continue
		}
		if first == nil {
			first = item.Attributes
			continue
		}
		if !reflect.DeepEqual(first, item.Attributes) {
			return nil, errors.New("create_many supports shared attributes only; use CreateManyOptions.Attributes or separate Create calls for per-item attributes")
		}
	}
	if first == nil {
		return attributes, nil
	}
	if attributes != nil && !reflect.DeepEqual(attributes, first) {
		return nil, errors.New("create_many item attributes must match shared attributes when both are provided")
	}
	return first, nil
}

func sharedCreateManyStateMeta(items []CreateItem, stateMeta map[string]any) (map[string]any, error) {
	var first map[string]any
	for _, item := range items {
		if len(item.StateMeta) == 0 {
			continue
		}
		if first == nil {
			first = item.StateMeta
			continue
		}
		if !reflect.DeepEqual(first, item.StateMeta) {
			return nil, errors.New("create_many supports shared state_meta only; use CreateManyOptions.StateMeta or separate Create calls for per-item state_meta")
		}
	}
	if first == nil {
		return stateMeta, nil
	}
	if stateMeta != nil && !reflect.DeepEqual(stateMeta, first) {
		return nil, errors.New("create_many item state_meta must match shared state_meta when both are provided")
	}
	return first, nil
}

func boolDefault(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

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

func scheduleResult(value any, err error) (ScheduleResult, error) {
	if err != nil {
		return ScheduleResult{}, err
	}
	m, err := nativeMap(value)
	if err != nil {
		return ScheduleResult{}, err
	}
	return scheduleResultFromMap(m), nil
}

func scheduleResultFromMap(m map[string]any) ScheduleResult {
	return ScheduleResult{
		ID:            asString(m["id"]),
		Kind:          asString(m["kind"]),
		Status:        asString(m["status"]),
		Target:        stringObjectMap(m["target"]),
		Timezone:      asString(m["timezone"]),
		Cron:          asString(m["cron"]),
		OverlapPolicy: asString(m["overlap_policy"]),
		NextFireAtMS:  asInt64(m["next_fire_at_ms"]),
		LastFireAtMS:  asInt64(m["last_fire_at_ms"]),
		Fires:         asInt64(m["fires"]),
		MaxFires:      asInt64(m["max_fires"]),
		EndAtMS:       asInt64(m["end_at_ms"]),
		Raw:           m,
	}
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
	return approvalResultFromMap(m), nil
}

func approvalResultFromMap(m map[string]any) ApprovalResult {
	assignees := []string{}
	if raw, ok := m["assignees"].([]any); ok {
		for _, item := range raw {
			assignees = append(assignees, asString(item))
		}
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
	}
}

func governanceOverviewFromMap(m map[string]any) GovernanceOverview {
	overview := GovernanceOverview{Counts: stringObjectMap(m["counts"]), Limits: []LimitResult{}, Raw: m}
	if approvals, _ := mapList(m["approvals"], nil); approvals != nil {
		for _, item := range approvals {
			overview.Approvals = append(overview.Approvals, approvalResultFromMap(item))
		}
	}
	if budgets, _ := mapList(m["budgets"], nil); budgets != nil {
		for _, item := range budgets {
			overview.Budgets = append(overview.Budgets, budgetResultFromMap(item))
		}
	}
	if limits, _ := mapList(m["limits"], nil); limits != nil {
		for _, item := range limits {
			overview.Limits = append(overview.Limits, limitResultFromMap(item))
		}
	}
	if effects, _ := mapList(m["effects"], nil); effects != nil {
		for _, item := range effects {
			result, _ := effectResult(item, nil)
			overview.Effects = append(overview.Effects, result)
		}
	}
	return overview
}

func circuitResult(value any, err error) (CircuitBreakerStatus, error) {
	if err != nil {
		return CircuitBreakerStatus{}, err
	}
	m, err := nativeMap(value)
	if err != nil {
		return CircuitBreakerStatus{}, err
	}
	return CircuitBreakerStatus{
		Scope:        asString(m["scope"]),
		Status:       asString(m["status"]),
		RetryAfterMS: asInt64(m["retry_after_ms"]),
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
	return budgetResultFromMap(m), nil
}

func budgetResultFromMap(m map[string]any) BudgetResult {
	return BudgetResult{
		Scope:         asString(m["scope"]),
		Status:        asString(m["status"]),
		Limit:         asInt64(m["limit"]),
		Used:          asInt64(m["used"]),
		Remaining:     asInt64(m["remaining"]),
		ReservationID: asString(m["reservation_id"]),
		Raw:           m,
	}
}

func limitResult(value any, err error) (LimitResult, error) {
	if err != nil {
		return LimitResult{}, err
	}
	m, err := nativeMap(value)
	if err != nil {
		return LimitResult{}, err
	}
	return limitResultFromMap(m), nil
}

func limitResultFromMap(m map[string]any) LimitResult {
	owner := m
	if nested := stringObjectMap(m["owner"]); len(nested) > 0 {
		owner = nested
	}
	result := limitOwnerFromMap(owner)
	if lease := stringObjectMap(m["lease"]); len(lease) > 0 {
		parsed := limitLeaseFromMap(lease, 0, false)
		result.Lease = &parsed
	}
	result.Raw = m
	return result
}

func limitOwnerFromMap(m map[string]any) LimitResult {
	return LimitResult{
		Scope:  asString(m["scope"]),
		Limit:  asInt64(m["limit"]),
		Free:   asInt64(m["free"]),
		Epoch:  asInt64(m["epoch"]),
		Leases: limitLeasesFromMap(m["leases"]),
		Raw:    m,
	}
}

func limitLeasesFromMap(value any) map[int64]LimitLeaseState {
	items := stringObjectMap(value)
	leases := make(map[int64]LimitLeaseState, len(items))
	for key, raw := range items {
		shardID, err := strconv.ParseInt(key, 10, 64)
		hasShardID := err == nil
		if !hasShardID {
			shardID = 0
		}
		lease := limitLeaseFromMap(stringObjectMap(raw), shardID, hasShardID)
		leases[lease.ShardID] = lease
	}
	return leases
}

func limitLeaseFromMap(m map[string]any, fallbackShardID int64, hasFallback bool) LimitLeaseState {
	shardID := asInt64(m["shard_id"])
	if shardID == 0 && hasFallback {
		shardID = fallbackShardID
	}
	return LimitLeaseState{
		ShardID:        shardID,
		Epoch:          asInt64(m["epoch"]),
		ExpiresAtMS:    asInt64(m["expires_at_ms"]),
		Available:      asInt64(m["available"]),
		InUse:          asInt64(m["in_use"]),
		PendingReclaim: asInt64(m["pending_reclaim"]),
		DrainRate:      asFloat64(m["drain_rate"]),
		LastSpendAtMS:  asInt64(m["last_spend_at_ms"]),
		Raw:            m,
	}
}
