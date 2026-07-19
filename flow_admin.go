package ferricstore

import "context"

type EffectResult struct {
	ID              string
	FlowID          string
	PartitionKey    string
	FlowType        string
	State           string
	EffectKey       string
	EffectType      string
	Status          string
	Decision        string
	Scope           string
	ExternalID      string
	Error           string
	Reason          string
	OperationDigest string
	IdempotencyKey  string
	PolicyHash      string
	PolicyVersion   string
	LatencyMS       int64
	CreatedAtMS     int64
	UpdatedAtMS     int64
	ReservedAtMS    int64
	ConfirmedAtMS   int64
	FailedAtMS      int64
	CompensatedAtMS int64
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
	if err := validateEffectReserve(id, effectKey, effectType, opt); err != nil {
		return EffectResult{}, err
	}
	args := []any{"FLOW.EFFECT.RESERVE", id, "EFFECT_KEY", effectKey, "EFFECT_TYPE", effectType}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendOpt(&args, "LEASE_TOKEN", opt.LeaseToken)
	appendInt64Ptr(&args, "FENCING", opt.FencingToken)
	appendOpt(&args, "OPERATION_DIGEST", opt.OperationDigest)
	appendOpt(&args, "IDEMPOTENCY_KEY", opt.IdempotencyKey)
	appendOpt(&args, "GOVERNANCE_SCOPE", opt.GovernanceScope)
	appendInt64Ptr(&args, "NOW", opt.NowMS)
	return effectResult(c.typedReply(ctx, args...))
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
	if err := validateEffectGet(id, effectKey, partitionKey); err != nil {
		return nil, err
	}
	args := []any{"FLOW.EFFECT.GET", id, "EFFECT_KEY", effectKey}
	appendOpt(&args, "PARTITION", partitionKey)
	value, err := c.typedReply(ctx, args...)
	if err != nil || value == nil {
		return nil, err
	}
	result, err := effectResult(value, nil)
	return &result, err
}

func (c *Client) effectStatus(ctx context.Context, command, id, effectKey string, opt EffectStatusOptions) (EffectResult, error) {
	if err := validateEffectStatus(id, effectKey, opt); err != nil {
		return EffectResult{}, err
	}
	args := []any{command, id, "EFFECT_KEY", effectKey}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendOpt(&args, "LEASE_TOKEN", opt.LeaseToken)
	appendInt64Ptr(&args, "FENCING", opt.FencingToken)
	appendOpt(&args, "EXTERNAL_ID", opt.ExternalID)
	appendOpt(&args, "ERROR", opt.Error)
	appendOpt(&args, "REASON", opt.Reason)
	appendInt64Ptr(&args, "LATENCY_MS", opt.LatencyMS)
	appendInt64Ptr(&args, "NOW", opt.NowMS)
	return effectResult(c.typedReply(ctx, args...))
}

type ApprovalResult struct {
	ID             string
	FlowID         string
	Scope          string
	Status         string
	Reason         string
	RequestedBy    string
	Approver       string
	DecisionReason string
	Assignees      []string
	PolicyHash     string
	PolicyVersion  string
	RequestedAtMS  int64
	DecidedAtMS    int64
	ExpiresAtMS    int64
	Raw            map[string]any
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
	if err := validateApprovalRequest(id, opt); err != nil {
		return ApprovalResult{}, err
	}
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
	return approvalResult(c.typedReply(ctx, args...))
}

func (c *Client) ApprovalApprove(ctx context.Context, id, approver, reason string, nowMS *int64) (ApprovalResult, error) {
	return c.approvalStatus(ctx, "FLOW.APPROVAL.APPROVE", id, approver, reason, nowMS)
}

func (c *Client) ApprovalReject(ctx context.Context, id, approver, reason string, nowMS *int64) (ApprovalResult, error) {
	return c.approvalStatus(ctx, "FLOW.APPROVAL.REJECT", id, approver, reason, nowMS)
}

func (c *Client) approvalStatus(ctx context.Context, command, id, approver, reason string, nowMS *int64) (ApprovalResult, error) {
	if err := validateApprovalDecision(id, approver, reason, nowMS); err != nil {
		return ApprovalResult{}, err
	}
	args := []any{command, id, "APPROVER", approver}
	appendOpt(&args, "REASON", reason)
	appendInt64Ptr(&args, "NOW", nowMS)
	return approvalResult(c.typedReply(ctx, args...))
}

func (c *Client) ApprovalGet(ctx context.Context, id string) (*ApprovalResult, error) {
	if err := validateGovernanceRequiredText("id", id, maxGovernanceDimensionBytesV080); err != nil {
		return nil, err
	}
	value, err := c.typedReply(ctx, "FLOW.APPROVAL.GET", id)
	if err != nil || value == nil {
		return nil, err
	}
	result, err := approvalResult(value, nil)
	return &result, err
}

func (c *Client) ApprovalList(ctx context.Context, opt ApprovalListOptions) ([]ApprovalResult, error) {
	if err := validateApprovalList(opt); err != nil {
		return nil, err
	}
	args := []any{"FLOW.APPROVAL.LIST"}
	appendOpt(&args, "STATUS", canonicalAdminEnum(opt.Status))
	appendGovernanceScopeFilter(&args, opt.Scope, opt.PartitionKey)
	appendOpt(&args, "FLOW_ID", opt.FlowID)
	appendIntPtr(&args, "LIMIT", opt.Limit)
	value, err := c.typedReply(ctx, args...)
	maps, err := mapListWithLimit(
		"FLOW.APPROVAL.LIST", opt.Limit,
		defaultFlowResponseLimitV080, maxClampedFlowListItemsV080,
		value, err,
	)
	if err != nil {
		return nil, err
	}
	out := make([]ApprovalResult, 0, len(maps))
	for _, item := range maps {
		result, err := approvalResultFromMap(item)
		if err != nil {
			return nil, err
		}
		out = append(out, result)
	}
	return out, nil
}

type GovernanceOverview struct {
	Counts    map[string]any
	Approvals []ApprovalResult
	Budgets   []BudgetResult
	Limits    []LimitResult
	Circuits  []CircuitBreakerStatus
	Effects   []EffectResult
	Raw       map[string]any
}

func (c *Client) GovernanceOverview(ctx context.Context, opt ApprovalListOptions) (GovernanceOverview, error) {
	if err := validateApprovalList(opt); err != nil {
		return GovernanceOverview{}, err
	}
	args := []any{"FLOW.GOVERNANCE.OVERVIEW"}
	appendGovernanceScopeFilter(&args, opt.Scope, opt.PartitionKey)
	appendOpt(&args, "STATUS", canonicalAdminEnum(opt.Status))
	appendOpt(&args, "FLOW_ID", opt.FlowID)
	appendIntPtr(&args, "LIMIT", opt.Limit)
	m, err := mapResult(c.typedReply(ctx, args...))
	if err != nil {
		return GovernanceOverview{}, err
	}
	for _, collection := range []string{"approvals", "budgets", "limits", "circuits", "effects"} {
		if err := validateDefaultedFlowResponseLimit(
			"FLOW.GOVERNANCE.OVERVIEW "+collection,
			m[collection],
			opt.Limit,
			defaultFlowResponseLimitV080,
			maxClampedFlowListItemsV080,
		); err != nil {
			return GovernanceOverview{}, err
		}
	}
	return governanceOverviewFromMap(m)
}

type CircuitBreakerStatus struct {
	Scope                    string
	Status                   string
	FailureThreshold         int64
	OpenMS                   int64
	OpenedAtMS               int64
	Failures                 int64
	FailureCount             int64
	WindowMS                 int64
	MinCalls                 int64
	FailureRatePct           int64
	LatencyThresholdMS       int64
	ErrorClasses             []string
	HalfOpenMaxProbes        int64
	HalfOpenSuccessThreshold int64
	HalfOpenInFlight         int64
	HalfOpenSuccesses        int64
	HalfOpenStartedAtMS      int64
	LastFailureMS            int64
	LastSuccessMS            int64
	UpdatedAtMS              int64
	Events                   []map[string]any
	EventCount               int64
	RetryAfterMS             int64
	Raw                      map[string]any
}

type CircuitOpenOptions struct {
	OpenMS                   *int64
	FailureThreshold         *int64
	WindowMS                 *int64
	MinCalls                 *int64
	FailureRatePct           *int64
	LatencyThresholdMS       *int64
	ErrorClasses             []string
	HalfOpenMaxProbes        *int64
	HalfOpenSuccessThreshold *int64
	NowMS                    *int64
	DeadlineMS               *int64
}

func (c *Client) CircuitOpen(ctx context.Context, scope string, openMS, failureThreshold, nowMS *int64) (CircuitBreakerStatus, error) {
	return c.CircuitOpenWithOptions(ctx, scope, CircuitOpenOptions{
		OpenMS: openMS, FailureThreshold: failureThreshold, NowMS: nowMS,
	})
}

// CircuitOpenWithOptions opens or reconfigures a circuit using the complete
// rule contract exposed by FerricStore.
func (c *Client) CircuitOpenWithOptions(ctx context.Context, scope string, opt CircuitOpenOptions) (CircuitBreakerStatus, error) {
	errorClasses, err := validateCircuitOpenOptions(scope, opt)
	if err != nil {
		return CircuitBreakerStatus{}, err
	}
	args := []any{"FLOW.CIRCUIT.OPEN", scope}
	appendInt64Ptr(&args, "OPEN_MS", opt.OpenMS)
	appendInt64Ptr(&args, "FAILURE_THRESHOLD", opt.FailureThreshold)
	appendInt64Ptr(&args, "WINDOW_MS", opt.WindowMS)
	appendInt64Ptr(&args, "MIN_CALLS", opt.MinCalls)
	appendInt64Ptr(&args, "FAILURE_RATE_PCT", opt.FailureRatePct)
	appendInt64Ptr(&args, "LATENCY_THRESHOLD_MS", opt.LatencyThresholdMS)
	if opt.ErrorClasses != nil {
		args = append(args, "ERROR_CLASSES", errorClasses)
	}
	appendInt64Ptr(&args, "HALF_OPEN_MAX_PROBES", opt.HalfOpenMaxProbes)
	appendInt64Ptr(&args, "HALF_OPEN_SUCCESS_THRESHOLD", opt.HalfOpenSuccessThreshold)
	appendInt64Ptr(&args, "NOW", opt.NowMS)
	appendInt64Ptr(&args, "DEADLINE_MS", opt.DeadlineMS)
	return circuitResult(c.typedReply(ctx, args...))
}

func (c *Client) CircuitClose(ctx context.Context, scope string, nowMS *int64) (CircuitBreakerStatus, error) {
	if err := validateCircuitOperation(scope, nil, nil, nowMS); err != nil {
		return CircuitBreakerStatus{}, err
	}
	args := []any{"FLOW.CIRCUIT.CLOSE", scope}
	appendInt64Ptr(&args, "NOW", nowMS)
	return circuitResult(c.typedReply(ctx, args...))
}

func (c *Client) CircuitGet(ctx context.Context, scope string) (*CircuitBreakerStatus, error) {
	if err := validateCircuitOperation(scope, nil, nil, nil); err != nil {
		return nil, err
	}
	value, err := c.typedReply(ctx, "FLOW.CIRCUIT.GET", scope)
	if err != nil || value == nil {
		return nil, err
	}
	result, err := circuitResult(value, nil)
	return &result, err
}
