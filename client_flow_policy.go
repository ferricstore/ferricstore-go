package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
)

func (c *Client) InstallPolicy(ctx context.Context, flowType string, opt PolicyOptions) (any, error) {
	return c.SetPolicy(ctx, flowType, opt)
}

func (c *Client) InstallRetryPolicy(ctx context.Context, flowType string, retry *RetryPolicy, states map[string]RetryPolicy) (any, error) {
	return c.SetPolicy(ctx, flowType, PolicyOptions{Retry: retry, States: states})
}

func (c *Client) SetPolicy(ctx context.Context, flowType string, opt PolicyOptions) (any, error) {
	opt = canonicalizePolicyMetadataKeys(opt)
	if err := validatePolicyOptions(flowType, opt); err != nil {
		return nil, err
	}
	args := []any{"FLOW.POLICY.SET", flowType}
	if err := appendFlowMaxActiveMS(&args, opt.MaxActiveMS); err != nil {
		return nil, err
	}
	if opt.IndexedAttributes != nil {
		appendOpt(&args, "INDEXED_ATTRIBUTES", opt.IndexedAttributes)
	}
	if opt.IndexedStateMeta != "" || opt.IndexedStateMetaSet {
		args = append(args, "INDEXED_STATE_META", opt.IndexedStateMeta)
	}
	if opt.Retry != nil {
		appendRetryPolicy(&args, *opt.Retry)
	}
	for _, state := range sortedKeys(opt.States) {
		policy := opt.States[state]
		args = append(args, "STATE", state)
		appendRetryPolicy(&args, policy)
	}
	for _, state := range sortedKeys(opt.StatePolicies) {
		policy := opt.StatePolicies[state]
		args = append(args, "STATE", state)
		if policy.Mode != "" {
			mode, err := flowStateModeCommandToken(policy.Mode)
			if err != nil {
				return nil, err
			}
			appendOpt(&args, "MODE", mode)
		}
		if policy.Retry != nil {
			appendRetryPolicy(&args, *policy.Retry)
		}
	}
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	if err := validateAppliedPolicy(value, opt); err != nil {
		return nil, err
	}
	return value, nil
}

func canonicalizePolicyMetadataKeys(opt PolicyOptions) PolicyOptions {
	if opt.IndexedAttributes != nil {
		normalized := make([]string, 0, len(opt.IndexedAttributes))
		seen := make(map[string]struct{}, len(opt.IndexedAttributes))
		for _, name := range opt.IndexedAttributes {
			name = canonicalFlowMetadataKey(name)
			if _, duplicate := seen[name]; duplicate {
				continue
			}
			seen[name] = struct{}{}
			normalized = append(normalized, name)
		}
		opt.IndexedAttributes = normalized
	}
	opt.IndexedStateMeta = canonicalFlowMetadataKey(opt.IndexedStateMeta)
	return opt
}

func validateAppliedPolicy(value any, opt PolicyOptions) error {
	if opt.MaxActiveMS == nil && opt.Retry == nil && len(opt.States) == 0 && len(opt.StatePolicies) == 0 && opt.IndexedAttributes == nil && opt.IndexedStateMeta == "" && !opt.IndexedStateMetaSet {
		return nil
	}
	normalized, normalizeErr := normalizeAdminResponse(value)
	if normalizeErr != nil {
		return normalizeErr
	}
	policy, err := nativeMap(normalized)
	if err != nil {
		// Some compatible executors return only an acknowledgement. Accept that
		// compatibility shape only when it is the protocol's exact OK response;
		// arbitrary scalars contain no state and must fail closed.
		if _, ackErr := responseOK(normalized, nil); ackErr == nil {
			return nil
		}
		return fmt.Errorf("FLOW.POLICY.SET expected a policy map or OK acknowledgement: %w", err)
	}
	if opt.IndexedAttributes != nil {
		raw, exists := policy["indexed_attributes"]
		if !exists {
			return errors.New("FLOW.POLICY.SET response omitted indexed attributes")
		}
		actual := stringList(raw)
		if !slices.Equal(actual, opt.IndexedAttributes) {
			return fmt.Errorf("FLOW.POLICY.SET response omitted indexed attributes: got %v, want %v", actual, opt.IndexedAttributes)
		}
	}
	if opt.IndexedStateMeta != "" || opt.IndexedStateMetaSet {
		raw, exists := policy["indexed_state_meta"]
		if !exists || asString(raw) != opt.IndexedStateMeta {
			return fmt.Errorf("FLOW.POLICY.SET response omitted indexed state metadata %q", opt.IndexedStateMeta)
		}
	}
	if opt.Retry != nil {
		if err := validateAppliedRetryPolicy(policy["retry"], *opt.Retry, "type"); err != nil {
			return err
		}
	}
	if opt.MaxActiveMS != nil {
		actual, exists := policy["max_active_ms"]
		if !exists {
			return errors.New("FLOW.POLICY.SET response omitted max_active_ms")
		}
		if err := validateAppliedMaxActiveMS(actual, opt.MaxActiveMS); err != nil {
			return err
		}
	}

	if len(opt.States) == 0 && len(opt.StatePolicies) == 0 {
		return nil
	}
	statesValue, exists := policy["states"]
	if !exists || statesValue == nil {
		return errors.New("FLOW.POLICY.SET response omitted state policy updates")
	}
	states, err := nativeMap(statesValue)
	if err != nil {
		return fmt.Errorf("FLOW.POLICY.SET returned invalid state policies: %w", err)
	}
	for state, expected := range opt.States {
		stateValue, exists := states[state]
		if !exists {
			return fmt.Errorf("FLOW.POLICY.SET response omitted state policy %q", state)
		}
		statePolicy, err := nativeMap(stateValue)
		if err != nil {
			return fmt.Errorf("FLOW.POLICY.SET returned invalid state policy %q: %w", state, err)
		}
		if err := validateAppliedRetryPolicy(statePolicy["retry"], expected, "state "+state); err != nil {
			return err
		}
	}
	for state, expected := range opt.StatePolicies {
		stateValue, exists := states[state]
		if !exists {
			return fmt.Errorf("FLOW.POLICY.SET response omitted state policy %q", state)
		}
		statePolicy, err := nativeMap(stateValue)
		if err != nil {
			return fmt.Errorf("FLOW.POLICY.SET returned invalid state policy %q: %w", state, err)
		}
		if expected.Mode != "" && !strings.EqualFold(asString(statePolicy["mode"]), string(expected.Mode)) {
			return fmt.Errorf("FLOW.POLICY.SET response omitted state policy %q mode %s", state, expected.Mode)
		}
		if expected.Retry != nil {
			if err := validateAppliedRetryPolicy(statePolicy["retry"], *expected.Retry, "state "+state); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateAppliedMaxActiveMS(actual, expected any) error {
	canonical, err := canonicalFlowMaxActiveMS(expected)
	if err != nil {
		return err
	}
	if canonical == FlowMaxActiveInfinity {
		if actual == nil || strings.EqualFold(asString(actual), FlowMaxActiveInfinity) {
			return nil
		}
		return fmt.Errorf("FLOW.POLICY.SET response omitted max_active_ms infinity")
	}
	value, err := responseInt64(actual, nil)
	if err != nil || value != canonical.(int64) {
		return fmt.Errorf("FLOW.POLICY.SET response omitted max_active_ms %d", canonical)
	}
	return nil
}

func validateAppliedRetryPolicy(value any, expected RetryPolicy, scope string) error {
	if expected.MaxRetries == 0 && !expected.MaxRetriesSet && expected.Backoff == "" && expected.BaseMS == 0 && !expected.BaseMSSet && expected.MaxMS == 0 && !expected.MaxMSSet && expected.JitterPct == 0 && !expected.JitterPctSet && expected.ExhaustedTo == "" {
		return nil
	}
	retry, err := nativeMap(value)
	if err != nil {
		return fmt.Errorf("FLOW.POLICY.SET response omitted %s retry policy: %w", scope, err)
	}
	if expected.MaxRetries != 0 || expected.MaxRetriesSet {
		if err := validateAppliedPolicyInteger(retry, "max_retries", int64(expected.MaxRetries), scope); err != nil {
			return err
		}
	}
	if expected.ExhaustedTo != "" && asString(retry["exhausted_to"]) != expected.ExhaustedTo {
		return fmt.Errorf("FLOW.POLICY.SET response omitted %s retry exhausted_to %q", scope, expected.ExhaustedTo)
	}
	if expected.Backoff == "" && expected.BaseMS == 0 && !expected.BaseMSSet && expected.MaxMS == 0 && !expected.MaxMSSet && expected.JitterPct == 0 && !expected.JitterPctSet {
		return nil
	}
	backoff, err := nativeMap(retry["backoff"])
	if err != nil {
		return fmt.Errorf("FLOW.POLICY.SET response omitted %s retry backoff: %w", scope, err)
	}
	if expected.Backoff != "" && !strings.EqualFold(asString(backoff["kind"]), expected.Backoff) {
		return fmt.Errorf("FLOW.POLICY.SET response omitted %s retry backoff %q", scope, expected.Backoff)
	}
	if expected.BaseMS != 0 || expected.BaseMSSet {
		if err := validateAppliedPolicyInteger(backoff, "base_ms", expected.BaseMS, scope); err != nil {
			return err
		}
	}
	if expected.MaxMS != 0 || expected.MaxMSSet {
		if err := validateAppliedPolicyInteger(backoff, "max_ms", expected.MaxMS, scope); err != nil {
			return err
		}
	}
	if expected.JitterPct != 0 || expected.JitterPctSet {
		if err := validateAppliedPolicyInteger(backoff, "jitter_pct", int64(expected.JitterPct), scope); err != nil {
			return err
		}
	}
	return nil
}

func validateAppliedPolicyInteger(values map[string]any, field string, expected int64, scope string) error {
	actual, err := responseInt64(values[field], nil)
	if err != nil || actual != expected {
		return fmt.Errorf("FLOW.POLICY.SET response omitted %s retry %s %d", scope, field, expected)
	}
	return nil
}

func (c *Client) PolicyGet(ctx context.Context, flowType, state string) (map[string]any, error) {
	if err := validatePublicFlowType("flow type", flowType); err != nil {
		return nil, err
	}
	args := []any{"FLOW.POLICY.GET", flowType}
	appendOpt(&args, "STATE", state)
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	return normalizedAdminMap(value)
}

func (c *Client) RetentionCleanup(ctx context.Context, opt RetentionCleanupOptions) (map[string]any, error) {
	if err := validateRetentionCleanup(opt); err != nil {
		return nil, err
	}
	args := []any{"FLOW.RETENTION_CLEANUP"}
	appendIntPtr(&args, "LIMIT", opt.Limit)
	appendInt64Ptr(&args, "NOW", opt.NowMS)
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	return nativeMap(value)
}

func appendRetryPolicy(args *[]any, policy RetryPolicy) {
	if policy.MaxRetries != 0 || policy.MaxRetriesSet {
		appendOpt(args, "MAX_RETRIES", policy.MaxRetries)
	}
	backoff := policy.Backoff
	if backoff != "" {
		backoff = strings.ToLower(strings.TrimSpace(backoff))
	}
	appendOpt(args, "BACKOFF", backoff)
	if policy.BaseMS != 0 || policy.BaseMSSet {
		appendOpt(args, "BASE_MS", policy.BaseMS)
	}
	if policy.MaxMS != 0 || policy.MaxMSSet {
		appendOpt(args, "MAX_MS", policy.MaxMS)
	}
	if policy.JitterPct != 0 || policy.JitterPctSet {
		appendOpt(args, "JITTER_PCT", policy.JitterPct)
	}
	appendOpt(args, "EXHAUSTED_TO", policy.ExhaustedTo)
}

func flowStateModeCommandToken(mode FlowStateMode) (string, error) {
	switch strings.ToUpper(string(mode)) {
	case string(FlowStateModeParallel):
		return string(FlowStateModeParallel), nil
	case string(FlowStateModeFIFO):
		return string(FlowStateModeFIFO), nil
	default:
		return "", errors.New("ERR flow state mode must be parallel or fifo")
	}
}
