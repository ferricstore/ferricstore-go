package ferricstore

import (
	"errors"
	"fmt"
	"strings"
)

const (
	maxFlowPolicyRetries = 1_000
	maxFlowRetryDelayMS  = int64(2_592_000_000)
	maxFlowIndexedAttrs  = 3
)

func validatePolicyOptions(flowType string, opt PolicyOptions) error {
	if err := validateRequiredText("flow type", flowType); err != nil {
		return err
	}
	if len(opt.IndexedAttributes) > maxFlowIndexedAttrs {
		return fmt.Errorf("flow indexed attributes support at most %d keys", maxFlowIndexedAttrs)
	}
	indexed := make(map[string]struct{}, len(opt.IndexedAttributes))
	for _, name := range opt.IndexedAttributes {
		name, err := validateFlowMetadataKey("attribute", name)
		if err != nil {
			return err
		}
		if _, exists := indexed[name]; exists {
			return fmt.Errorf("flow indexed attribute %q is duplicated", name)
		}
		indexed[name] = struct{}{}
	}
	if opt.IndexedStateMeta != "" {
		if _, err := validateFlowMetadataKey("state_meta", opt.IndexedStateMeta); err != nil {
			return err
		}
	}
	if opt.Retry != nil {
		if err := validateRetryPolicy(*opt.Retry); err != nil {
			return err
		}
	}
	for state, policy := range opt.States {
		if err := validateRequiredText("flow state", state); err != nil {
			return err
		}
		if _, exists := opt.StatePolicies[state]; exists {
			return fmt.Errorf("flow state %q appears in both States and StatePolicies", state)
		}
		if err := validateRetryPolicy(policy); err != nil {
			return fmt.Errorf("flow state %q: %w", state, err)
		}
	}
	for state, policy := range opt.StatePolicies {
		if err := validateRequiredText("flow state", state); err != nil {
			return err
		}
		if policy.Mode != "" {
			if _, err := flowStateModeCommandToken(policy.Mode); err != nil {
				return fmt.Errorf("flow state %q: %w", state, err)
			}
		}
		if policy.Retry != nil {
			if err := validateRetryPolicy(*policy.Retry); err != nil {
				return fmt.Errorf("flow state %q: %w", state, err)
			}
		}
	}
	return nil
}

func validateRetryPolicy(policy RetryPolicy) error {
	if policy.MaxRetries < 0 || policy.MaxRetries > maxFlowPolicyRetries {
		return fmt.Errorf("flow retry max_retries must be between 0 and %d", maxFlowPolicyRetries)
	}
	if policy.Backoff != "" {
		switch strings.ToLower(strings.TrimSpace(policy.Backoff)) {
		case "none", "fixed", "linear", "exponential":
		default:
			return errors.New("flow retry backoff must be none, fixed, linear, or exponential")
		}
	}
	if policy.BaseMS < 0 || policy.BaseMS > maxFlowRetryDelayMS {
		return fmt.Errorf("flow retry base_ms must be between 0 and %d", maxFlowRetryDelayMS)
	}
	if policy.MaxMS < 0 || policy.MaxMS > maxFlowRetryDelayMS {
		return fmt.Errorf("flow retry max_ms must be between 0 and %d", maxFlowRetryDelayMS)
	}
	if policy.JitterPct < 0 || policy.JitterPct > 100 {
		return errors.New("flow retry jitter_pct must be between 0 and 100")
	}
	if policy.ExhaustedTo == "running" {
		return errors.New("flow retry exhausted_to cannot be running")
	}
	return nil
}

func validateRetentionCleanup(opt RetentionCleanupOptions) error {
	if err := validateOptionalPositiveInt("limit", opt.Limit); err != nil {
		return err
	}
	return validateOptionalNonNegativeInt64("now_ms", opt.NowMS)
}
