package ferricstore

import (
	"fmt"
	"strings"
)

func decodePolicySnapshot(value any, expectedType, expectedState string) (PolicySnapshot, error) {
	policy, err := normalizedPolicyMap(value)
	if err != nil {
		return PolicySnapshot{}, fmt.Errorf("decode Flow policy snapshot: %w", err)
	}
	flowType, err := policySnapshotText(policy, "type", true)
	if err != nil {
		return PolicySnapshot{}, err
	}
	if flowType != expectedType {
		return PolicySnapshot{}, fmt.Errorf(
			"flow policy snapshot type = %q, want %q", flowType, expectedType,
		)
	}
	generation, err := policySnapshotGeneration(policy)
	if err != nil {
		return PolicySnapshot{}, err
	}
	state, err := policySnapshotText(policy, "state", false)
	if err != nil {
		return PolicySnapshot{}, err
	}
	if state != expectedState {
		return PolicySnapshot{}, fmt.Errorf(
			"flow policy snapshot state = %q, want %q", state, expectedState,
		)
	}
	mode, err := decodePolicyStateMode(policy["mode"], expectedState != "")
	if err != nil {
		return PolicySnapshot{}, fmt.Errorf("decode Flow policy mode: %w", err)
	}
	maxActiveMS, err := decodePolicyMaxActiveMS(policy["max_active_ms"])
	if err != nil {
		return PolicySnapshot{}, fmt.Errorf("decode Flow policy max_active_ms: %w", err)
	}
	retry, err := decodePolicyMap(policy["retry"], "retry")
	if err != nil {
		return PolicySnapshot{}, err
	}
	retention, err := decodePolicyMap(policy["retention"], "retention")
	if err != nil {
		return PolicySnapshot{}, err
	}
	governance, err := decodePolicyMap(policy["governance"], "governance")
	if err != nil {
		return PolicySnapshot{}, err
	}
	indexedAttributes, err := decodePolicyStringList(policy["indexed_attributes"], "indexed_attributes")
	if err != nil {
		return PolicySnapshot{}, err
	}
	indexedStateMeta, err := policySnapshotText(policy, "indexed_state_meta", false)
	if err != nil {
		return PolicySnapshot{}, err
	}
	states, err := decodePolicyStates(policy["states"])
	if err != nil {
		return PolicySnapshot{}, err
	}
	return PolicySnapshot{
		Type:              flowType,
		State:             state,
		Generation:        generation,
		Version:           policy["version"],
		Mode:              mode,
		MaxActiveMS:       maxActiveMS,
		Retry:             retry,
		Retention:         retention,
		IndexedAttributes: indexedAttributes,
		IndexedStateMeta:  indexedStateMeta,
		Governance:        governance,
		States:            states,
		Raw:               policy,
	}, nil
}

func normalizedPolicyMap(value any) (map[string]any, error) {
	normalized, err := normalizeAdminResponse(value)
	if err != nil {
		return nil, err
	}
	if mapping, ok := normalized.(map[string]any); ok {
		return mapping, nil
	}
	return nativeMap(normalized)
}

func policySnapshotGeneration(policy map[string]any) (int64, error) {
	raw, exists := policy["generation"]
	if !exists {
		return 0, fmt.Errorf("flow policy snapshot omitted generation")
	}
	generation, err := responseInt64(raw, nil)
	if err != nil || generation < 0 || generation > maxFlowPolicyGeneration {
		return 0, fmt.Errorf(
			"flow policy snapshot generation must be between 0 and %d",
			maxFlowPolicyGeneration,
		)
	}
	return generation, nil
}

func policySnapshotText(policy map[string]any, field string, required bool) (string, error) {
	value, exists := policy[field]
	if !exists || value == nil {
		if required {
			return "", fmt.Errorf("flow policy snapshot omitted %s", field)
		}
		return "", nil
	}
	text, err := responseString(value, nil)
	if err != nil || required && text == "" {
		return "", fmt.Errorf("flow policy snapshot %s must be a non-empty string", field)
	}
	return text, nil
}

func decodePolicyStateMode(value any, required bool) (FlowStateMode, error) {
	if value == nil {
		if required {
			return "", fmt.Errorf("state policy omitted mode")
		}
		return "", nil
	}
	mode, err := responseString(value, nil)
	if err != nil {
		return "", err
	}
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case string(FlowStateModeParallel):
		return FlowStateModeParallel, nil
	case string(FlowStateModeFIFO):
		return FlowStateModeFIFO, nil
	default:
		return "", fmt.Errorf("state policy mode %q is unsupported", mode)
	}
}

func decodePolicyMaxActiveMS(value any) (*int64, error) {
	if value == nil {
		return nil, nil
	}
	milliseconds, err := responseInt64(value, nil)
	if err != nil || milliseconds <= 0 {
		return nil, fmt.Errorf("expected a positive integer or nil")
	}
	return &milliseconds, nil
}

func decodePolicyMap(value any, field string) (map[string]any, error) {
	if value == nil {
		return nil, nil
	}
	if mapping, ok := value.(map[string]any); ok {
		return mapping, nil
	}
	mapping, err := nativeMap(value)
	if err != nil {
		return nil, fmt.Errorf("decode Flow policy %s: %w", field, err)
	}
	return mapping, nil
}

func decodePolicyStringList(value any, field string) ([]string, error) {
	if value == nil {
		return nil, nil
	}
	if values, ok := value.([]string); ok {
		return append([]string(nil), values...), nil
	}
	values, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("decode Flow policy %s: expected an array", field)
	}
	out := make([]string, len(values))
	for index, value := range values {
		text, err := responseString(value, nil)
		if err != nil {
			return nil, fmt.Errorf("decode Flow policy %s item %d: %w", field, index, err)
		}
		out[index] = text
	}
	return out, nil
}

func decodePolicyStates(value any) (map[string]PolicyStateSnapshot, error) {
	if value == nil {
		return nil, nil
	}
	states, ok := value.(map[string]any)
	if !ok {
		var err error
		states, err = nativeMap(value)
		if err != nil {
			return nil, fmt.Errorf("decode Flow policy states: %w", err)
		}
	}
	out := make(map[string]PolicyStateSnapshot, len(states))
	for state, value := range states {
		if state == "" {
			return nil, fmt.Errorf("decode Flow policy states: empty state name")
		}
		policy, ok := value.(map[string]any)
		if !ok {
			var err error
			policy, err = nativeMap(value)
			if err != nil {
				return nil, fmt.Errorf("decode Flow policy state %q: %w", state, err)
			}
		}
		mode, err := decodePolicyStateMode(policy["mode"], true)
		if err != nil {
			return nil, fmt.Errorf("decode Flow policy state %q: %w", state, err)
		}
		maxActiveMS, err := decodePolicyMaxActiveMS(policy["max_active_ms"])
		if err != nil {
			return nil, fmt.Errorf("decode Flow policy state %q max_active_ms: %w", state, err)
		}
		retry, err := decodePolicyMap(policy["retry"], "state "+state+" retry")
		if err != nil {
			return nil, err
		}
		retention, err := decodePolicyMap(policy["retention"], "state "+state+" retention")
		if err != nil {
			return nil, err
		}
		governance, err := decodePolicyMap(policy["governance"], "state "+state+" governance")
		if err != nil {
			return nil, err
		}
		out[state] = PolicyStateSnapshot{
			Mode: mode, MaxActiveMS: maxActiveMS, Retry: retry,
			Retention: retention, Governance: governance, Raw: policy,
		}
	}
	return out, nil
}
