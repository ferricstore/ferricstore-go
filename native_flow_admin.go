package ferricstore

import (
	"errors"
	"strings"
)

func buildFlowAdminNative(name string, opcode uint16, args []any, leadingFields ...string) (nativeCommand, bool, error) {
	payload := map[string]any{}
	if len(args) < len(leadingFields) {
		return nativeCommand{}, true, errors.New(name + " missing required arguments")
	}
	for idx, field := range leadingFields {
		payload[field] = args[idx]
	}
	ok, err := appendFlowAdminOptions(payload, args[len(leadingFields):])
	if err != nil {
		return nativeCommand{}, true, err
	}
	if !ok {
		return nativeCommand{}, false, nil
	}
	return nativeCommand{name: name, opcode: opcode, laneID: 1, payload: payload}, true, nil
}

func buildFlowPolicySetNative(args []any) (nativeCommand, bool, error) {
	if len(args) == 0 {
		return nativeCommand{}, true, errors.New("FLOW.POLICY.SET missing required arguments")
	}
	payload := map[string]any{"type": args[0]}
	retry := map[string]any{}
	retention := map[string]any{}
	states := map[string]any{}

	for idx := 1; idx < len(args); {
		token := strings.ToUpper(asString(args[idx]))
		if token == "STATE" {
			if idx+1 >= len(args) {
				return nativeCommand{}, true, errors.New("FLOW.POLICY.SET state requires name")
			}
			stateName := asString(args[idx+1])
			idx += 2
			statePolicy := map[string]any{}
			stateRetry := map[string]any{}
			stateRetention := map[string]any{}
			for idx < len(args) && strings.ToUpper(asString(args[idx])) != "STATE" {
				if idx+1 >= len(args) {
					return nativeCommand{}, true, errors.New("FLOW.POLICY.SET options must be key/value pairs")
				}
				if err := putFlowPolicyOption(payload, statePolicy, stateRetry, stateRetention, args[idx], args[idx+1], true); err != nil {
					return nativeCommand{}, true, err
				}
				idx += 2
			}
			if len(stateRetry) > 0 {
				statePolicy["retry"] = stateRetry
			}
			if len(stateRetention) > 0 {
				statePolicy["retention"] = stateRetention
			}
			states[stateName] = statePolicy
			continue
		}

		if idx+1 >= len(args) {
			return nativeCommand{}, true, errors.New("FLOW.POLICY.SET options must be key/value pairs")
		}
		if err := putFlowPolicyOption(payload, payload, retry, retention, args[idx], args[idx+1], false); err != nil {
			return nativeCommand{}, true, err
		}
		idx += 2
	}

	if len(retry) > 0 {
		payload["retry"] = retry
	}
	if len(retention) > 0 {
		payload["retention"] = retention
	}
	if len(states) > 0 {
		payload["states"] = states
	}
	return nativeCommand{name: "FLOW.POLICY.SET", opcode: nativeOpFlowPolicySet, laneID: 1, payload: payload}, true, nil
}

func putFlowPolicyOption(payload, policy, retry, retention map[string]any, tokenValue, value any, stateScoped bool) error {
	token := strings.ToUpper(asString(tokenValue))
	switch token {
	case "INDEXED_ATTRIBUTES":
		if stateScoped {
			return errors.New("ERR flow indexed_attributes is type-level only")
		}
		payload["indexed_attributes"] = value
	case "INDEXED_STATE_META":
		if stateScoped {
			return errors.New("ERR flow indexed_state_meta is type-level only")
		}
		payload["indexed_state_meta"] = value
	case "MODE":
		if !stateScoped {
			return errors.New("ERR flow state mode is state-level only")
		}
		mode, err := flowStateModeNativeValue(value)
		if err != nil {
			return err
		}
		policy["mode"] = mode
	case "RETENTION_TTL", "RETENTION_TTL_MS":
		retention["ttl_ms"] = value
	case "HISTORY_MAX_EVENTS":
		retention["history_max_events"] = value
	case "HISTORY_HOT_MAX_EVENTS":
		return errors.New("ERR flow retention history_hot_max_events is internal")
	case "MAX_RETRIES":
		retry["max_retries"] = value
	case "EXHAUSTED_TO":
		retry["exhausted_to"] = value
	case "BACKOFF":
		putFlowPolicyBackoff(retry, "kind", value)
	case "BASE_MS":
		putFlowPolicyBackoff(retry, "base_ms", value)
	case "MAX_MS":
		putFlowPolicyBackoff(retry, "max_ms", value)
	case "JITTER_PCT":
		putFlowPolicyBackoff(retry, "jitter_pct", value)
	default:
		return errors.New("ERR syntax error")
	}
	return nil
}

func flowStateModeNativeValue(value any) (string, error) {
	switch strings.ToUpper(asString(value)) {
	case string(FlowStateModeParallel):
		return "parallel", nil
	case string(FlowStateModeFIFO):
		return "fifo", nil
	default:
		return "", errors.New("ERR flow state mode must be parallel or fifo")
	}
}

func putFlowPolicyBackoff(retry map[string]any, key string, value any) {
	backoff, _ := retry["backoff"].(map[string]any)
	if backoff == nil {
		backoff = map[string]any{}
		retry["backoff"] = backoff
	}
	backoff[key] = value
}

func appendFlowAdminOptions(payload map[string]any, args []any) (bool, error) {
	for idx := 0; idx < len(args); {
		token := strings.ToUpper(asString(args[idx]))
		switch token {
		case "ATTRIBUTE", "ATTRIBUTE_MERGE", "STATE_META", "VALUE", "VALUE_REF":
			if token == "STATE_META" && idx+1 < len(args) {
				switch args[idx+1].(type) {
				case map[string]any, map[string]string, map[interface{}]interface{}:
					payload["state_meta"] = args[idx+1]
					idx += 2
					continue
				}
			}
			if idx+2 >= len(args) {
				return false, errors.New("FLOW admin map options require key and value")
			}
			field := flowAdminMapField(token)
			putNativeMapValue(payload, field, asString(args[idx+1]), args[idx+2])
			idx += 3
			continue
		case "ATTRIBUTE_DELETE", "DROP_VALUE", "OVERRIDE_VALUE":
			if idx+1 >= len(args) {
				return false, errors.New("FLOW admin list options require value")
			}
			field := flowAdminListField(token)
			payload[field] = appendNativeListValue(payload[field], asString(args[idx+1]))
			idx += 2
			continue
		}

		if idx+1 >= len(args) {
			return false, errors.New("FLOW admin options must be key/value pairs")
		}
		key, ok := flowAdminNativeField(asString(args[idx]))
		if !ok {
			return false, nil
		}
		converted, ok := flowAdminNativeValue(key, args[idx+1])
		if !ok {
			return false, nil
		}
		payload[key] = converted
		idx += 2
	}
	return true, nil
}

func flowAdminMapField(token string) string {
	switch token {
	case "ATTRIBUTE":
		return "attributes"
	case "ATTRIBUTE_MERGE":
		return "attributes_merge"
	case "STATE_META":
		return "state_meta"
	case "VALUE":
		return "values"
	case "VALUE_REF":
		return "value_refs"
	default:
		return strings.ToLower(token)
	}
}

func flowAdminListField(token string) string {
	switch token {
	case "ATTRIBUTE_DELETE":
		return "attributes_delete"
	case "DROP_VALUE":
		return "drop_values"
	case "OVERRIDE_VALUE":
		return "override_values"
	default:
		return strings.ToLower(token)
	}
}

func putNativeMapValue(payload map[string]any, field, key string, value any) {
	mapping, _ := payload[field].(map[string]any)
	if mapping == nil {
		mapping = map[string]any{}
		payload[field] = mapping
	}
	mapping[key] = value
}

func appendNativeListValue(value any, item string) []string {
	items, _ := value.([]string)
	return append(items, item)
}

func flowAdminNativeField(token string) (string, bool) {
	switch strings.ToUpper(token) {
	case "TYPE":
		return "type", true
	case "INITIAL_STATE":
		return "initial_state", true
	case "WORKER":
		return "worker", true
	case "LEASE_MS":
		return "lease_ms", true
	case "COUNT":
		return "count", true
	case "PAYLOAD":
		return "payload", true
	case "PAYLOAD_REF":
		return "payload_ref", true
	case "RESULT":
		return "result", true
	case "STATES":
		return "states", true
	case "STEPS":
		return "steps", true
	case "ITEMS":
		return "items", true
	case "PARENT_FLOW_ID", "PARENT_ID":
		return "parent_flow_id", true
	case "ROOT_FLOW_ID", "ROOT_ID":
		return "root_flow_id", true
	case "CORRELATION_ID":
		return "correlation_id", true
	case "PRIORITY":
		return "priority", true
	case "RUN_AT":
		return "run_at_ms", true
	case "IDEMPOTENT":
		return "idempotent", true
	case "INDEPENDENT":
		return "independent", true
	case "RETENTION_TTL_MS":
		return "retention_ttl_ms", true
	case "INDEXED_ATTRIBUTES":
		return "indexed_attributes", true
	case "INDEXED_STATE_META":
		return "indexed_state_meta", true
	case "STATE":
		return "state", true
	case "FROM_MS":
		return "from_ms", true
	case "TO_MS":
		return "to_ms", true
	case "REV":
		return "rev", true
	case "TERMINAL_ONLY":
		return "terminal_only", true
	case "INCLUDE_COLD":
		return "include_cold", true
	case "CONSISTENT_PROJECTION":
		return "consistent_projection", true
	case "PARTITION":
		return "partition_key", true
	case "LEASE_TOKEN":
		return "lease_token", true
	case "FENCING", "FENCING_TOKEN":
		return "fencing_token", true
	case "EFFECT_KEY":
		return "effect_key", true
	case "EFFECT_TYPE":
		return "effect_type", true
	case "OPERATION_DIGEST":
		return "operation_digest", true
	case "IDEMPOTENCY_KEY":
		return "idempotency_key", true
	case "GOVERNANCE_SCOPE":
		return "governance_scope", true
	case "EXTERNAL_ID":
		return "external_id", true
	case "ERROR":
		return "error", true
	case "REASON":
		return "reason", true
	case "LATENCY_MS":
		return "latency_ms", true
	case "FLOW_ID":
		return "flow_id", true
	case "SCOPE":
		return "scope", true
	case "REQUESTED_BY":
		return "requested_by", true
	case "ASSIGNEES":
		return "assignees", true
	case "POLICY_HASH":
		return "policy_hash", true
	case "POLICY_VERSION":
		return "policy_version", true
	case "TIMEOUT_MS":
		return "timeout_ms", true
	case "EXPIRES_AT_MS":
		return "expires_at_ms", true
	case "APPROVER":
		return "approver", true
	case "STATUS":
		return "status", true
	case "OPEN_MS":
		return "open_ms", true
	case "FAILURE_THRESHOLD":
		return "failure_threshold", true
	case "MIN_CALLS":
		return "min_calls", true
	case "FAILURE_RATE_PCT":
		return "failure_rate_pct", true
	case "LATENCY_THRESHOLD_MS":
		return "latency_threshold_ms", true
	case "ERROR_CLASSES":
		return "error_classes", true
	case "HALF_OPEN_MAX_PROBES":
		return "half_open_max_probes", true
	case "HALF_OPEN_SUCCESS_THRESHOLD":
		return "half_open_success_threshold", true
	case "AMOUNT":
		return "amount", true
	case "LIMIT":
		return "limit", true
	case "WINDOW_MS":
		return "window_ms", true
	case "RESERVATION_ID":
		return "reservation_id", true
	case "RESERVATION_IDS":
		return "reservation_ids", true
	case "ACTUAL_AMOUNT":
		return "actual_amount", true
	case "USAGE":
		return "usage", true
	case "SHARD_ID":
		return "shard_id", true
	case "TTL_MS":
		return "ttl_ms", true
	case "GROUP":
		return "group_id", true
	case "WAIT":
		return "wait", true
	case "WAIT_STATE":
		return "wait_state", true
	case "SUCCESS":
		return "success", true
	case "FAILURE":
		return "failure", true
	case "FROM_STATE":
		return "from_state", true
	case "ON_CHILD_FAILED":
		return "on_child_failed", true
	case "ON_PARENT_CLOSED":
		return "on_parent_closed", true
	case "NOW":
		return "now_ms", true
	case "DEADLINE_MS":
		return "deadline_ms", true
	default:
		return scheduleNativeField(token)
	}
}

func flowAdminNativeValue(key string, value any) (any, bool) {
	switch key {
	case "rev", "terminal_only", "include_cold", "consistent_projection", "idempotent", "independent", "override":
		if value == nil {
			return nil, false
		}
		parsed, err := nativeFlowBool(value)
		return parsed, err == nil
	default:
		return value, true
	}
}
