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
	if err := appendFlowAdminOptions(payload, args[len(leadingFields):]); err != nil {
		return nativeCommand{}, true, err
	}
	return nativeCommand{name: name, opcode: opcode, laneID: 1, payload: payload}, true, nil
}

func appendFlowAdminOptions(payload map[string]any, args []any) error {
	if len(args)%2 != 0 {
		return errors.New("FLOW admin options must be key/value pairs")
	}
	for idx := 0; idx < len(args); idx += 2 {
		key, ok := flowAdminNativeField(asString(args[idx]))
		if !ok {
			continue
		}
		payload[key] = flowAdminNativeValue(key, args[idx+1])
	}
	return nil
}

func flowAdminNativeField(token string) (string, bool) {
	switch strings.ToUpper(token) {
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
	case "AMOUNT":
		return "amount", true
	case "LIMIT":
		return "limit", true
	case "WINDOW_MS":
		return "window_ms", true
	case "RESERVATION_ID":
		return "reservation_id", true
	case "ACTUAL_AMOUNT":
		return "actual_amount", true
	case "USAGE":
		return "usage", true
	case "SHARD_ID":
		return "shard_id", true
	case "TTL_MS":
		return "ttl_ms", true
	case "NOW":
		return "now_ms", true
	case "STATE":
		return "state", true
	case "COUNT":
		return "count", true
	case "FROM_MS":
		return "from_ms", true
	case "TO_MS":
		return "to_ms", true
	case "REV":
		return "rev", true
	case "CONSISTENT_PROJECTION":
		return "consistent_projection", true
	case "DEADLINE_MS":
		return "deadline_ms", true
	default:
		return scheduleNativeField(token)
	}
}

func flowAdminNativeValue(key string, value any) any {
	switch key {
	case "rev", "consistent_projection":
		return asBool(value)
	default:
		return value
	}
}

func buildFlowScheduleCreateNative(args []any) (nativeCommand, bool, error) {
	if len(args) < 1 {
		return nativeCommand{}, true, errors.New("FLOW.SCHEDULE.CREATE requires id")
	}
	payload := map[string]any{"id": args[0]}
	if err := appendScheduleOptions(payload, args[1:]); err != nil {
		return nativeCommand{}, true, err
	}
	return nativeCommand{name: "FLOW.SCHEDULE.CREATE", opcode: nativeOpFlowScheduleCreate, laneID: 1, payload: payload}, true, nil
}

func buildFlowScheduleIDNative(name string, opcode uint16, args []any) (nativeCommand, bool, error) {
	if len(args) < 1 {
		return nativeCommand{}, true, errors.New(name + " requires id")
	}
	payload := map[string]any{"id": args[0]}
	if err := appendScheduleOptions(payload, args[1:]); err != nil {
		return nativeCommand{}, true, err
	}
	return nativeCommand{name: name, opcode: opcode, laneID: 1, payload: payload}, true, nil
}

func buildFlowScheduleOptionsNative(name string, opcode uint16, args []any, requireID bool) (nativeCommand, bool, error) {
	payload := map[string]any{}
	if requireID {
		if len(args) < 1 {
			return nativeCommand{}, true, errors.New(name + " requires id")
		}
		payload["id"] = args[0]
		args = args[1:]
	}
	if err := appendScheduleOptions(payload, args); err != nil {
		return nativeCommand{}, true, err
	}
	return nativeCommand{name: name, opcode: opcode, laneID: 1, payload: payload}, true, nil
}

func appendScheduleOptions(payload map[string]any, args []any) error {
	if len(args)%2 != 0 {
		return errors.New("FLOW.SCHEDULE options must be key/value pairs")
	}
	for idx := 0; idx < len(args); idx += 2 {
		key, ok := scheduleNativeField(asString(args[idx]))
		if !ok {
			continue
		}
		payload[key] = scheduleNativeValue(key, args[idx+1])
	}
	return nil
}

func scheduleNativeValue(key string, value any) any {
	switch key {
	case "overwrite":
		return asBool(value)
	default:
		return value
	}
}

func scheduleNativeField(token string) (string, bool) {
	switch strings.ToUpper(token) {
	case "KIND":
		return "kind", true
	case "TARGET":
		return "target", true
	case "AT_MS":
		return "at_ms", true
	case "DELAY_MS":
		return "delay_ms", true
	case "START_AT_MS":
		return "start_at_ms", true
	case "EVERY_MS":
		return "every_ms", true
	case "CRON":
		return "cron", true
	case "TIMEZONE":
		return "timezone", true
	case "NOW":
		return "now_ms", true
	case "OVERWRITE":
		return "overwrite", true
	case "OVERLAP_POLICY":
		return "overlap_policy", true
	case "OVERLAP_RETRY_MS":
		return "overlap_retry_ms", true
	case "MAX_FIRES":
		return "max_fires", true
	case "END_AT_MS":
		return "end_at_ms", true
	case "WORKER":
		return "worker", true
	case "LIMIT":
		return "limit", true
	case "LEASE_MS":
		return "lease_ms", true
	case "BLOCK", "BLOCK_MS":
		return "block_ms", true
	case "STATE":
		return "state", true
	case "TARGET_TYPE":
		return "target_type", true
	case "FROM_MS":
		return "from_ms", true
	case "TO_MS":
		return "to_ms", true
	case "COUNT":
		return "count", true
	case "DEADLINE_MS":
		return "deadline_ms", true
	default:
		return "", false
	}
}
