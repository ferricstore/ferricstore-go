package ferricstore

import (
	"errors"
	"strings"
)

func buildFlowScheduleCreateNative(args []any) (nativeCommand, bool, error) {
	if len(args) < 1 {
		return nativeCommand{}, true, errors.New("FLOW.SCHEDULE.CREATE requires id")
	}
	payload := map[string]any{"id": args[0]}
	ok, err := appendScheduleOptions(payload, args[1:])
	if err != nil {
		return nativeCommand{}, true, err
	}
	if !ok {
		return nativeCommand{}, false, nil
	}
	return nativeCommand{name: "FLOW.SCHEDULE.CREATE", opcode: nativeOpFlowScheduleCreate, laneID: 1, payload: payload}, true, nil
}

func buildFlowScheduleIDNative(name string, opcode uint16, args []any) (nativeCommand, bool, error) {
	if len(args) < 1 {
		return nativeCommand{}, true, errors.New(name + " requires id")
	}
	payload := map[string]any{"id": args[0]}
	ok, err := appendScheduleOptions(payload, args[1:])
	if err != nil {
		return nativeCommand{}, true, err
	}
	if !ok {
		return nativeCommand{}, false, nil
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
	ok, err := appendScheduleOptions(payload, args)
	if err != nil {
		return nativeCommand{}, true, err
	}
	if !ok {
		return nativeCommand{}, false, nil
	}
	return nativeCommand{name: name, opcode: opcode, laneID: 1, payload: payload}, true, nil
}

func appendScheduleOptions(payload map[string]any, args []any) (bool, error) {
	if len(args)%2 != 0 {
		return false, errors.New("FLOW.SCHEDULE options must be key/value pairs")
	}
	for idx := 0; idx < len(args); idx += 2 {
		key, ok := scheduleNativeField(asString(args[idx]))
		if !ok {
			return false, nil
		}
		converted, ok := scheduleNativeValue(key, args[idx+1])
		if !ok {
			return false, nil
		}
		payload[key] = converted
	}
	return true, nil
}

func scheduleNativeValue(key string, value any) (any, bool) {
	switch key {
	case "overwrite":
		if value == nil {
			return nil, false
		}
		parsed, err := nativeFlowBool(value)
		return parsed, err == nil
	default:
		return value, true
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
	case "FIRE_AT_MS":
		return "fire_at_ms", true
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
