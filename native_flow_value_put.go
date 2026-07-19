package ferricstore

import (
	"errors"
	"strings"
)

func buildFlowValuePutNative(args []any) (nativeCommand, bool, error) {
	if len(args) == 0 {
		return nativeCommand{}, true, errors.New("FLOW.VALUE.PUT requires a value")
	}
	payload := map[string]any{"value": args[0]}
	for index := 1; index < len(args); index += 2 {
		if index+1 >= len(args) {
			return nativeCommand{}, true, errors.New("FLOW.VALUE.PUT options must be key/value pairs")
		}
		token := strings.ToUpper(asString(args[index]))
		value := args[index+1]
		switch token {
		case "PARTITION":
			payload["partition_key"] = value
		case "OWNER_FLOW_ID":
			payload["owner_flow_id"] = value
		case "NAME":
			payload["name"] = value
		case "OVERRIDE":
			parsed, err := nativeFlowBool(value)
			if err != nil {
				return nativeCommand{}, true, err
			}
			payload["override"] = parsed
		case "TTL", "TTL_MS":
			payload["ttl_ms"] = value
		case "NOW":
			payload["now_ms"] = value
		case "RETURN":
			payload["return"] = value
		default:
			return nativeCommand{}, false, nil
		}
	}
	return nativeCommand{
		name: "FLOW.VALUE.PUT", opcode: nativeOpFlowValuePut, laneID: 1, payload: payload,
	}, true, nil
}
