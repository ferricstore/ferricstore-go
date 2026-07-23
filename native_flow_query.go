package ferricstore

import (
	"errors"
	"fmt"
	"strings"
)

type nativeFlowQueryShape struct {
	leadingFields  []string
	repeatedStates bool
}

func buildFlowClaimDueAnyNative(args []any) (nativeCommand, bool, error) {
	if command, ok, err := buildFlowClaimDueNative(args); ok || err != nil {
		return command, ok, err
	}
	return buildFlowQueryNative(
		"FLOW.CLAIM_DUE", nativeOpFlowClaimDue, args,
		nativeFlowQueryShape{leadingFields: []string{"type"}, repeatedStates: true},
	)
}

func buildFlowQueryNative(
	name string,
	opcode uint16,
	args []any,
	shape nativeFlowQueryShape,
) (nativeCommand, bool, error) {
	if len(args) < len(shape.leadingFields) {
		return nativeCommand{}, true, errors.New(name + " missing required arguments")
	}
	payload := make(map[string]any, len(shape.leadingFields)+len(args)/2)
	for index, field := range shape.leadingFields {
		payload[field] = args[index]
	}
	ok, err := appendFlowQueryOptions(payload, args[len(shape.leadingFields):], shape)
	if err != nil {
		return nativeCommand{}, true, err
	}
	if !ok {
		return nativeCommand{}, false, nil
	}
	return nativeCommand{name: name, opcode: opcode, laneID: 1, payload: payload}, true, nil
}

func appendFlowQueryOptions(payload map[string]any, args []any, shape nativeFlowQueryShape) (bool, error) {
	for index := 0; index < len(args); {
		token := strings.ToUpper(asString(args[index]))
		switch token {
		case "NOPAYLOAD":
			payload["payload"] = false
			index++
			continue
		case "PAYLOAD":
			payload["payload"] = true
			index++
			continue
		case "VALUE":
			if index+1 >= len(args) {
				return false, errors.New("FLOW read VALUE requires a name")
			}
			payload["values"] = appendNativeListValue(payload["values"], asString(args[index+1]))
			index += 2
			continue
		case "PARTITIONS":
			if index+1 >= len(args) {
				return false, errors.New("FLOW read PARTITIONS requires a count")
			}
			count, err := responseInt64(args[index+1], nil)
			if err != nil || count <= 0 || count > int64(len(args)-index-2) {
				return false, errors.New("FLOW read PARTITIONS has invalid count")
			}
			partitions := make([]string, int(count))
			for item := range partitions {
				partitions[item] = asString(args[index+2+item])
			}
			payload["partition_keys"] = partitions
			index += int(count) + 2
			continue
		case "STATE":
			if shape.repeatedStates {
				if index+1 >= len(args) {
					return false, errors.New("FLOW read STATE requires a value")
				}
				payload["states"] = appendNativeListValue(payload["states"], asString(args[index+1]))
				index += 2
				continue
			}
		case "ATTRIBUTE":
			if index+2 >= len(args) {
				return false, errors.New("FLOW read ATTRIBUTE requires name and value")
			}
			putNativeMapValue(payload, "attributes", asString(args[index+1]), args[index+2])
			index += 3
			continue
		}

		if index+1 >= len(args) {
			return false, fmt.Errorf("FLOW read option %s requires a value", token)
		}
		field, ok := flowAdminNativeField(token)
		if !ok {
			return false, nil
		}
		value, ok := flowAdminNativeValue(field, args[index+1])
		if !ok {
			return false, nil
		}
		payload[field] = value
		index += 2
	}
	return true, nil
}
