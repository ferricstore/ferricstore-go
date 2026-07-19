package ferricstore

import (
	"errors"
	"fmt"
	"strings"
)

type nativeFlowManyItemKind uint8

const (
	nativeFlowManyClaimed nativeFlowManyItemKind = iota + 1
	nativeFlowManyTransition
	nativeFlowManyCancel
)

func buildFlowCompleteManyAnyNative(args []any) (nativeCommand, bool, error) {
	if command, ok, err := buildFlowCompleteManyNative(args); ok || err != nil {
		return command, ok, err
	}
	return buildFlowMutationManyNative(
		"FLOW.COMPLETE_MANY", nativeOpFlowCompleteMany, args, nativeFlowManyClaimed,
	)
}

func buildFlowMutationManyNative(
	name string,
	opcode uint16,
	args []any,
	kind nativeFlowManyItemKind,
) (nativeCommand, bool, error) {
	leading := 1
	if kind == nativeFlowManyTransition {
		leading = 3
	}
	if len(args) < leading {
		return nativeCommand{}, true, errors.New(name + " missing required arguments")
	}

	wirePartition := asString(args[0])
	payload := map[string]any{}
	if !strings.EqualFold(wirePartition, "MIXED") {
		payload["partition_key"] = args[0]
	}
	if kind == nativeFlowManyTransition {
		payload["from_state"] = args[1]
		payload["to_state"] = args[2]
	}

	remaining := args[leading:]
	marker, token, ok := flowItemMarker(remaining)
	if !ok || token != "ITEMS" {
		return nativeCommand{}, false, nil
	}
	optionsOK, err := appendFlowAdminOptions(payload, remaining[:marker])
	if err != nil {
		return nativeCommand{}, true, err
	}
	if !optionsOK {
		return nativeCommand{}, false, nil
	}
	items, err := parseFlowMutationManyItems(
		name, remaining[marker+1:], strings.EqualFold(wirePartition, "MIXED"), kind,
	)
	if err != nil {
		return nativeCommand{}, true, err
	}
	payload["items"] = items
	return nativeCommand{name: name, opcode: opcode, laneID: 1, payload: payload}, true, nil
}

func parseFlowMutationManyItems(
	name string,
	values []any,
	mixed bool,
	kind nativeFlowManyItemKind,
) ([]any, error) {
	width := 3
	if kind == nativeFlowManyCancel {
		width = 2
	}
	if mixed {
		width++
	}
	if len(values) == 0 || len(values)%width != 0 {
		return nil, fmt.Errorf("%s has invalid item fields", name)
	}

	items := make([]any, 0, len(values)/width)
	for offset := 0; offset < len(values); offset += width {
		item := map[string]any{"id": values[offset]}
		field := offset + 1
		if mixed {
			item["partition_key"] = values[field]
			field++
		}
		switch kind {
		case nativeFlowManyClaimed:
			item["lease_token"] = values[field]
			fencing, err := responseInt64(values[field+1], nil)
			if err != nil {
				return nil, fmt.Errorf("%s item fencing token: %w", name, err)
			}
			item["fencing_token"] = fencing
		case nativeFlowManyTransition:
			fencing, err := responseInt64(values[field], nil)
			if err != nil {
				return nil, fmt.Errorf("%s item fencing token: %w", name, err)
			}
			item["fencing_token"] = fencing
			item["lease_token"] = values[field+1]
		case nativeFlowManyCancel:
			fencing, err := responseInt64(values[field], nil)
			if err != nil {
				return nil, fmt.Errorf("%s item fencing token: %w", name, err)
			}
			item["fencing_token"] = fencing
		default:
			return nil, fmt.Errorf("%s has unsupported item schema", name)
		}
		items = append(items, item)
	}
	return items, nil
}
