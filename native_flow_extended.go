package ferricstore

import (
	"errors"
	"fmt"
	"math"
	"strings"
)

func buildFlowCreateManyExtendedNative(args []any) (nativeCommand, bool, error) {
	if len(args) < 2 {
		return nativeCommand{}, true, errors.New("FLOW.CREATE_MANY requires partition and items")
	}
	marker, token, ok := flowItemMarker(args[1:])
	if !ok || token != "ITEMS_EXT" {
		return nativeCommand{}, false, nil
	}
	payload := map[string]any{}
	optionsOK, err := appendFlowAdminOptions(payload, args[1:1+marker])
	if err != nil {
		return nativeCommand{}, true, err
	}
	if !optionsOK {
		return nativeCommand{}, false, nil
	}
	itemArgs := args[1+marker+1:]
	items, err := parseFlowCreateManyExtendedItems(itemArgs, strings.EqualFold(asString(args[0]), "MIXED"))
	if err != nil {
		return nativeCommand{}, true, err
	}
	payload["items"] = items
	wirePartition := asString(args[0])
	if !strings.EqualFold(wirePartition, "AUTO") &&
		!strings.EqualFold(wirePartition, "MIXED") &&
		!strings.EqualFold(wirePartition, "NONE") && wirePartition != "" {
		payload["partition_key"] = args[0]
	}
	return nativeCommand{
		name: "FLOW.CREATE_MANY", opcode: nativeOpFlowCreateMany, laneID: 1, payload: payload,
	}, true, nil
}

func buildFlowSpawnChildrenNative(args []any) (nativeCommand, bool, error) {
	if len(args) == 0 {
		return nativeCommand{}, true, errors.New("FLOW.SPAWN_CHILDREN requires parent id")
	}
	marker, token, ok := flowItemMarker(args[1:])
	if !ok {
		return nativeCommand{}, false, nil
	}
	payload := map[string]any{"id": args[0]}
	optionsOK, err := appendFlowAdminOptions(payload, args[1:1+marker])
	if err != nil {
		return nativeCommand{}, true, err
	}
	if !optionsOK {
		return nativeCommand{}, false, nil
	}
	itemArgs := args[1+marker+1:]
	var children []any
	if token == "ITEMS_EXT" {
		children, err = parseFlowSpawnExtendedItems(itemArgs)
	} else {
		children, err = parseFlowSpawnItems(itemArgs)
	}
	if err != nil {
		return nativeCommand{}, true, err
	}
	payload["children"] = children
	return nativeCommand{
		name: "FLOW.SPAWN_CHILDREN", opcode: nativeOpFlowSpawnChildren, laneID: 1, payload: payload,
	}, true, nil
}

// flowItemMarker follows the SDK's option grammar so an encoded value equal
// to ITEMS or ITEMS_EXT cannot be mistaken for the structural marker.
func flowItemMarker(args []any) (int, string, bool) {
	for index := 0; index < len(args); {
		token := strings.ToUpper(asString(args[index]))
		if token == "ITEMS" || token == "ITEMS_EXT" {
			return index, token, true
		}
		width := 2
		switch token {
		case "ATTRIBUTE", "ATTRIBUTE_MERGE", "STATE_META", "VALUE", "VALUE_REF":
			width = 3
		}
		if index > len(args)-width {
			return 0, "", false
		}
		index += width
	}
	return 0, "", false
}

func parseFlowCreateManyExtendedItems(args []any, mixed bool) ([]any, error) {
	count, values, err := flowExtendedItemCount("FLOW.CREATE_MANY", args)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, count)
	index := 0
	for itemIndex := 0; itemIndex < count; itemIndex++ {
		if len(values)-index < 3 {
			return nil, errors.New("FLOW.CREATE_MANY ITEMS_EXT item is truncated")
		}
		item := map[string]any{"id": values[index], "payload": values[index+2]}
		partition := asString(values[index+1])
		if mixed || partition != "-" {
			item["partition_key"] = values[index+1]
		}
		index += 3
		itemValues, next, err := parseFlowExtendedPairs("FLOW.CREATE_MANY VALUE", values, index)
		if err != nil {
			return nil, err
		}
		index = next
		if len(itemValues) > 0 {
			item["values"] = itemValues
		}
		refs, next, err := parseFlowExtendedPairs("FLOW.CREATE_MANY VALUE_REF", values, index)
		if err != nil {
			return nil, err
		}
		index = next
		if len(refs) > 0 {
			item["value_refs"] = refs
		}
		items = append(items, item)
	}
	if index != len(values) {
		return nil, errors.New("FLOW.CREATE_MANY ITEMS_EXT count does not match items")
	}
	return items, nil
}

func parseFlowSpawnExtendedItems(args []any) ([]any, error) {
	count, values, err := flowExtendedItemCount("FLOW.SPAWN_CHILDREN", args)
	if err != nil {
		return nil, err
	}
	children := make([]any, 0, count)
	index := 0
	for childIndex := 0; childIndex < count; childIndex++ {
		if len(values)-index < 4 {
			return nil, errors.New("FLOW.SPAWN_CHILDREN ITEMS_EXT child is truncated")
		}
		child := map[string]any{
			"id": values[index], "type": values[index+2], "payload": values[index+3],
		}
		if asString(values[index+1]) != "-" {
			child["partition_key"] = values[index+1]
		}
		index += 4
		itemValues, next, err := parseFlowExtendedPairs("FLOW.SPAWN_CHILDREN VALUE", values, index)
		if err != nil {
			return nil, err
		}
		index = next
		if len(itemValues) > 0 {
			child["values"] = itemValues
		}
		refs, next, err := parseFlowExtendedPairs("FLOW.SPAWN_CHILDREN VALUE_REF", values, index)
		if err != nil {
			return nil, err
		}
		index = next
		if len(refs) > 0 {
			child["value_refs"] = refs
		}
		children = append(children, child)
	}
	if index != len(values) {
		return nil, errors.New("FLOW.SPAWN_CHILDREN ITEMS_EXT count does not match children")
	}
	return children, nil
}

func parseFlowSpawnItems(args []any) ([]any, error) {
	mixed := len(args) > 0 && strings.EqualFold(asString(args[0]), "MIXED")
	if mixed {
		args = args[1:]
	}
	width := 3
	if mixed {
		width = 4
	}
	if len(args) == 0 || len(args)%width != 0 {
		return nil, errors.New("FLOW.SPAWN_CHILDREN ITEMS has invalid child width")
	}
	children := make([]any, 0, len(args)/width)
	for index := 0; index < len(args); index += width {
		child := map[string]any{"id": args[index]}
		offset := 1
		if mixed {
			child["partition_key"] = args[index+1]
			offset = 2
		}
		child["type"] = args[index+offset]
		child["payload"] = args[index+offset+1]
		children = append(children, child)
	}
	return children, nil
}

func flowExtendedItemCount(command string, args []any) (int, []any, error) {
	if len(args) == 0 {
		return 0, nil, fmt.Errorf("%s ITEMS_EXT requires item count", command)
	}
	count64, err := responseInt64(args[0], nil)
	if err != nil || count64 <= 0 || count64 > math.MaxInt {
		return 0, nil, fmt.Errorf("%s ITEMS_EXT count must be a positive integer", command)
	}
	return int(count64), args[1:], nil
}

func parseFlowExtendedPairs(command string, args []any, index int) (map[string]any, int, error) {
	if index >= len(args) {
		return nil, index, fmt.Errorf("%s count is missing", command)
	}
	count64, err := responseInt64(args[index], nil)
	if err != nil || count64 < 0 || count64 > math.MaxInt {
		return nil, index, fmt.Errorf("%s count must be non-negative", command)
	}
	count := int(count64)
	index++
	if count > (len(args)-index)/2 {
		return nil, index, fmt.Errorf("%s entries are truncated", command)
	}
	out := make(map[string]any, count)
	for pair := 0; pair < count; pair++ {
		name, err := responseString(args[index], nil)
		if err != nil || name == "" {
			return nil, index, fmt.Errorf("%s name must be a non-empty string", command)
		}
		out[name] = args[index+1]
		index += 2
	}
	return out, index, nil
}
