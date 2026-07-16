package ferricstore

import (
	"errors"
	"reflect"
	"sort"
	"strings"
)

func scheduleExtraOptionArgs(options map[string]any) ([]any, error) {
	if len(options) == 0 {
		return nil, nil
	}
	values := make(map[string]any, len(options))
	for rawKey, value := range options {
		key := strings.ToUpper(strings.TrimSpace(rawKey))
		if key == "" {
			return nil, errors.New("schedule extra option name must be non-empty")
		}
		if scheduleOptionHasTypedField(key) {
			return nil, errors.New("schedule extra option " + key + " conflicts with a typed ScheduleOptions field")
		}
		if _, duplicate := values[key]; duplicate {
			return nil, errors.New("schedule extra option " + key + " is duplicated after normalization")
		}
		if key == "DEADLINE_MS" {
			if err := validateNonNegativeAnyInt64("deadline_ms", value); err != nil {
				return nil, err
			}
		}
		values[key] = value
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	args := make([]any, 0, len(keys)*2)
	for _, key := range keys {
		args = append(args, key, values[key])
	}
	return args, nil
}

func scheduleOptionHasTypedField(key string) bool {
	switch key {
	case "KIND", "AT_MS", "DELAY_MS", "START_AT_MS", "EVERY_MS", "CRON", "TIMEZONE", "TARGET",
		"OVERLAP_POLICY", "OVERLAP_RETRY_MS", "MAX_FIRES", "END_AT_MS", "OVERWRITE", "NOW":
		return true
	default:
		return false
	}
}

func appendAttributes(args *[]any, attributes, attributesMerge map[string]any, attributesDelete []string) {
	for name, value := range attributes {
		*args = append(*args, "ATTRIBUTE", canonicalFlowMetadataKey(name), value)
	}
	for name, value := range attributesMerge {
		*args = append(*args, "ATTRIBUTE_MERGE", canonicalFlowMetadataKey(name), value)
	}
	for _, name := range attributesDelete {
		*args = append(*args, "ATTRIBUTE_DELETE", canonicalFlowMetadataKey(name))
	}
}

func appendStateMeta(args *[]any, stateMeta map[string]any) {
	for name, value := range stateMeta {
		*args = append(*args, "STATE_META", canonicalFlowMetadataKey(name), value)
	}
}

func appendSearchStateMeta(args *[]any, stateMeta map[string]map[string]any) {
	for state, meta := range stateMeta {
		for name, value := range meta {
			*args = append(*args, "STATE_META", strings.TrimSpace(state), canonicalFlowMetadataKey(name), value)
		}
	}
}

func sharedCreateManyAttributes(items []CreateItem, attributes map[string]any) (map[string]any, error) {
	var first map[string]any
	for _, item := range items {
		if len(item.Attributes) == 0 {
			continue
		}
		if first == nil {
			first = item.Attributes
			continue
		}
		if !reflect.DeepEqual(first, item.Attributes) {
			return nil, errors.New("create_many supports shared attributes only; use CreateManyOptions.Attributes or separate Create calls for per-item attributes")
		}
	}
	if first == nil {
		return attributes, nil
	}
	if attributes != nil && !reflect.DeepEqual(attributes, first) {
		return nil, errors.New("create_many item attributes must match shared attributes when both are provided")
	}
	return first, nil
}

func sharedCreateManyStateMeta(items []CreateItem, stateMeta map[string]any) (map[string]any, error) {
	var first map[string]any
	for _, item := range items {
		if len(item.StateMeta) == 0 {
			continue
		}
		if first == nil {
			first = item.StateMeta
			continue
		}
		if !reflect.DeepEqual(first, item.StateMeta) {
			return nil, errors.New("create_many supports shared state_meta only; use CreateManyOptions.StateMeta or separate Create calls for per-item state_meta")
		}
	}
	if first == nil {
		return stateMeta, nil
	}
	if stateMeta != nil && !reflect.DeepEqual(stateMeta, first) {
		return nil, errors.New("create_many item state_meta must match shared state_meta when both are provided")
	}
	return first, nil
}

func boolDefault(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}
