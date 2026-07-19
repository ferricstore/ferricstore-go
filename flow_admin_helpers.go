package ferricstore

import "strings"

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

func appendGovernanceScopeFilter(args *[]any, scope, partitionKey string) {
	if scope != "" {
		appendOpt(args, "SCOPE", scope)
		return
	}
	appendOpt(args, "PARTITION", partitionKey)
}

func boolDefault(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}
