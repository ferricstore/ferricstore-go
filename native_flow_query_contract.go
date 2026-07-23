package ferricstore

import (
	"errors"
	"fmt"
)

var requiredNativeFlowQueryCapabilities = []string{
	"flow_query_v1",
	"flow_explain_v1",
	"flow_explain_analyze_v1",
	"flow_composite_index_v1",
	"flow_query_index_status_v1",
}

var requiredNativeFlowQueryShapes = []string{
	"runs_by_run_id_record",
	"runs_by_partition_and_run_id_record",
	"runs_by_partition_predicates_ordered_records",
	"runs_by_partition_type_state_ordered_records",
	"runs_by_partition_type_terminals_ordered_records",
	"runs_by_partition_metadata_ordered_records",
	"runs_by_partition_type_running_lease_deadline_ordered_records",
	"runs_by_partition_parent_ordered_records",
	"runs_by_partition_root_ordered_records",
	"runs_by_partition_correlation_ordered_records",
	"runs_by_partition_predicates_count",
	"events_by_run_id_ordered_records",
}

type nativeFlowQueryContract struct {
	RequestContract     string
	ResultContract      string
	ExplainContract     string
	IndexStatusContract string
	capabilities        map[string]struct{}
	languages           map[string]struct{}
	shapes              map[string]struct{}
}

func (c nativeFlowQueryContract) supportsCapability(name string) bool {
	_, ok := c.capabilities[name]
	return ok
}

func (c nativeFlowQueryContract) supportsShape(name string) bool {
	_, ok := c.shapes[name]
	return ok
}

func parseNativeFlowQueryContract(capabilities map[string]any) (nativeFlowQueryContract, error) {
	manifest, err := requiredNativeCapabilityMap(capabilities, "flow_query")
	if err != nil {
		return nativeFlowQueryContract{}, fmt.Errorf("ferricstore native HELLO flow_query: %w", err)
	}
	requestContract, err := requiredNativeFlowQueryString(manifest, "request_contract")
	if err != nil || requestContract != flowQueryRequestContract {
		return nativeFlowQueryContract{}, fmt.Errorf("ferricstore native HELLO flow_query request_contract must be %q", flowQueryRequestContract)
	}
	resultContract, err := requiredNativeFlowQueryString(manifest, "result_contract")
	if err != nil || resultContract != flowQueryResultContract {
		return nativeFlowQueryContract{}, fmt.Errorf("ferricstore native HELLO flow_query result_contract must be %q", flowQueryResultContract)
	}
	explainContract, err := requiredNativeFlowQueryString(manifest, "explain_contract")
	if err != nil || explainContract != flowExplainContract {
		return nativeFlowQueryContract{}, fmt.Errorf("ferricstore native HELLO flow_query explain_contract must be %q", flowExplainContract)
	}
	indexStatusContract, err := requiredNativeFlowQueryString(manifest, "index_status_contract")
	if err != nil || indexStatusContract != flowQueryIndexesContract {
		return nativeFlowQueryContract{}, fmt.Errorf("ferricstore native HELLO flow_query index_status_contract must be %q", flowQueryIndexesContract)
	}
	queryCapabilities, err := nativeFlowQueryStringSet(manifest["capabilities"], "capabilities", 64)
	if err != nil {
		return nativeFlowQueryContract{}, err
	}
	languages, err := nativeFlowQueryStringSet(manifest["language_versions"], "language_versions", 16)
	if err != nil {
		return nativeFlowQueryContract{}, err
	}
	shapes, err := nativeFlowQueryStringSet(manifest["shapes"], "shapes", 128)
	if err != nil {
		return nativeFlowQueryContract{}, err
	}
	if _, supported := languages[flowQueryLanguageVersion]; !supported {
		return nativeFlowQueryContract{}, fmt.Errorf("ferricstore native HELLO flow_query does not support %s", flowQueryLanguageVersion)
	}
	for _, capability := range requiredNativeFlowQueryCapabilities {
		if _, supported := queryCapabilities[capability]; !supported {
			return nativeFlowQueryContract{}, fmt.Errorf("ferricstore native HELLO flow_query missing capability %q", capability)
		}
	}
	for _, shape := range requiredNativeFlowQueryShapes {
		if _, supported := shapes[shape]; !supported {
			return nativeFlowQueryContract{}, fmt.Errorf("ferricstore native HELLO flow_query missing shape %q", shape)
		}
	}
	if err := validateNativeFlowQuerySchema(capabilities); err != nil {
		return nativeFlowQueryContract{}, err
	}
	return nativeFlowQueryContract{
		RequestContract:     requestContract,
		ResultContract:      resultContract,
		ExplainContract:     explainContract,
		IndexStatusContract: indexStatusContract,
		capabilities:        queryCapabilities,
		languages:           languages,
		shapes:              shapes,
	}, nil
}

func requiredNativeFlowQueryString(mapping map[string]any, key string) (string, error) {
	value, present := mapping[key]
	if !present {
		return "", fmt.Errorf("ferricstore native HELLO flow_query missing %s", key)
	}
	text, ok := commandText(value)
	if !ok || text == "" || len(text) > 256 {
		return "", fmt.Errorf("ferricstore native HELLO flow_query %s must be a non-empty string", key)
	}
	return text, nil
}

func nativeFlowQueryStringSet(value any, field string, maximum int) (map[string]struct{}, error) {
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("ferricstore native HELLO flow_query %s must be an array", field)
	}
	if len(items) == 0 || len(items) > maximum {
		return nil, fmt.Errorf("ferricstore native HELLO flow_query %s must contain 1..%d entries", field, maximum)
	}
	values := make(map[string]struct{}, len(items))
	for _, item := range items {
		text, ok := commandText(item)
		if !ok || text == "" || len(text) > 256 {
			return nil, fmt.Errorf("ferricstore native HELLO flow_query %s must contain bounded non-empty strings", field)
		}
		if _, duplicate := values[text]; duplicate {
			return nil, fmt.Errorf("ferricstore native HELLO flow_query %s contains duplicate %q", field, text)
		}
		values[text] = struct{}{}
	}
	return values, nil
}

func validateNativeFlowQuerySchema(capabilities map[string]any) error {
	schemas, err := requiredNativeCapabilityMap(capabilities, "schemas")
	if err != nil {
		return err
	}
	querySchema, err := requiredNativeCapabilityMap(schemas, "FLOW.QUERY")
	if err != nil {
		return fmt.Errorf("ferricstore native HELLO flow_query schema: %w", err)
	}
	fields, err := nativeSchemaFields(querySchema["fields"])
	if err != nil {
		return fmt.Errorf("ferricstore native HELLO flow_query fields: %w", err)
	}
	for _, required := range []string{"version", "query", "params", "deadline_ms"} {
		if _, present := fields[required]; !present {
			return fmt.Errorf("ferricstore native HELLO flow_query schema missing field %q", required)
		}
	}
	required, err := nativeSchemaFields(querySchema["required"])
	if err != nil {
		return fmt.Errorf("ferricstore native HELLO flow_query required fields: %w", err)
	}
	for _, field := range []string{"version", "query"} {
		if _, present := required[field]; !present {
			return fmt.Errorf("ferricstore native HELLO flow_query schema must require %q", field)
		}
	}
	if len(required) != 2 {
		return errors.New("ferricstore native HELLO flow_query schema has unexpected required fields")
	}
	return nil
}
