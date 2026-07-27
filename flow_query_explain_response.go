package ferricstore

import (
	"errors"
	"fmt"
)

func decodeFlowExplainResult(value any) (*FlowExplainResult, error) {
	mapping, err := flowQueryResponseMap(value)
	if err != nil {
		return nil, fmt.Errorf("decode FLOW.QUERY explain: %w", err)
	}
	version, err := requiredFlowQueryStringField(mapping, "version", "FLOW.QUERY explain")
	if err != nil || version != flowExplainContract {
		return nil, fmt.Errorf("decode FLOW.QUERY explain: unsupported contract %q", version)
	}
	fingerprint, err := requiredFlowQueryStringField(mapping, "query_fingerprint", "FLOW.QUERY explain")
	if err != nil || !validFlowQueryFingerprint(fingerprint) {
		if err == nil {
			err = errors.New("query_fingerprint must be 64 hexadecimal characters")
		}
		return nil, err
	}
	status, err := requiredFlowQueryStringField(mapping, "status", "FLOW.QUERY explain")
	if err != nil {
		return nil, err
	}
	if status != "planned" && status != "rejected" && status != "executed" {
		return nil, fmt.Errorf("decode FLOW.QUERY explain: unsupported status %q", status)
	}
	plan, err := requiredFlowQueryMap(mapping, "plan", "FLOW.QUERY explain")
	if err != nil {
		return nil, err
	}
	estimate, err := requiredFlowQueryMap(mapping, "estimate", "FLOW.QUERY explain")
	if err != nil {
		return nil, err
	}
	bounds, err := requiredFlowQueryMap(mapping, "bounds", "FLOW.QUERY explain")
	if err != nil {
		return nil, err
	}
	capabilities, err := decodeFlowExplainCapabilities(mapping)
	if err != nil {
		return nil, err
	}
	extendedFields := []string{"stats", "quality", "pressure", "decision", "alternatives"}
	hasExtended := false
	hasAllExtended := true
	for _, field := range extendedFields {
		_, present := mapping[field]
		hasExtended = hasExtended || present
		hasAllExtended = hasAllExtended && present
	}
	specialized := capabilities != nil && !hasExtended
	var stats map[string]any
	var quality *FlowQueryQuality
	var pressure map[string]any
	var decision map[string]any
	alternatives := make([]map[string]any, 0)
	if specialized {
		if status != "planned" {
			return nil, errors.New("decode FLOW.QUERY explain: specialized explain must be planned")
		}
		if _, present := mapping["actual"]; present {
			return nil, errors.New("decode FLOW.QUERY explain: specialized explain contains extended status fields")
		}
		if _, present := mapping["diagnostic"]; present {
			return nil, errors.New("decode FLOW.QUERY explain: specialized explain contains extended status fields")
		}
	} else {
		_, hasActual := mapping["actual"]
		_, hasDiagnostic := mapping["diagnostic"]
		if !hasAllExtended || !hasActual || !hasDiagnostic {
			return nil, errors.New("decode FLOW.QUERY explain: missing required v1 fields")
		}
		stats, err = requiredFlowQueryMap(mapping, "stats", "FLOW.QUERY explain")
		if err != nil {
			return nil, err
		}
		decodedQuality, qualityErr := decodeFlowQueryQuality(mapping["quality"])
		if qualityErr != nil {
			return nil, qualityErr
		}
		quality = &decodedQuality
		pressure, err = requiredFlowQueryMap(mapping, "pressure", "FLOW.QUERY explain")
		if err != nil {
			return nil, err
		}
		decision, err = requiredFlowQueryMap(mapping, "decision", "FLOW.QUERY explain")
		if err != nil {
			return nil, err
		}
		alternatives, err = decodeFlowExplainAlternatives(mapping["alternatives"])
		if err != nil {
			return nil, err
		}
	}
	result := &FlowExplainResult{
		Version: version, QueryFingerprint: fingerprint, Status: status, Plan: plan,
		Estimate: estimate, Stats: stats, Quality: quality, Bounds: bounds,
		Pressure: pressure, Decision: decision, Alternatives: alternatives,
		Capabilities: capabilities, Raw: mapping,
	}
	if actual, present := mapping["actual"]; present && actual != nil {
		if status != "executed" {
			return nil, fmt.Errorf("decode FLOW.QUERY explain: status %q contains actual usage", status)
		}
		usage, err := decodeFlowQueryUsage(actual)
		if err != nil {
			return nil, err
		}
		result.Actual = &usage
	}
	if status == "executed" && result.Actual == nil {
		return nil, errors.New("decode FLOW.QUERY explain: executed result is missing actual usage")
	}
	if diagnostic, present := mapping["diagnostic"]; present && diagnostic != nil {
		if status != "rejected" {
			return nil, fmt.Errorf("decode FLOW.QUERY explain: status %q contains a diagnostic", status)
		}
		result.Diagnostic, err = decodeFlowQueryErrorPayload(diagnostic, nil)
		if err != nil {
			return nil, fmt.Errorf("decode FLOW.QUERY explain diagnostic: %w", err)
		}
	}
	if status == "rejected" && result.Diagnostic == nil {
		return nil, errors.New("decode FLOW.QUERY explain: rejected result is missing its diagnostic")
	}
	return result, nil
}

func decodeFlowExplainCapabilities(mapping map[string]any) (*FlowExplainCapabilities, error) {
	value, present := mapping["capabilities"]
	if !present {
		return nil, nil
	}
	capabilities, err := nativeMap(value)
	if err != nil {
		return nil, fmt.Errorf("decode FLOW.QUERY explain capabilities: %w", err)
	}
	requested, err := decodeFlowExplainCapabilityList(capabilities["requested"], "requested")
	if err != nil {
		return nil, err
	}
	available, err := decodeFlowExplainCapabilityList(capabilities["available"], "available")
	if err != nil {
		return nil, err
	}
	missing, err := decodeFlowExplainCapabilityList(capabilities["missing"], "missing")
	if err != nil {
		return nil, err
	}
	return &FlowExplainCapabilities{
		Requested: requested, Available: available, Missing: missing, Raw: capabilities,
	}, nil
}

func decodeFlowExplainCapabilityList(value any, field string) ([]string, error) {
	items, ok := value.([]any)
	if !ok || len(items) > 64 {
		return nil, fmt.Errorf("decode FLOW.QUERY explain capabilities %s: expected at most 64 strings", field)
	}
	values := make([]string, len(items))
	seen := make(map[string]struct{}, len(items))
	for position, item := range items {
		text, err := flowQueryResponseString(item, "FLOW.QUERY explain capability")
		if err != nil || text == "" || len(text) > 128 {
			return nil, fmt.Errorf("decode FLOW.QUERY explain capabilities %s item %d: invalid text", field, position)
		}
		if _, duplicate := seen[text]; duplicate {
			return nil, fmt.Errorf("decode FLOW.QUERY explain capabilities %s: duplicate value", field)
		}
		seen[text] = struct{}{}
		values[position] = text
	}
	return values, nil
}

func decodeFlowExplainAlternatives(value any) ([]map[string]any, error) {
	items, ok := value.([]any)
	if !ok || len(items) > 31 {
		return nil, errors.New("decode FLOW.QUERY explain: alternatives must contain at most 31 maps")
	}
	alternatives := make([]map[string]any, len(items))
	for index, item := range items {
		mapping, err := nativeMap(item)
		if err != nil {
			return nil, fmt.Errorf("decode FLOW.QUERY explain alternative %d: %w", index, err)
		}
		alternatives[index] = mapping
	}
	return alternatives, nil
}

func validFlowQueryFingerprint(value string) bool {
	if len(value) != 64 {
		return false
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if (character < '0' || character > '9') &&
			(character < 'a' || character > 'f') &&
			(character < 'A' || character > 'F') {
			return false
		}
	}
	return true
}
