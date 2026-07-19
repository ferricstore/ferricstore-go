package ferricstore

import (
	"errors"
	"fmt"
)

const nativeMaxSchemaFields = 256

func validateNativeFlowPolicyCapabilities(capabilities map[string]any) error {
	schemas, err := requiredNativeCapabilityMap(capabilities, "schemas")
	if err != nil {
		return err
	}
	policySchema, err := requiredNativeCapabilityMap(schemas, "FLOW.POLICY.SET")
	if err != nil {
		return fmt.Errorf("ferricstore native HELLO policy schema: %w", err)
	}
	fields, err := nativeSchemaFields(policySchema["fields"])
	if err != nil {
		return fmt.Errorf("ferricstore native HELLO FLOW.POLICY.SET fields: %w", err)
	}
	for _, required := range []string{"replace", "expected_generation"} {
		if _, exists := fields[required]; !exists {
			return fmt.Errorf(
				"ferricstore native HELLO FLOW.POLICY.SET missing %s capability",
				required,
			)
		}
	}
	return nil
}

func nativeSchemaFields(value any) (map[string]struct{}, error) {
	var values []any
	switch typed := value.(type) {
	case []any:
		values = typed
	case []string:
		values = make([]any, len(typed))
		for index, field := range typed {
			values[index] = field
		}
	default:
		return nil, errors.New("must be an array")
	}
	if len(values) > nativeMaxSchemaFields {
		return nil, fmt.Errorf("advertises too many fields: %d", len(values))
	}
	fields := make(map[string]struct{}, len(values))
	for _, value := range values {
		field, ok := commandText(value)
		if !ok || field == "" {
			return nil, errors.New("must contain non-empty strings")
		}
		if _, duplicate := fields[field]; duplicate {
			return nil, fmt.Errorf("contains duplicate field %q", field)
		}
		fields[field] = struct{}{}
	}
	return fields, nil
}
