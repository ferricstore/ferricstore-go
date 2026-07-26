package ferricstore

import (
	"errors"
	"fmt"
)

const (
	flowQueryMaxCoveringFields = 32
	flowQueryMaxFieldNameBytes = 512
	flowQueryMaxCodecNameBytes = 128
)

func decodeFlowQueryIndexStatus(value any) (*FlowQueryIndexStatus, error) {
	mapping, err := nativeMap(value)
	if err != nil {
		return nil, fmt.Errorf("decode FLOW.QUERY.INDEXES: %w", err)
	}
	contract, err := requiredFlowQueryStringField(mapping, "contract_version", "FLOW.QUERY.INDEXES")
	if err != nil || contract != flowQueryIndexesContract {
		return nil, fmt.Errorf("decode FLOW.QUERY.INDEXES: unsupported contract %q", contract)
	}
	observed, err := nonNegativeResponseInteger(mapping["observed_at_ms"], "FLOW.QUERY.INDEXES observed_at_ms")
	if err != nil {
		return nil, err
	}
	maxAge, err := nonNegativeResponseInteger(mapping["statistics_max_age_ms"], "FLOW.QUERY.INDEXES statistics_max_age_ms")
	if err != nil {
		return nil, err
	}
	registry, err := requiredFlowQueryMap(mapping, "registry", "FLOW.QUERY.INDEXES")
	if err != nil {
		return nil, err
	}
	epoch, err := unsignedResponseInteger(registry["epoch"], "FLOW.QUERY.INDEXES registry epoch")
	if err != nil {
		return nil, err
	}
	catalogVersion, err := unsignedResponseInteger(registry["catalog_version"], "FLOW.QUERY.INDEXES catalog version")
	if err != nil || catalogVersion == 0 {
		return nil, errors.New("decode FLOW.QUERY.INDEXES: catalog_version must be positive")
	}
	services, err := requiredFlowQueryMap(mapping, "services", "FLOW.QUERY.INDEXES")
	if err != nil {
		return nil, err
	}
	rawIndexes, ok := mapping["indexes"].([]any)
	if !ok {
		return nil, errors.New("decode FLOW.QUERY.INDEXES: indexes must be an array")
	}
	if len(rawIndexes) > flowQueryMaxIndexEntries {
		return nil, fmt.Errorf("decode FLOW.QUERY.INDEXES: indexes must contain at most %d entries", flowQueryMaxIndexEntries)
	}
	indexes := make([]FlowQueryIndex, len(rawIndexes))
	for index, raw := range rawIndexes {
		entry, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("decode FLOW.QUERY.INDEXES index %d: expected map", index)
		}
		id, fieldErr := requiredFlowQueryStringField(entry, "id", "FLOW.QUERY.INDEXES index")
		if fieldErr != nil {
			return nil, fieldErr
		}
		version, fieldErr := unsignedResponseInteger(entry["version"], "FLOW.QUERY.INDEXES index version")
		if fieldErr != nil || version == 0 {
			return nil, fmt.Errorf("decode FLOW.QUERY.INDEXES index %q: version must be positive", id)
		}
		buildID, fieldErr := requiredFlowQueryStringField(entry, "build_id", "FLOW.QUERY.INDEXES index")
		if fieldErr != nil {
			return nil, fieldErr
		}
		state, fieldErr := requiredFlowQueryStringField(entry, "state", "FLOW.QUERY.INDEXES index")
		if fieldErr != nil {
			return nil, fieldErr
		}
		queryable, ok := entry["queryable"].(bool)
		if !ok {
			return nil, fmt.Errorf("decode FLOW.QUERY.INDEXES index %q: queryable must be boolean", id)
		}
		coveringFields, fieldErr := decodeFlowQueryCoveringFields(entry, id)
		if fieldErr != nil {
			return nil, fieldErr
		}
		format, fieldErr := decodeFlowQueryIndexFormat(entry, id)
		if fieldErr != nil {
			return nil, fieldErr
		}
		indexes[index] = FlowQueryIndex{
			ID: id, Version: version, BuildID: buildID, State: state, Queryable: queryable,
			CoveringFields: coveringFields, Format: format, Raw: entry,
		}
	}
	return &FlowQueryIndexStatus{
		ContractVersion: contract, ObservedAtMS: observed, StatisticsMaxAgeMS: maxAge,
		Registry: FlowQueryIndexRegistry{Epoch: epoch, CatalogVersion: catalogVersion},
		Services: services, Indexes: indexes, Raw: mapping,
	}, nil
}

func decodeFlowQueryCoveringFields(entry map[string]any, id string) ([]string, error) {
	raw, ok := entry["covering_fields"].([]any)
	if !ok || len(raw) > flowQueryMaxCoveringFields {
		return nil, fmt.Errorf(
			"decode FLOW.QUERY.INDEXES index %q: covering_fields must contain at most %d entries",
			id, flowQueryMaxCoveringFields,
		)
	}
	fields := make([]string, len(raw))
	seen := make(map[string]struct{}, len(raw))
	for position, value := range raw {
		field, err := flowQueryResponseString(value, "FLOW.QUERY.INDEXES covering field")
		if err != nil || field == "" || len(field) > flowQueryMaxFieldNameBytes {
			return nil, fmt.Errorf("decode FLOW.QUERY.INDEXES index %q: covering_fields entry %d is invalid", id, position)
		}
		if _, duplicate := seen[field]; duplicate {
			return nil, fmt.Errorf("decode FLOW.QUERY.INDEXES index %q: covering_fields contains duplicate %q", id, field)
		}
		seen[field] = struct{}{}
		fields[position] = field
	}
	return fields, nil
}

func decodeFlowQueryIndexFormat(entry map[string]any, id string) (FlowQueryIndexFormat, error) {
	mapping, err := requiredFlowQueryMap(entry, "format", "FLOW.QUERY.INDEXES index")
	if err != nil {
		return FlowQueryIndexFormat{}, err
	}
	values := make([]string, 4)
	for position, field := range []string{"query_row", "key", "entry", "reverse"} {
		value, fieldErr := requiredFlowQueryStringField(mapping, field, "FLOW.QUERY.INDEXES index format")
		if fieldErr != nil || len(value) > flowQueryMaxCodecNameBytes {
			return FlowQueryIndexFormat{}, fmt.Errorf("decode FLOW.QUERY.INDEXES index %q: format %s is invalid", id, field)
		}
		values[position] = value
	}
	rawCounter, present := mapping["counter"]
	if !present {
		return FlowQueryIndexFormat{}, fmt.Errorf("decode FLOW.QUERY.INDEXES index %q: format counter is missing", id)
	}
	counter := ""
	if rawCounter != nil {
		counter, err = flowQueryResponseString(rawCounter, "FLOW.QUERY.INDEXES index format counter")
		if err != nil || counter == "" || len(counter) > flowQueryMaxCodecNameBytes {
			return FlowQueryIndexFormat{}, fmt.Errorf("decode FLOW.QUERY.INDEXES index %q: format counter is invalid", id)
		}
	}
	return FlowQueryIndexFormat{
		QueryRow: values[0], Key: values[1], Entry: values[2], Reverse: values[3], Counter: counter,
	}, nil
}

func unsignedResponseInteger(value any, context string) (uint64, error) {
	parsed, ok := flowQueryUint64(value)
	if !ok {
		return 0, fmt.Errorf("decode %s: expected an unsigned integer", context)
	}
	return parsed, nil
}
