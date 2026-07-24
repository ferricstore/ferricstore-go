package ferricstore

import (
	"errors"
	"fmt"
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
		indexes[index] = FlowQueryIndex{ID: id, Version: version, BuildID: buildID, State: state, Queryable: queryable, Raw: entry}
	}
	return &FlowQueryIndexStatus{
		ContractVersion: contract, ObservedAtMS: observed, StatisticsMaxAgeMS: maxAge,
		Registry: FlowQueryIndexRegistry{Epoch: epoch, CatalogVersion: catalogVersion},
		Services: services, Indexes: indexes, Raw: mapping,
	}, nil
}

func unsignedResponseInteger(value any, context string) (uint64, error) {
	parsed, ok := flowQueryUint64(value)
	if !ok {
		return 0, fmt.Errorf("decode %s: expected an unsigned integer", context)
	}
	return parsed, nil
}
