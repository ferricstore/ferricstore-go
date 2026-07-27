package ferricstore

import (
	"errors"
	"fmt"
)

const (
	flowQueryMaxCoveringFields = 32
	flowQueryMaxFieldNameBytes = 512
	flowQueryMaxCodecNameBytes = 128
	flowQueryMaxIndexFields    = 8
	flowQueryMaxWorkloads      = 16
)

var (
	flowQueryBuildPhases      = []string{"pending", "snapshot", "backfill", "done"}
	flowQueryValidationPhases = []string{"pending", "source", "index", "counter", "cleanup", "done"}
	flowQueryRetirementPhases = []string{"pending", "fence", "index", "counter", "reverse", "cleanup", "done"}
)

func decodeFlowQueryIndexStatus(value any, expectedID string) (*FlowQueryIndexStatus, error) {
	mapping, err := nativeMap(value)
	if err != nil {
		return nil, fmt.Errorf("decode FLOW.QUERY.INDEXES: %w", err)
	}
	contract, err := requiredFlowQueryStringField(mapping, "contract_version", "FLOW.QUERY.INDEXES")
	if err != nil || contract != flowQueryIndexesContract {
		return nil, fmt.Errorf("decode FLOW.QUERY.INDEXES: unsupported contract %q", contract)
	}
	observed, err := unsignedResponseInteger(mapping["observed_at_ms"], "FLOW.QUERY.INDEXES observed_at_ms")
	if err != nil {
		return nil, err
	}
	maxAge, err := unsignedResponseInteger(mapping["statistics_max_age_ms"], "FLOW.QUERY.INDEXES statistics_max_age_ms")
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
	services, err := decodeFlowQueryIndexServices(mapping["services"])
	if err != nil {
		return nil, err
	}
	rawIndexes, ok := mapping["indexes"].([]any)
	if !ok || len(rawIndexes) > flowQueryMaxIndexEntries {
		return nil, fmt.Errorf("decode FLOW.QUERY.INDEXES: indexes must contain at most %d entries", flowQueryMaxIndexEntries)
	}
	indexes := make([]FlowQueryIndex, len(rawIndexes))
	for position, raw := range rawIndexes {
		indexes[position], err = decodeFlowQueryIndex(raw, position)
		if err != nil {
			return nil, err
		}
	}
	status := &FlowQueryIndexStatus{
		ContractVersion: contract, ObservedAtMS: observed, StatisticsMaxAgeMS: maxAge,
		Registry: FlowQueryIndexRegistry{Epoch: epoch, CatalogVersion: catalogVersion},
		Services: services, Indexes: indexes, Raw: mapping,
	}
	if err := validateFlowQueryIndexContract(status, expectedID); err != nil {
		return nil, err
	}
	return status, nil
}

func decodeFlowQueryIndexServices(value any) (FlowQueryIndexServices, error) {
	mapping, err := nativeMap(value)
	if err != nil {
		return FlowQueryIndexServices{}, fmt.Errorf("decode FLOW.QUERY.INDEXES services: %w", err)
	}
	values := make([]string, 4)
	for position, field := range []string{"registry", "lifecycle_worker", "statistics_store", "statistics_worker"} {
		values[position], err = requiredFlowQueryChoice(mapping, field, "FLOW.QUERY.INDEXES services", "ready", "unavailable")
		if err != nil {
			return FlowQueryIndexServices{}, err
		}
	}
	return FlowQueryIndexServices{
		Registry: values[0], LifecycleWorker: values[1], StatisticsStore: values[2],
		StatisticsWorker: values[3], Raw: mapping,
	}, nil
}

func decodeFlowQueryIndex(value any, position int) (FlowQueryIndex, error) {
	entry, err := nativeMap(value)
	if err != nil {
		return FlowQueryIndex{}, fmt.Errorf("decode FLOW.QUERY.INDEXES index %d: %w", position, err)
	}
	id, err := requiredBoundedFlowQueryString(entry, "id", "FLOW.QUERY.INDEXES index", 64)
	if err != nil {
		return FlowQueryIndex{}, err
	}
	version, err := unsignedResponseInteger(entry["version"], "FLOW.QUERY.INDEXES index version")
	if err != nil || version == 0 {
		return FlowQueryIndex{}, fmt.Errorf("decode FLOW.QUERY.INDEXES index %q: version must be positive", id)
	}
	buildID, err := requiredBoundedFlowQueryString(entry, "build_id", "FLOW.QUERY.INDEXES index", 128)
	if err != nil {
		return FlowQueryIndex{}, err
	}
	source, err := requiredFlowQueryChoice(entry, "source", "FLOW.QUERY.INDEXES index", "runs")
	if err != nil {
		return FlowQueryIndex{}, err
	}
	state, err := requiredFlowQueryChoice(entry, "state", "FLOW.QUERY.INDEXES index", "building", "validating", "active", "retiring", "failed")
	if err != nil {
		return FlowQueryIndex{}, err
	}
	queryable, ok := entry["queryable"].(bool)
	if !ok {
		return FlowQueryIndex{}, fmt.Errorf("decode FLOW.QUERY.INDEXES index %q: queryable must be boolean", id)
	}
	fields, err := decodeFlowQueryIndexFields(entry["fields"])
	if err != nil {
		return FlowQueryIndex{}, err
	}
	workloads, err := decodeUniqueFlowQueryTextArray(entry["workloads"], "FLOW.QUERY.INDEXES index workloads", flowQueryMaxWorkloads, 64)
	if err != nil {
		return FlowQueryIndex{}, err
	}
	countPrefixes, err := decodeFlowQueryCountPrefixes(entry["count_prefixes"], len(fields))
	if err != nil {
		return FlowQueryIndex{}, err
	}
	coveringFields, err := decodeUniqueFlowQueryTextArray(entry["covering_fields"], "FLOW.QUERY.INDEXES index covering_fields", flowQueryMaxCoveringFields, flowQueryMaxFieldNameBytes)
	if err != nil {
		return FlowQueryIndex{}, err
	}
	format, err := decodeFlowQueryIndexFormat(entry["format"], id)
	if err != nil {
		return FlowQueryIndex{}, err
	}
	coverage, err := decodeFlowQueryIndexCoverage(entry["coverage"])
	if err != nil {
		return FlowQueryIndex{}, err
	}
	build, err := decodeFlowQueryIndexBuild(entry["build"])
	if err != nil {
		return FlowQueryIndex{}, err
	}
	validation, err := decodeFlowQueryIndexValidation(entry["validation"])
	if err != nil {
		return FlowQueryIndex{}, err
	}
	retirement, err := decodeFlowQueryIndexRetirement(entry["retirement"])
	if err != nil {
		return FlowQueryIndex{}, err
	}
	statistics, err := decodeFlowQueryIndexStatistics(entry["statistics"])
	if err != nil {
		return FlowQueryIndex{}, err
	}
	return FlowQueryIndex{
		ID: id, Version: version, BuildID: buildID, Source: source, State: state, Queryable: queryable,
		Fields: fields, Workloads: workloads, CountPrefixes: countPrefixes,
		CoveringFields: coveringFields, Format: format, Coverage: coverage, Build: build,
		Validation: validation, Retirement: retirement, Statistics: statistics, Raw: entry,
	}, nil
}

func decodeFlowQueryIndexFields(value any) ([]FlowQueryIndexField, error) {
	items, ok := value.([]any)
	if !ok || len(items) < 2 || len(items) > flowQueryMaxIndexFields {
		return nil, fmt.Errorf("decode FLOW.QUERY.INDEXES index fields: expected 2 to %d entries", flowQueryMaxIndexFields)
	}
	fields := make([]FlowQueryIndexField, len(items))
	seen := make(map[string]struct{}, len(items))
	for position, item := range items {
		mapping, err := nativeMap(item)
		if err != nil {
			return nil, fmt.Errorf("decode FLOW.QUERY.INDEXES index field %d: %w", position, err)
		}
		name, err := requiredBoundedFlowQueryString(mapping, "name", "FLOW.QUERY.INDEXES index field", flowQueryMaxFieldNameBytes)
		if err != nil {
			return nil, err
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, errors.New("decode FLOW.QUERY.INDEXES index fields: duplicate name")
		}
		seen[name] = struct{}{}
		direction, err := requiredFlowQueryChoice(mapping, "direction", "FLOW.QUERY.INDEXES index field", "asc", "desc")
		if err != nil {
			return nil, err
		}
		encoding, err := requiredFlowQueryChoice(mapping, "encoding", "FLOW.QUERY.INDEXES index field", "hashed", "ordered")
		if err != nil {
			return nil, err
		}
		fields[position] = FlowQueryIndexField{Name: name, Direction: direction, Encoding: encoding, Raw: mapping}
	}
	return fields, nil
}

func decodeFlowQueryCountPrefixes(value any, fieldCount int) ([]int64, error) {
	items, ok := value.([]any)
	if !ok || len(items) > fieldCount {
		return nil, errors.New("decode FLOW.QUERY.INDEXES index count_prefixes: invalid array")
	}
	prefixes := make([]int64, len(items))
	previous := int64(0)
	for position, item := range items {
		prefix, err := nonNegativeResponseInteger(item, "FLOW.QUERY.INDEXES index count_prefix")
		if err != nil || prefix == 0 || prefix > int64(fieldCount) || prefix <= previous {
			return nil, errors.New("decode FLOW.QUERY.INDEXES index count_prefixes: invalid entry")
		}
		prefixes[position] = prefix
		previous = prefix
	}
	return prefixes, nil
}

func decodeFlowQueryIndexFormat(value any, id string) (FlowQueryIndexFormat, error) {
	mapping, err := nativeMap(value)
	if err != nil {
		return FlowQueryIndexFormat{}, fmt.Errorf("decode FLOW.QUERY.INDEXES index %q format: %w", id, err)
	}
	values := make([]string, 4)
	for position, field := range []string{"query_row", "key", "entry", "reverse"} {
		values[position], err = requiredBoundedFlowQueryString(mapping, field, "FLOW.QUERY.INDEXES index format", flowQueryMaxCodecNameBytes)
		if err != nil {
			return FlowQueryIndexFormat{}, err
		}
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
		QueryRow: values[0], Key: values[1], Entry: values[2], Reverse: values[3], Counter: counter, Raw: mapping,
	}, nil
}

func decodeUniqueFlowQueryTextArray(value any, context string, maximum, maximumBytes int) ([]string, error) {
	items, ok := value.([]any)
	if !ok || len(items) > maximum {
		return nil, fmt.Errorf("decode %s: expected a bounded array", context)
	}
	values := make([]string, len(items))
	seen := make(map[string]struct{}, len(items))
	for position, item := range items {
		text, err := flowQueryResponseString(item, context)
		if err != nil || text == "" || len(text) > maximumBytes {
			return nil, fmt.Errorf("decode %s: invalid text at position %d", context, position)
		}
		if _, duplicate := seen[text]; duplicate {
			return nil, fmt.Errorf("decode %s: duplicate text", context)
		}
		seen[text] = struct{}{}
		values[position] = text
	}
	return values, nil
}

func requiredFlowQueryChoice(mapping map[string]any, key, context string, choices ...string) (string, error) {
	value, err := requiredBoundedFlowQueryString(mapping, key, context, 64)
	if err != nil {
		return "", err
	}
	if !containsFlowQueryString(choices, value) {
		return "", fmt.Errorf("decode %s %s: unsupported value %q", context, key, value)
	}
	return value, nil
}

func requiredBoundedFlowQueryString(mapping map[string]any, key, context string, maximum int) (string, error) {
	value, err := requiredFlowQueryStringField(mapping, key, context)
	if err != nil || len(value) > maximum {
		return "", fmt.Errorf("decode %s %s: expected non-empty text of at most %d bytes", context, key, maximum)
	}
	return value, nil
}

func requiredNullableFlowQueryText(mapping map[string]any, key, context string, maximum int) (string, error) {
	value, present := mapping[key]
	if !present {
		return "", fmt.Errorf("decode %s: missing nullable %s", context, key)
	}
	if value == nil {
		return "", nil
	}
	text, err := flowQueryResponseString(value, context+" "+key)
	if err != nil || text == "" || len(text) > maximum {
		return "", fmt.Errorf("decode %s %s: invalid nullable text", context, key)
	}
	return text, nil
}

func requiredNullableFlowQueryUnsigned(mapping map[string]any, key, context string) (*uint64, error) {
	value, present := mapping[key]
	if !present {
		return nil, fmt.Errorf("decode %s: missing nullable %s", context, key)
	}
	if value == nil {
		return nil, nil
	}
	parsed, err := unsignedResponseInteger(value, context+" "+key)
	if err != nil {
		return nil, err
	}
	return uint64Pointer(parsed), nil
}

func positiveFlowQueryUnsigned(value any, context string) (uint64, error) {
	parsed, err := unsignedResponseInteger(value, context)
	if err != nil || parsed == 0 {
		return 0, fmt.Errorf("decode %s: expected a positive integer", context)
	}
	return parsed, nil
}

func flowQueryIndexCounter(mapping map[string]any, key, section string) (uint64, error) {
	return unsignedResponseInteger(mapping[key], "FLOW.QUERY.INDEXES index "+section+" "+key)
}

func unsignedResponseInteger(value any, context string) (uint64, error) {
	parsed, ok := flowQueryUint64(value)
	if !ok {
		return 0, fmt.Errorf("decode %s: expected an unsigned integer", context)
	}
	return parsed, nil
}

func containsFlowQueryString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func uint64Pointer(value uint64) *uint64 {
	return &value
}
