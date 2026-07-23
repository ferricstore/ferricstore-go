package ferricstore

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"
)

const (
	flowQueryMaxResponseRecords = 100
	flowQueryMaxIndexEntries    = 32
	flowQueryMaxQualityBytes    = 64
)

var flowQueryRecordTextFields = [...]string{
	"id", "type", "state", "partition_key", "run_state",
	"parent_flow_id", "root_flow_id", "correlation_id",
}

func decodeFlowQueryResult(value any) (*FlowQueryResult, error) {
	mapping, err := nativeMap(value)
	if err != nil {
		return nil, fmt.Errorf("decode FLOW.QUERY result: %w", err)
	}
	version, err := requiredFlowQueryStringField(mapping, "version", "FLOW.QUERY result")
	if err != nil || version != flowQueryResultContract {
		return nil, fmt.Errorf("decode FLOW.QUERY result: unsupported contract %q", version)
	}
	quality, err := decodeFlowQueryQuality(mapping["quality"])
	if err != nil {
		return nil, err
	}
	usage, err := decodeFlowQueryUsage(mapping["usage"])
	if err != nil {
		return nil, err
	}
	_, hasRecords := mapping["records"]
	_, hasCount := mapping["result"]
	if hasRecords == hasCount {
		return nil, errors.New("decode FLOW.QUERY result: expected exactly one records or count shape")
	}
	result := &FlowQueryResult{Version: version, Quality: quality, Usage: usage, Raw: mapping}
	if hasRecords {
		records, err := flowQueryRecordMaps(mapping["records"])
		if err != nil {
			return nil, err
		}
		page, err := decodeFlowQueryPage(mapping["page"])
		if err != nil {
			return nil, err
		}
		if usage.ResultRecords != int64(len(records)) {
			return nil, fmt.Errorf("decode FLOW.QUERY result: usage reports %d records for %d returned records", usage.ResultRecords, len(records))
		}
		result.Records = records
		result.Page = page
		return result, nil
	}
	if _, present := mapping["page"]; present {
		return nil, errors.New("decode FLOW.QUERY count result: unexpected page")
	}
	countResult, err := nativeMap(mapping["result"])
	if err != nil {
		return nil, fmt.Errorf("decode FLOW.QUERY count result: %w", err)
	}
	kind, err := requiredFlowQueryStringField(countResult, "kind", "FLOW.QUERY count result")
	if err != nil || kind != "count" {
		return nil, errors.New("decode FLOW.QUERY count result: kind must be count")
	}
	count, err := nonNegativeResponseInteger(countResult["value"], "FLOW.QUERY count value")
	if err != nil {
		return nil, err
	}
	if usage.ResultRecords != 1 {
		return nil, fmt.Errorf("decode FLOW.QUERY count result: usage result_records = %d, want 1", usage.ResultRecords)
	}
	result.Count = &count
	return result, nil
}

func flowQueryRecordMaps(value any) ([]map[string]any, error) {
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("decode FLOW.QUERY records: expected array, got %T", value)
	}
	if len(items) > flowQueryMaxResponseRecords {
		return nil, fmt.Errorf("decode FLOW.QUERY records: expected at most %d records", flowQueryMaxResponseRecords)
	}
	records := make([]map[string]any, len(items))
	for index, item := range items {
		mapping, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("decode FLOW.QUERY record %d: expected map, got %T", index, item)
		}
		for _, field := range flowQueryRecordTextFields {
			raw, present := mapping[field]
			if !present || raw == nil {
				continue
			}
			text, err := flowQueryResponseString(raw, "FLOW.QUERY record "+field)
			if err != nil || text == "" {
				return nil, fmt.Errorf("decode FLOW.QUERY record %d: %s must be non-empty UTF-8 text", index, field)
			}
			mapping[field] = text
		}
		records[index] = mapping
	}
	return records, nil
}

func decodeFlowQueryPage(value any) (*FlowQueryPage, error) {
	mapping, err := nativeMap(value)
	if err != nil {
		return nil, fmt.Errorf("decode FLOW.QUERY page: %w", err)
	}
	hasMore, ok := mapping["has_more"].(bool)
	if !ok {
		return nil, errors.New("decode FLOW.QUERY page: has_more must be boolean")
	}
	cursor := ""
	if value, present := mapping["cursor"]; present && value != nil {
		cursor, err = flowQueryResponseString(value, "FLOW.QUERY page cursor")
		if err != nil || cursor == "" || len(cursor) > 4096 || !strings.HasPrefix(cursor, "fqc1_") {
			return nil, errors.New("decode FLOW.QUERY page: cursor is invalid")
		}
	}
	if hasMore != (cursor != "") {
		return nil, errors.New("decode FLOW.QUERY page: has_more and cursor are inconsistent")
	}
	return &FlowQueryPage{HasMore: hasMore, Cursor: cursor}, nil
}

func decodeFlowQueryQuality(value any) (FlowQueryQuality, error) {
	mapping, err := nativeMap(value)
	if err != nil {
		return FlowQueryQuality{}, fmt.Errorf("decode FLOW.QUERY quality: %w", err)
	}
	fields := make([]string, 4)
	for index, name := range []string{"exactness", "freshness", "coverage", "pagination"} {
		fields[index], err = requiredFlowQueryStringField(mapping, name, "FLOW.QUERY quality")
		if err != nil {
			return FlowQueryQuality{}, err
		}
		if len(fields[index]) > flowQueryMaxQualityBytes {
			return FlowQueryQuality{}, fmt.Errorf("decode FLOW.QUERY quality %s: exceeds %d bytes", name, flowQueryMaxQualityBytes)
		}
	}
	return FlowQueryQuality{Exactness: fields[0], Freshness: fields[1], Coverage: fields[2], Pagination: fields[3]}, nil
}

func decodeFlowQueryUsage(value any) (FlowQueryUsage, error) {
	mapping, err := nativeMap(value)
	if err != nil {
		return FlowQueryUsage{}, fmt.Errorf("decode FLOW.QUERY usage: %w", err)
	}
	names := []string{
		"range_seeks", "range_pages", "scanned_entries", "scanned_bytes", "hydrated_records",
		"residual_checks", "duplicate_entries", "result_records", "response_bytes",
		"memory_high_water_bytes", "wall_time_us",
	}
	values := make([]int64, len(names))
	for index, name := range names {
		if _, present := mapping[name]; !present {
			return FlowQueryUsage{}, fmt.Errorf("decode FLOW.QUERY usage: missing %s", name)
		}
		values[index], err = nonNegativeResponseInteger(mapping[name], "FLOW.QUERY usage "+name)
		if err != nil {
			return FlowQueryUsage{}, err
		}
	}
	return FlowQueryUsage{
		RangeSeeks: values[0], RangePages: values[1], ScannedEntries: values[2], ScannedBytes: values[3],
		HydratedRecords: values[4], ResidualChecks: values[5], DuplicateEntries: values[6],
		ResultRecords: values[7], ResponseBytes: values[8], MemoryHighWaterBytes: values[9], WallTimeUS: values[10],
	}, nil
}

func nonNegativeResponseInteger(value any, context string) (int64, error) {
	parsed, ok := flowQueryUint64(value)
	if !ok || parsed > math.MaxInt64 {
		return 0, fmt.Errorf("decode %s: expected a non-negative signed integer", context)
	}
	return int64(parsed), nil
}

func decodeFlowExplainResult(value any) (*FlowExplainResult, error) {
	mapping, err := nativeMap(value)
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
	result := &FlowExplainResult{
		Version: version, QueryFingerprint: fingerprint, Status: status, Plan: plan,
		Estimate: estimate, Bounds: bounds, Raw: mapping,
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

func requiredFlowQueryStringField(mapping map[string]any, key, context string) (string, error) {
	raw, found := mapping[key]
	if !found || raw == nil {
		return "", fmt.Errorf("decode %s %s: missing string field", context, key)
	}
	value, err := flowQueryResponseString(raw, "")
	if err != nil {
		return "", fmt.Errorf("decode %s %s: %w", context, key, err)
	}
	if value == "" {
		return "", fmt.Errorf("decode %s %s: field must be a non-empty string", context, key)
	}
	return value, nil
}

func optionalFlowQueryStringField(mapping map[string]any, key, context string) (string, error) {
	raw, found := mapping[key]
	if !found || raw == nil {
		return "", nil
	}
	value, err := flowQueryResponseString(raw, "")
	if err != nil {
		return "", fmt.Errorf("decode %s %s: %w", context, key, err)
	}
	if value == "" {
		return "", fmt.Errorf("decode %s %s: field must be a non-empty string", context, key)
	}
	return value, nil
}

func flowQueryResponseString(value any, context string) (string, error) {
	switch typed := value.(type) {
	case string:
		if utf8.ValidString(typed) {
			return typed, nil
		}
	case []byte:
		if utf8.Valid(typed) {
			return string(typed), nil
		}
	default:
		return responseString(value, nil)
	}

	if context != "" {
		return "", fmt.Errorf("decode %s: text is not valid UTF-8", context)
	}
	return "", errors.New("text is not valid UTF-8")
}

func requiredFlowQueryMap(mapping map[string]any, key, context string) (map[string]any, error) {
	value, present := mapping[key]
	if !present {
		return nil, fmt.Errorf("decode %s: missing %s", context, key)
	}
	parsed, err := nativeMap(value)
	if err != nil {
		return nil, fmt.Errorf("decode %s %s: %w", context, key, err)
	}
	return parsed, nil
}

func wrapFlowQueryError(err error) error {
	if err == nil {
		return nil
	}
	nativeErr, ok := nativeErrorValue(err)
	if !ok {
		return err
	}
	mapping, mapErr := nativeMap(nativeErr.Value)
	if mapErr != nil {
		return err
	}
	queryErr, decodeErr := decodeFlowQueryErrorMap(mapping, err)
	if decodeErr != nil {
		return err
	}
	return queryErr
}

func decodeFlowQueryErrorPayload(value any, cause error) (*FlowQueryError, error) {
	mapping, err := nativeMap(value)
	if err != nil {
		return nil, err
	}
	return decodeFlowQueryErrorMap(mapping, cause)
}

func decodeFlowQueryErrorMap(mapping map[string]any, cause error) (*FlowQueryError, error) {
	code, codeErr := requiredFlowQueryStringField(mapping, "code", "FLOW.QUERY error")
	message, messageErr := requiredFlowQueryStringField(mapping, "message", "FLOW.QUERY error")
	if codeErr != nil || messageErr != nil {
		return nil, errors.New("diagnostic requires non-empty code and message")
	}
	queryErr := &FlowQueryError{Code: code, Message: message, cause: cause}
	var err error
	if queryErr.Detail, err = optionalFlowQueryStringField(mapping, "detail", "FLOW.QUERY error"); err != nil {
		return nil, err
	}
	if queryErr.Hint, err = optionalFlowQueryStringField(mapping, "hint", "FLOW.QUERY error"); err != nil {
		return nil, err
	}
	var ok bool
	if queryErr.Retryable, ok = mapping["retryable"].(bool); !ok {
		return nil, errors.New("diagnostic retryable must be boolean")
	}
	if queryErr.SafeToRetry, ok = mapping["safe_to_retry"].(bool); !ok {
		return nil, errors.New("diagnostic safe_to_retry must be boolean")
	}
	retryAfter, present := mapping["retry_after_ms"]
	if !present || retryAfter == nil {
		return nil, errors.New("diagnostic retry_after_ms is missing")
	}
	if queryErr.RetryAfterMS, err = nonNegativeResponseInteger(retryAfter, "FLOW.QUERY error retry_after_ms"); err != nil {
		return nil, err
	}
	if contextValue, present := mapping["context"]; present && contextValue != nil {
		if queryErr.Context, err = nativeMap(contextValue); err != nil {
			return nil, errors.New("diagnostic context must be a map")
		}
	}
	if positionValue, present := mapping["position"]; present && positionValue != nil {
		position, positionErr := nativeMap(positionValue)
		if positionErr != nil {
			return nil, errors.New("diagnostic position must be a map")
		}
		byteOffset, byteErr := nonNegativeResponseInteger(position["byte"], "FLOW.QUERY error position byte")
		line, lineErr := nonNegativeResponseInteger(position["line"], "FLOW.QUERY error position line")
		column, columnErr := nonNegativeResponseInteger(position["column"], "FLOW.QUERY error position column")
		if byteErr != nil || lineErr != nil || columnErr != nil || byteOffset == 0 || line == 0 || column == 0 {
			return nil, errors.New("diagnostic position values must be positive integers")
		}
		queryErr.Position = &FlowQueryErrorPosition{Byte: byteOffset, Line: line, Column: column}
	}
	return queryErr, nil
}

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

func flowQueryUint64(value any) (uint64, bool) {
	switch typed := value.(type) {
	case int:
		return uint64(typed), typed >= 0
	case int8:
		return uint64(typed), typed >= 0
	case int16:
		return uint64(typed), typed >= 0
	case int32:
		return uint64(typed), typed >= 0
	case int64:
		return uint64(typed), typed >= 0
	case uint:
		return uint64(typed), true
	case uint8:
		return uint64(typed), true
	case uint16:
		return uint64(typed), true
	case uint32:
		return uint64(typed), true
	case uint64:
		return typed, true
	default:
		return 0, false
	}
}
