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
	"parent_flow_id", "root_flow_id", "correlation_id", "event_id",
}

func decodeFlowQueryResult(value any) (*FlowQueryResult, error) {
	if owned, ok := value.(ownedNativeFlowQueryResult); ok {
		if owned.result == nil {
			return nil, errors.New("decode FLOW.QUERY result: native typed result is nil")
		}
		return owned.result, nil
	}
	mapping, err := flowQueryResponseMap(value)
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
		if err := validateFlowQueryRecordUsage(usage, len(records)); err != nil {
			return nil, err
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
	if err := validateFlowQueryCountUsage(usage); err != nil {
		return nil, err
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
			if text, ok := raw.(string); ok {
				if text == "" || !utf8.ValidString(text) {
					return nil, fmt.Errorf("decode FLOW.QUERY record %d: %s must be non-empty UTF-8 text", index, field)
				}
				continue
			}
			text, err := flowQueryResponseString(raw, "")
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
		if err != nil || len(cursor) < 16 || len(cursor) > 4096 || !strings.HasPrefix(cursor, "fqc1_") {
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
		if !validFlowQueryQuality(name, fields[index]) {
			return FlowQueryQuality{}, fmt.Errorf("decode FLOW.QUERY quality %s: unsupported value %q", name, fields[index])
		}
	}
	return FlowQueryQuality{Exactness: fields[0], Freshness: fields[1], Coverage: fields[2], Pagination: fields[3]}, nil
}

func validFlowQueryQuality(field, value string) bool {
	switch field {
	case "exactness":
		return value == "authoritative" || value == "projected_exact" || value == "exact" || value == "not_applicable"
	case "freshness":
		return value == "current" || value == "projection_watermark" || value == "not_applicable"
	case "coverage":
		return value == "complete" || value == "unavailable"
	case "pagination":
		return value == "none" || value == "complete" || value == "authenticated_seek" || value == "live_seek"
	default:
		return false
	}
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
	usage := FlowQueryUsage{
		RangeSeeks: values[0], RangePages: values[1], ScannedEntries: values[2], ScannedBytes: values[3],
		HydratedRecords: values[4], ResidualChecks: values[5], DuplicateEntries: values[6],
		ResultRecords: values[7], ResponseBytes: values[8], MemoryHighWaterBytes: values[9], WallTimeUS: values[10],
	}
	if err := validateFlowQueryUsageCounters(usage); err != nil {
		return FlowQueryUsage{}, err
	}
	return usage, nil
}

func validateFlowQueryUsageCounters(usage FlowQueryUsage) error {
	maxPages := int64(math.MaxInt64)
	if usage.ScannedEntries <= math.MaxInt64-usage.RangeSeeks {
		maxPages = usage.ScannedEntries + usage.RangeSeeks
	}
	maxResidualChecks := int64(math.MaxInt64)
	if usage.ScannedEntries <= math.MaxInt64/12 {
		maxResidualChecks = usage.ScannedEntries * 12
	}
	if usage.HydratedRecords > usage.ScannedEntries ||
		usage.DuplicateEntries > usage.ScannedEntries ||
		usage.RangePages > maxPages || usage.ResidualChecks > maxResidualChecks {
		return errors.New("decode FLOW.QUERY usage: counters are inconsistent")
	}
	return nil
}

func validateFlowQueryRecordUsage(usage FlowQueryUsage, count int) error {
	if usage.ResultRecords != int64(count) {
		return fmt.Errorf(
			"decode FLOW.QUERY result: usage reports %d records for %d returned records",
			usage.ResultRecords,
			count,
		)
	}
	if usage.ResultRecords > usage.ScannedEntries {
		return errors.New("decode FLOW.QUERY result: returned records exceed scanned entries")
	}
	return nil
}

func validateFlowQueryCountUsage(usage FlowQueryUsage) error {
	if usage.ResultRecords != 1 {
		return fmt.Errorf(
			"decode FLOW.QUERY count result: usage result_records = %d, want 1",
			usage.ResultRecords,
		)
	}
	return nil
}

func nonNegativeResponseInteger(value any, context string) (int64, error) {
	parsed, ok := flowQueryUint64(value)
	if !ok || parsed > math.MaxInt64 {
		return 0, fmt.Errorf("decode %s: expected a non-negative signed integer", context)
	}
	return int64(parsed), nil
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
	code, codeErr := requiredFlowQueryDiagnosticText(mapping, "code")
	message, messageErr := requiredFlowQueryDiagnosticText(mapping, "message")
	if codeErr != nil || messageErr != nil {
		return nil, errors.New("diagnostic requires non-empty code and message")
	}
	queryErr := &FlowQueryError{Code: code, Message: message, cause: cause}
	var err error
	if queryErr.Detail, err = optionalFlowQueryDiagnosticText(mapping, "detail"); err != nil {
		return nil, err
	}
	if queryErr.Hint, err = optionalFlowQueryDiagnosticText(mapping, "hint"); err != nil {
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
		if err = validateFlowQueryDiagnosticContext(queryErr.Context); err != nil {
			return nil, err
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
