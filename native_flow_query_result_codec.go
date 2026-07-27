package ferricstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"unicode/utf8"
)

const (
	nativeCompactQueryMaxRecords     = 100
	nativeCompactQueryMinCursorBytes = 16
	nativeCompactQueryMaxCursorBytes = 4096
	nativeCompactQueryCursorPrefix   = "fqc1_"
	nativeCompactQueryRecordMask     = uint32(1<<20) - 1
	nativeCompactQueryResultRecords  = 7
	nativeCompactQueryResponseBytes  = 8
)

var nativeCompactQueryRecordFields = [...]string{
	"id", "type", "state", "version", "priority", "partition_key",
	"created_at_ms", "updated_at_ms", "next_run_at_ms", "lease_deadline_ms",
	"attempts", "run_state", "max_active_ms", "parent_flow_id", "root_flow_id",
	"correlation_id", "attributes", "state_meta", "event_id", "fields",
}

var nativeCompactQueryQualityFields = [...]string{"exactness", "freshness", "coverage", "pagination"}

var nativeCompactQueryQualityValues = [...][4]string{
	{"authoritative", "projected_exact", "exact", "not_applicable"},
	{"current", "projection_watermark", "not_applicable", ""},
	{"complete", "unavailable", "", ""},
	{"none", "complete", "authenticated_seek", "live_seek"},
}

var nativeCompactQueryUsageFields = [...]string{
	"range_seeks", "range_pages", "scanned_entries", "scanned_bytes",
	"hydrated_records", "residual_checks", "duplicate_entries", "result_records",
	"response_bytes", "memory_high_water_bytes", "wall_time_us",
}

type nativeCompactQueryHeader struct {
	kind    byte
	quality [len(nativeCompactQueryQualityFields)]string
	usage   [len(nativeCompactQueryUsageFields)]int64
	offset  int
}

type nativeCompactQueryPage struct {
	hasMore bool
	cursor  []byte
}

func readNativeCompactQueryHeader(data []byte) (nativeCompactQueryHeader, error) {
	var header nativeCompactQueryHeader
	if len(data) < 2+len(nativeCompactQueryQualityFields)+8*len(nativeCompactQueryUsageFields) || data[0] != nativeCompactFlowQueryResult {
		return header, errors.New("ferricstore native compact FLOW.QUERY result is truncated")
	}
	header.kind = data[1]
	offset := 2
	for index := range nativeCompactQueryQualityFields {
		code := data[offset]
		offset++
		if code >= 4 || nativeCompactQueryQualityValues[index][code] == "" {
			return header, errors.New("ferricstore native compact FLOW.QUERY quality code is invalid")
		}
		header.quality[index] = nativeCompactQueryQualityValues[index][code]
	}
	for index := range nativeCompactQueryUsageFields {
		value, next, err := readNativeCompactQueryU64(data, offset)
		if err != nil {
			return header, err
		}
		offset = next
		header.usage[index] = value
	}
	header.offset = offset
	return header, nil
}

func decodeNativeCompactFlowQueryResult(data []byte) (map[string]any, error) {
	header, err := readNativeCompactQueryHeader(data)
	if err != nil {
		return nil, err
	}
	usageValues := flowQueryUsageFromCompact(header.usage)
	if err := validateFlowQueryUsageCounters(usageValues); err != nil {
		return nil, err
	}
	offset := header.offset
	quality := make(map[string]any, len(nativeCompactQueryQualityFields))
	for index, field := range nativeCompactQueryQualityFields {
		quality[field] = []byte(header.quality[index])
	}
	usage := make(map[string]any, len(nativeCompactQueryUsageFields))
	for index, field := range nativeCompactQueryUsageFields {
		usage[field] = header.usage[index]
	}

	result := map[string]any{
		"version": []byte(flowQueryResultContract),
		"quality": quality,
		"usage":   usage,
	}
	switch header.kind {
	case 0:
		page, next, err := readNativeCompactQueryPage(data, offset)
		if err != nil {
			return nil, err
		}
		offset = next
		if len(data)-offset < 4 {
			return nil, errors.New("ferricstore native compact FLOW.QUERY record count is truncated")
		}
		rawCount := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		if rawCount > nativeCompactQueryMaxRecords {
			return nil, fmt.Errorf("ferricstore native compact FLOW.QUERY page exceeds %d records", nativeCompactQueryMaxRecords)
		}
		count := int(rawCount)
		if err := validateFlowQueryRecordUsage(usageValues, count); err != nil {
			return nil, err
		}
		budget := newNativeCompactFlowRecordBudget()
		if err := consumeNativeCompactFlowRecordItems("FLOW.QUERY records", budget, count); err != nil {
			return nil, err
		}
		records := make([]any, 0, count)
		for index := 0; index < count; index++ {
			record, next, err := readNativeCompactQueryRecord(data, offset, budget)
			if err != nil {
				return nil, fmt.Errorf("ferricstore native compact FLOW.QUERY record %d: %w", index, err)
			}
			offset = next
			records = append(records, record)
		}
		result["records"] = records
		result["page"] = page
	case 1:
		if err := validateFlowQueryCountUsage(usageValues); err != nil {
			return nil, err
		}
		count, next, err := readNativeCompactQueryU64(data, offset)
		if err != nil {
			return nil, err
		}
		offset = next
		result["result"] = map[string]any{"kind": []byte("count"), "value": count}
	default:
		return nil, fmt.Errorf("ferricstore native compact FLOW.QUERY result kind %d is unsupported", header.kind)
	}
	if offset != len(data) {
		return nil, errors.New("ferricstore native compact FLOW.QUERY result has trailing bytes")
	}
	if header.usage[nativeCompactQueryResponseBytes] != int64(len(data)) {
		return nil, errors.New("ferricstore native compact FLOW.QUERY response_bytes does not match payload")
	}
	return result, nil
}

func readNativeCompactQueryPage(data []byte, offset int) (map[string]any, int, error) {
	page, next, err := readNativeCompactQueryPageData(data, offset)
	if err != nil {
		return nil, offset, err
	}
	if !page.hasMore {
		return map[string]any{"has_more": false, "cursor": nil}, next, nil
	}
	return map[string]any{"has_more": true, "cursor": page.cursor}, next, nil
}

func readNativeCompactQueryPageData(data []byte, offset int) (nativeCompactQueryPage, int, error) {
	if len(data)-offset < 5 {
		return nativeCompactQueryPage{}, offset, errors.New("ferricstore native compact FLOW.QUERY page is truncated")
	}
	hasMore := data[offset]
	size := binary.BigEndian.Uint32(data[offset+1 : offset+5])
	offset += 5
	if hasMore == 0 && size == nativeCompactNilU32 {
		return nativeCompactQueryPage{}, offset, nil
	}
	if hasMore != 1 || size < nativeCompactQueryMinCursorBytes || size == nativeCompactNilU32 || size > nativeCompactQueryMaxCursorBytes || uint64(size) > uint64(len(data)-offset) {
		return nativeCompactQueryPage{}, offset, errors.New("ferricstore native compact FLOW.QUERY cursor is invalid")
	}
	cursorSize := int(size)
	cursor := data[offset : offset+cursorSize : offset+cursorSize]
	if !bytes.HasPrefix(cursor, []byte(nativeCompactQueryCursorPrefix)) || !utf8.Valid(cursor) {
		return nativeCompactQueryPage{}, offset, errors.New("ferricstore native compact FLOW.QUERY cursor is invalid")
	}
	return nativeCompactQueryPage{hasMore: true, cursor: cursor}, offset + cursorSize, nil
}

func readNativeCompactQueryRecord(data []byte, offset int, budget *nativeDecodeBudget) (map[string]any, int, error) {
	if len(data)-offset < 4 {
		return nil, offset, errors.New("record bitmap is truncated")
	}
	bitmap := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	if bitmap & ^nativeCompactQueryRecordMask != 0 {
		return nil, offset, errors.New("record contains reserved fields")
	}
	record := make(map[string]any, bits.OnesCount32(bitmap))
	for index, field := range nativeCompactQueryRecordFields {
		if bitmap&(1<<index) == 0 {
			continue
		}
		value, rest, err := decodeNativeValueBudget(data[offset:], budget, 0)
		if err != nil {
			return nil, offset, err
		}
		offset += len(data[offset:]) - len(rest)
		record[field] = value
	}
	return record, offset, nil
}

func readNativeCompactQueryU64(data []byte, offset int) (int64, int, error) {
	if len(data)-offset < 8 {
		return 0, offset, errors.New("ferricstore native compact FLOW.QUERY integer is truncated")
	}
	value := binary.BigEndian.Uint64(data[offset : offset+8])
	if value > math.MaxInt64 {
		return 0, offset, errors.New("ferricstore native compact FLOW.QUERY integer exceeds signed 64-bit range")
	}
	return int64(value), offset + 8, nil
}
