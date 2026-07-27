package ferricstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"unicode/utf8"
)

// decodeNativeCompactFlowQueryResultTyped decodes the fixed compact query
// envelope directly into the public result shape. It is selected only for the
// typed Client.FlowQuery path; raw Executor.Do calls retain the generic native
// map and []byte representation.
func decodeNativeCompactFlowQueryResultTyped(data []byte) (*FlowQueryResult, error) {
	header, err := readNativeCompactQueryHeader(data)
	if err != nil {
		return nil, err
	}
	quality := FlowQueryQuality{
		Exactness:  header.quality[0],
		Freshness:  header.quality[1],
		Coverage:   header.quality[2],
		Pagination: header.quality[3],
	}
	usage := flowQueryUsageFromCompact(header.usage)
	if err := validateFlowQueryUsageCounters(usage); err != nil {
		return nil, err
	}
	raw := map[string]any{
		"version": []byte(flowQueryResultContract),
		"quality": nativeCompactQueryRawQuality(header.quality),
		"usage":   nativeCompactQueryRawUsage(header.usage),
	}
	result := &FlowQueryResult{
		Version: flowQueryResultContract,
		Quality: quality,
		Usage:   usage,
		Raw:     raw,
	}
	offset := header.offset
	switch header.kind {
	case 0:
		pageData, next, err := readNativeCompactQueryPageData(data, offset)
		if err != nil {
			return nil, err
		}
		offset = next
		page, rawPage, err := typedNativeCompactQueryPage(pageData)
		if err != nil {
			return nil, err
		}
		if len(data)-offset < 4 {
			return nil, errors.New("ferricstore native compact FLOW.QUERY record count is truncated")
		}
		rawCount := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		if rawCount > nativeCompactQueryMaxRecords {
			return nil, fmt.Errorf("ferricstore native compact FLOW.QUERY page exceeds %d records", nativeCompactQueryMaxRecords)
		}
		count := int(rawCount)
		budget := newNativeCompactFlowRecordBudget()
		if err := consumeNativeCompactFlowRecordItems("FLOW.QUERY records", budget, count); err != nil {
			return nil, err
		}
		records := make([]map[string]any, count)
		rawRecords := make([]any, count)
		for index := 0; index < count; index++ {
			record, next, err := readNativeCompactQueryRecordTyped(data, offset, budget)
			if err != nil {
				return nil, fmt.Errorf("ferricstore native compact FLOW.QUERY record %d: %w", index, err)
			}
			offset = next
			records[index] = record
			rawRecords[index] = record
		}
		if err := validateFlowQueryRecordUsage(usage, count); err != nil {
			return nil, err
		}
		result.Records = records
		result.Page = page
		raw["records"] = rawRecords
		raw["page"] = rawPage
	case 1:
		count, next, err := readNativeCompactQueryU64(data, offset)
		if err != nil {
			return nil, err
		}
		offset = next
		if err := validateFlowQueryCountUsage(usage); err != nil {
			return nil, err
		}
		result.Count = &count
		raw["result"] = map[string]any{"kind": []byte("count"), "value": count}
	default:
		return nil, fmt.Errorf("ferricstore native compact FLOW.QUERY result kind %d is unsupported", header.kind)
	}
	if offset != len(data) {
		return nil, errors.New("ferricstore native compact FLOW.QUERY result has trailing bytes")
	}
	if usage.ResponseBytes != int64(len(data)) {
		return nil, errors.New("ferricstore native compact FLOW.QUERY response_bytes does not match payload")
	}
	return result, nil
}

func flowQueryUsageFromCompact(values [len(nativeCompactQueryUsageFields)]int64) FlowQueryUsage {
	return FlowQueryUsage{
		RangeSeeks: values[0], RangePages: values[1], ScannedEntries: values[2], ScannedBytes: values[3],
		HydratedRecords: values[4], ResidualChecks: values[5], DuplicateEntries: values[6],
		ResultRecords: values[nativeCompactQueryResultRecords], ResponseBytes: values[nativeCompactQueryResponseBytes],
		MemoryHighWaterBytes: values[9], WallTimeUS: values[10],
	}
}

func nativeCompactQueryRawQuality(values [len(nativeCompactQueryQualityFields)]string) map[string]any {
	raw := make(map[string]any, len(nativeCompactQueryQualityFields))
	for index, field := range nativeCompactQueryQualityFields {
		raw[field] = []byte(values[index])
	}
	return raw
}

func nativeCompactQueryRawUsage(values [len(nativeCompactQueryUsageFields)]int64) map[string]any {
	raw := make(map[string]any, len(nativeCompactQueryUsageFields))
	for index, field := range nativeCompactQueryUsageFields {
		raw[field] = values[index]
	}
	return raw
}

func typedNativeCompactQueryPage(page nativeCompactQueryPage) (*FlowQueryPage, map[string]any, error) {
	if !page.hasMore {
		return &FlowQueryPage{}, map[string]any{"has_more": false, "cursor": nil}, nil
	}
	cursor := string(page.cursor)
	return &FlowQueryPage{HasMore: true, Cursor: cursor}, map[string]any{
		"has_more": true,
		"cursor":   page.cursor,
	}, nil
}

func readNativeCompactQueryRecordTyped(
	data []byte,
	offset int,
	budget *nativeDecodeBudget,
) (map[string]any, int, error) {
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
		if nativeCompactQueryRecordTextField(index) {
			text, isNil, next, err := readNativeCompactQueryText(data, offset, budget)
			if err != nil {
				return nil, offset, fmt.Errorf("%s must be non-empty UTF-8 text: %w", field, err)
			}
			offset = next
			if isNil {
				record[field] = nil
			} else {
				record[field] = text
			}
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

func nativeCompactQueryRecordTextField(index int) bool {
	switch index {
	case 0, 1, 2, 5, 11, 13, 14, 15, 18:
		return true
	default:
		return false
	}
}

func readNativeCompactQueryText(
	data []byte,
	offset int,
	budget *nativeDecodeBudget,
) (text string, isNil bool, next int, err error) {
	if offset >= len(data) {
		return "", false, offset, errors.New("native value is empty")
	}
	budget.remaining--
	if budget.remaining < 0 {
		return "", false, offset, errors.New("native value exceeds aggregate item limit")
	}
	if data[offset] == 0 {
		return "", true, offset + 1, nil
	}
	if data[offset] != 4 {
		return "", false, offset, fmt.Errorf("expected binary value, got native tag %d", data[offset])
	}
	if len(data)-offset < 5 {
		return "", false, offset, errors.New("binary length is truncated")
	}
	size := binary.BigEndian.Uint32(data[offset+1 : offset+5])
	offset += 5
	if uint64(size) > uint64(len(data)-offset) {
		return "", false, offset, errors.New("binary value is truncated")
	}
	next = offset + int(size)
	value := data[offset:next]
	if len(value) == 0 || !utf8.Valid(value) {
		return "", false, offset, errors.New("text is empty or invalid UTF-8")
	}
	return string(value), false, next, nil
}
