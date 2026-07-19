package ferricstore

import (
	"encoding/binary"
	"errors"
	"fmt"
)

func decodeNativeCompactFlowRecord(data []byte) (map[string]any, error) {
	record, offset, err := takeNativeCompactFlowRecord(data, 0)
	if err != nil {
		return nil, err
	}
	if offset != len(data) {
		return nil, errors.New("ferricstore native compact Flow record has trailing bytes")
	}
	return record, nil
}

func decodeNativeCompactFlowRecordList(data []byte) ([]any, error) {
	records, offset, err := takeNativeCompactFlowRecordList(data, 0)
	if err != nil {
		return nil, err
	}
	if offset != len(data) {
		return nil, errors.New("ferricstore native compact Flow record list has trailing bytes")
	}
	return records, nil
}

func takeNativeCompactFlowRecordList(data []byte, offset int) ([]any, int, error) {
	return takeNativeCompactFlowRecordListWithBudget(data, offset, newNativeCompactFlowRecordBudget())
}

func takeNativeCompactFlowRecordListWithBudget(data []byte, offset int, budget *nativeDecodeBudget) ([]any, int, error) {
	if len(data)-offset < 5 || data[offset] != nativeCompactFlowRecordList {
		return nil, offset, errors.New("ferricstore native compact Flow record list is invalid")
	}
	count, err := nativeBoundedItemCount(
		"compact Flow record list", binary.BigEndian.Uint32(data[offset+1:offset+5]), len(data)-offset-5, 5,
	)
	if err != nil {
		return nil, offset, err
	}
	if err := consumeNativeCompactFlowRecordItems("compact Flow record list", budget, count); err != nil {
		return nil, offset, err
	}
	offset += 5
	records := make([]any, 0, count)
	for index := 0; index < count; index++ {
		record, next, err := takeNativeCompactFlowRecordWithBudget(data, offset, budget)
		if err != nil {
			return nil, offset, err
		}
		offset = next
		records = append(records, record)
	}
	return records, offset, nil
}

func takeNativeCompactFlowRecord(data []byte, offset int) (map[string]any, int, error) {
	return takeNativeCompactFlowRecordWithBudget(data, offset, newNativeCompactFlowRecordBudget())
}

func takeNativeCompactFlowRecordWithBudget(data []byte, offset int, budget *nativeDecodeBudget) (map[string]any, int, error) {
	if len(data)-offset < 5 || data[offset] != nativeCompactFlowRecord {
		return nil, offset, errors.New("ferricstore native compact Flow record is invalid")
	}
	count, err := nativeBoundedItemCount(
		"compact Flow record", binary.BigEndian.Uint32(data[offset+1:offset+5]), len(data)-offset-5, 2,
	)
	if err != nil {
		return nil, offset, err
	}
	if err := consumeNativeCompactFlowRecordItems("compact Flow record", budget, count); err != nil {
		return nil, offset, err
	}
	offset += 5
	record := make(map[string]any, count)
	for index := 0; index < count; index++ {
		if offset >= len(data) {
			return nil, offset, errors.New("ferricstore native compact Flow record field is truncated")
		}
		fieldID := data[offset]
		offset++
		fieldName := nativeCompactFlowRecordFieldName(fieldID)
		if fieldID == 0 {
			name, next, err := readNativeCompactBinary(data, offset)
			if err != nil {
				return nil, offset, fmt.Errorf("compact Flow extension name: %w", err)
			}
			if len(name) == 0 {
				return nil, offset, errors.New("ferricstore native compact Flow extension name is empty")
			}
			fieldName = string(name)
			offset = next
		}
		value, rest, err := decodeNativeValueBudget(data[offset:], budget, 0)
		if err != nil {
			return nil, offset, fmt.Errorf("compact Flow field %d: %w", fieldID, err)
		}
		offset += len(data[offset:]) - len(rest)
		if fieldName == "" {
			// Future numeric fields are length-delimited by their typed value. They
			// can be skipped without weakening validation of the remaining record.
			continue
		}
		if _, duplicate := record[fieldName]; duplicate {
			return nil, offset, fmt.Errorf("ferricstore native compact Flow record contains duplicate field %q", fieldName)
		}
		record[fieldName] = value
	}
	return record, offset, nil
}

func newNativeCompactFlowRecordBudget() *nativeDecodeBudget {
	return &nativeDecodeBudget{
		maxDepth: nativeMaxDecodeDepth, remaining: nativeMaxContainerItems, copyBinary: false,
	}
}

func consumeNativeCompactFlowRecordItems(kind string, budget *nativeDecodeBudget, count int) error {
	if count > budget.remaining {
		return fmt.Errorf("ferricstore native %s exceeds aggregate item limit", kind)
	}
	budget.remaining -= count
	return nil
}

func nativeCompactFlowRecordFieldName(id byte) string {
	if int(id) >= len(nativeCompactFlowRecordFields) {
		return ""
	}
	return nativeCompactFlowRecordFields[id]
}

var nativeCompactFlowRecordFields = [...]string{
	"", "id", "type", "state", "version", "priority", "partition_key",
	"payload_ref", "result_ref", "error_ref", "payload", "result", "error",
	"created_at_ms", "updated_at_ms", "next_run_at_ms", "lease_deadline_ms",
	"lease_owner", "lease_token", "fencing_token", "attempts", "history_max_events",
	"history_hot_max_events", "child_groups", "parent_flow_id", "parent_partition_key",
	"root_flow_id", "correlation_id", "terminal_retention_until_ms", "ttl_ms",
	"retention_ttl_ms", "run_state", "value_refs", "values", "payload_omitted",
	"payload_size", "result_omitted", "result_size", "error_omitted", "error_size",
	"max_attempts", "attributes",
}
