package ferricstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

func decodeNativeCompactValue(opcode uint16, data []byte) (any, bool, error) {
	if len(data) == 0 {
		return nil, false, errors.New("ferricstore native compact response is empty")
	}
	if !nativeCompactResponseMarkerAllowed(opcode, data[0]) {
		return nil, false, nil
	}
	switch data[0] {
	case nativeCompactFlowClaimJobs:
		value, err := decodeNativeCompactClaimJobs(data)
		return value, true, err
	case nativeCompactOKList:
		value, err := decodeNativeCompactOKList(data)
		return value, true, err
	case nativeCompactKVGet:
		value, err := decodeNativeCompactKVGet(data)
		return value, true, err
	case nativeCompactKVMGet:
		value, err := decodeNativeCompactKVMGet(data)
		return value, true, err
	case nativeCompactKVMGetFixed:
		value, err := decodeNativeCompactKVMGetFixed(data)
		return value, true, err
	case nativeCompactPipelineResponse:
		value, err := decodeNativeCompactPipelineResponse(data)
		return value, true, err
	default:
		return nil, false, nil
	}
}

func nativeCompactResponseMarkerAllowed(opcode uint16, marker byte) bool {
	switch marker {
	case nativeCompactFlowClaimJobs:
		return opcode == nativeOpFlowClaimDue
	case nativeCompactOKList:
		switch opcode {
		case nativeOpPipeline, nativeOpSet, nativeOpMSet, nativeOpFlowCreateMany, nativeOpFlowCompleteMany,
			0x0212, 0x0213, 0x0214: // FLOW retry/fail/cancel many.
			return true
		}
	case nativeCompactKVGet:
		return opcode == nativeOpGet
	case nativeCompactKVMGet, nativeCompactKVMGetFixed:
		return opcode == nativeOpPipeline || opcode == nativeOpMGet || opcode == 0x020C // FLOW.VALUE.MGET.
	case nativeCompactPipelineResponse:
		return opcode == nativeOpPipeline
	}
	return false
}

func decodeNativeCompactOKList(data []byte) (any, error) {
	if len(data) != 5 || data[0] != nativeCompactOKList {
		return nil, errors.New("ferricstore native compact OK list is invalid")
	}
	count, err := nativeBoundedItemCount("compact OK list", binary.BigEndian.Uint32(data[1:5]), 0, 0)
	if err != nil {
		return nil, err
	}
	return nativeCompactOKCount(count), nil
}

func decodeNativeCompactKVGet(data []byte) (any, error) {
	if len(data) < 2 || data[0] != nativeCompactKVGet {
		return nil, errors.New("ferricstore native compact GET is invalid")
	}
	switch data[1] {
	case 0:
		if len(data) != 2 {
			return nil, errors.New("ferricstore native compact nil GET has trailing bytes")
		}
		return nil, nil
	case 1:
		value, next, err := readNativeCompactBinary(data, 2)
		if err != nil {
			return nil, err
		}
		if next != len(data) {
			return nil, errors.New("ferricstore native compact GET has trailing bytes")
		}
		return value, nil
	default:
		return nil, fmt.Errorf("ferricstore native compact GET present tag %d is unsupported", data[1])
	}
}

func decodeNativeCompactKVMGet(data []byte) ([]any, error) {
	if len(data) < 5 || data[0] != nativeCompactKVMGet {
		return nil, errors.New("ferricstore native compact MGET is invalid")
	}
	count, err := nativeBoundedItemCount("compact MGET", binary.BigEndian.Uint32(data[1:5]), len(data)-5, 1)
	if err != nil {
		return nil, err
	}
	offset := 5
	items := make([]any, 0, count)
	for i := 0; i < count; i++ {
		if offset >= len(data) {
			return nil, errors.New("ferricstore native compact MGET item is truncated")
		}
		present := data[offset]
		offset++
		switch present {
		case 0:
			items = append(items, nil)
		case 1:
			value, next, err := readNativeCompactBinary(data, offset)
			if err != nil {
				return nil, err
			}
			offset = next
			items = append(items, value)
		default:
			return nil, fmt.Errorf("ferricstore native compact MGET present tag %d is unsupported", present)
		}
	}
	if offset != len(data) {
		return nil, errors.New("ferricstore native compact MGET has trailing bytes")
	}
	return items, nil
}

func decodeNativeCompactKVMGetFixed(data []byte) ([]any, error) {
	if len(data) < 9 || data[0] != nativeCompactKVMGetFixed {
		return nil, errors.New("ferricstore native compact fixed MGET is invalid")
	}
	count, err := nativeBoundedItemCount("compact fixed MGET", binary.BigEndian.Uint32(data[1:5]), len(data)-9, 0)
	if err != nil {
		return nil, err
	}
	size := int(binary.BigEndian.Uint32(data[5:9]))
	offset := 9
	if uint64(count)*uint64(size) != uint64(len(data)-offset) {
		return nil, errors.New("ferricstore native compact fixed MGET payload length mismatch")
	}
	items := make([]any, 0, count)
	for i := 0; i < count; i++ {
		value := data[offset : offset+size : offset+size]
		offset += size
		items = append(items, value)
	}
	return items, nil
}

func decodeNativeCompactClaimJobs(data []byte) ([]ClaimedItem, error) {
	if len(data) < 5 || data[0] != nativeCompactFlowClaimJobs {
		return nil, errors.New("ferricstore native compact claim jobs is invalid")
	}
	count, err := nativeBoundedItemCount("compact claim jobs", binary.BigEndian.Uint32(data[1:5]), len(data)-5, 16)
	if err != nil {
		return nil, err
	}
	for _, layout := range []nativeCompactClaimLayout{
		nativeCompactClaimBase,
		nativeCompactClaimAttributes,
		nativeCompactClaimState,
		nativeCompactClaimStateAttributes,
	} {
		items, ok := tryDecodeNativeCompactClaimJobsLayout(data, 5, count, layout)
		if ok {
			return items, nil
		}
	}
	return nil, errors.New("ferricstore native compact claim jobs payload is invalid")
}

type nativeCompactClaimLayout byte

const (
	nativeCompactClaimBase nativeCompactClaimLayout = iota
	nativeCompactClaimAttributes
	nativeCompactClaimState
	nativeCompactClaimStateAttributes
)

func tryDecodeNativeCompactClaimJobsLayout(data []byte, offset, count int, layout nativeCompactClaimLayout) ([]ClaimedItem, bool) {
	items := make([]ClaimedItem, 0, count)
	for i := 0; i < count; i++ {
		id, next, err := readNativeCompactBinary(data, offset)
		if err != nil {
			return nil, false
		}
		offset = next
		partition, next, err := readNativeCompactOptionalBinary(data, offset)
		if err != nil {
			return nil, false
		}
		offset = next
		lease, next, err := readNativeCompactBinary(data, offset)
		if err != nil {
			return nil, false
		}
		offset = next
		if len(data)-offset < 8 {
			return nil, false
		}
		fencingRaw := binary.BigEndian.Uint64(data[offset : offset+8])
		if fencingRaw > math.MaxInt64 {
			return nil, false
		}
		fencing := int64(fencingRaw)
		offset += 8
		item := ClaimedItem{
			ID:           string(id),
			PartitionKey: string(partition),
			LeaseToken:   string(lease),
			FencingToken: fencing,
			State:        "running",
		}
		switch layout {
		case nativeCompactClaimAttributes:
			attrs, rest, err := decodeNativeOwnedValue(data[offset:])
			if err != nil {
				return nil, false
			}
			consumed := len(data[offset:]) - len(rest)
			offset += consumed
			attributes, ok := attrs.(map[string]any)
			if !ok {
				return nil, false
			}
			item.Attributes = attributes
		case nativeCompactClaimState:
			runState, next, err := readNativeCompactOptionalBinary(data, offset)
			if err != nil {
				return nil, false
			}
			offset = next
			item.RunState = string(runState)
		case nativeCompactClaimStateAttributes:
			runState, next, err := readNativeCompactOptionalBinary(data, offset)
			if err != nil {
				return nil, false
			}
			offset = next
			attrs, rest, err := decodeNativeOwnedValue(data[offset:])
			if err != nil {
				return nil, false
			}
			consumed := len(data[offset:]) - len(rest)
			offset += consumed
			attributes, ok := attrs.(map[string]any)
			if !ok {
				return nil, false
			}
			item.RunState = string(runState)
			item.Attributes = attributes
		}
		items = append(items, item)
	}
	return items, offset == len(data)
}

func decodeNativeCompactPipelineResponse(data []byte) ([]any, error) {
	if len(data) < 5 || data[0] != nativeCompactPipelineResponse {
		return nil, errors.New("ferricstore native compact pipeline response is truncated")
	}
	count, err := nativeBoundedItemCount("compact pipeline response", binary.BigEndian.Uint32(data[1:5]), len(data)-5, 1)
	if err != nil {
		return nil, err
	}
	offset := 5
	items := make([]any, 0, count)
	for i := 0; i < count; i++ {
		if offset >= len(data) {
			return nil, errors.New("ferricstore native compact pipeline item is truncated")
		}
		status := data[offset]
		offset++
		switch status {
		case 0:
			if offset >= len(data) {
				return nil, errors.New("ferricstore native compact pipeline ok item is truncated")
			}
			present := data[offset]
			offset++
			value, next, err := decodeNativeCompactPipelineOKValue(present, data, offset)
			if err != nil {
				return nil, err
			}
			offset = next
			items = append(items, []any{"ok", value})
		case 1, 2:
			reason, next, err := readNativeCompactBinary(data, offset)
			if err != nil {
				return nil, err
			}
			offset = next
			label := "error"
			if status == 1 {
				label = "busy"
			}
			items = append(items, []any{label, reason})
		default:
			return nil, fmt.Errorf("ferricstore native compact pipeline status %d is unsupported", status)
		}
	}
	if offset != len(data) {
		return nil, errors.New("ferricstore native compact pipeline response has trailing bytes")
	}
	return items, nil
}

func decodeNativeCompactPipelineOKValue(present byte, data []byte, offset int) (any, int, error) {
	switch present {
	case 0:
		return nil, offset, nil
	case 1:
		return readNativeCompactBinary(data, offset)
	default:
		return nil, offset, fmt.Errorf("ferricstore native compact pipeline value tag %d is unsupported", present)
	}
}

func readNativeCompactBinary(data []byte, offset int) ([]byte, int, error) {
	if len(data)-offset < 4 {
		return nil, offset, errors.New("ferricstore native compact binary length is truncated")
	}
	size := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4
	if len(data)-offset < size {
		return nil, offset, errors.New("ferricstore native compact binary value is truncated")
	}
	value := data[offset : offset+size : offset+size]
	return value, offset + size, nil
}

func readNativeCompactOptionalBinary(data []byte, offset int) ([]byte, int, error) {
	if len(data)-offset < 4 {
		return nil, offset, errors.New("ferricstore native compact optional binary length is truncated")
	}
	size := binary.BigEndian.Uint32(data[offset : offset+4])
	if size == nativeCompactNilU32 {
		return nil, offset + 4, nil
	}
	return readNativeCompactBinary(data, offset)
}
