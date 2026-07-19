package ferricstore

import (
	"errors"
	"fmt"
	"math"
)

const nativeCompactFlowValueMGetRequest = 0x9D

type nativeFlowValueMGetPayload struct {
	refs     []any
	maxBytes int64
}

func (p nativeFlowValueMGetPayload) nativePipelineBody() (any, error) {
	refs := make([]any, len(p.refs))
	for index, ref := range p.refs {
		switch ref.(type) {
		case string, []byte:
			refs[index] = ref
		default:
			text, ok := commandText(ref)
			if !ok || text == "" {
				return nil, fmt.Errorf("FLOW.VALUE.MGET reference must be non-empty text, got %T", ref)
			}
			refs[index] = text
		}
	}
	body := map[string]any{"refs": refs}
	if p.maxBytes != math.MinInt64 {
		body["max_bytes"] = p.maxBytes
	}
	return body, nil
}

func buildFlowValueMGetNative(args []any) (nativeCommand, bool, error) {
	if len(args) == 0 {
		return nativeCommand{}, true, errors.New("FLOW.VALUE.MGET requires at least one reference")
	}
	refs := args
	maxBytes := int64(math.MinInt64)
	if len(args) >= 2 {
		option := commandPart(args[len(args)-2])
		if option == "MAX_BYTES" || option == "MAXBYTES" {
			value, err := topologyInteger(args[len(args)-1], "FLOW.VALUE.MGET MAX_BYTES")
			if err != nil || value < 0 {
				return nativeCommand{}, true, errors.New("FLOW.VALUE.MGET MAX_BYTES must be a non-negative integer")
			}
			maxBytes = value
			refs = args[:len(args)-2]
		}
	}
	if len(refs) == 0 {
		return nativeCommand{}, true, errors.New("FLOW.VALUE.MGET requires at least one reference")
	}
	for _, ref := range refs {
		if text, ok := commandText(ref); !ok || text == "" {
			return nativeCommand{}, true, fmt.Errorf("FLOW.VALUE.MGET reference must be non-empty text, got %T", ref)
		}
	}
	return nativeCommand{
		name: "FLOW.VALUE.MGET", opcode: nativeOpFlowValueMGet, laneID: 1,
		payload: nativeFlowValueMGetPayload{refs: refs, maxBytes: maxBytes},
		flags:   nativeFlagCustomPayload,
	}, true, nil
}

func (p nativeFlowValueMGetPayload) encodeNativeCustomPayload(limit int) ([]byte, error) {
	if len(p.refs) > nativeMaxContainerItems || uint64(len(p.refs)) > math.MaxUint32 {
		return nil, nativeEncodeLimitError{limit: limit}
	}
	w, err := newNativeCompactPayloadWriter(limit)
	if err != nil {
		return nil, err
	}
	if err := w.byte(nativeCompactFlowValueMGetRequest); err != nil {
		return nil, err
	}
	if err := w.int64(p.maxBytes); err != nil {
		return nil, err
	}
	if err := w.uint32(uint32(len(p.refs))); err != nil {
		return nil, err
	}
	for _, ref := range p.refs {
		switch value := ref.(type) {
		case string:
			err = w.binaryString(value)
		case []byte:
			err = w.binaryBytes(value)
		default:
			text, _ := commandText(ref)
			err = w.binaryString(text)
		}
		if err != nil {
			return nil, err
		}
	}
	return w.bytes(), nil
}
