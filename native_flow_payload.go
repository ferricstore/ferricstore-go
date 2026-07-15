package ferricstore

import (
	"encoding/binary"
	"fmt"
	"math"
)

type nativeCustomPayloadEncoder interface {
	encodeNativeCustomPayload(limit int) ([]byte, error)
}

type nativeCompactPayloadWriter struct {
	buf nativeEncodeBuffer
}

func newNativeCompactPayloadWriter(limit int) (*nativeCompactPayloadWriter, error) {
	if limit <= 0 {
		return nil, nativeEncodeLimitError{limit: limit}
	}
	return &nativeCompactPayloadWriter{buf: nativeEncodeBuffer{limit: limit}}, nil
}

func (w *nativeCompactPayloadWriter) byte(value byte) error {
	return w.buf.writeByte(value)
}

func (w *nativeCompactPayloadWriter) uint32(value uint32) error {
	var raw [4]byte
	binary.BigEndian.PutUint32(raw[:], value)
	return w.buf.write(raw[:])
}

func (w *nativeCompactPayloadWriter) int64(value int64) error {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], uint64(value))
	return w.buf.write(raw[:])
}

func (w *nativeCompactPayloadWriter) binary(value any) error {
	switch v := value.(type) {
	case []byte:
		return w.binaryBytes(v)
	case string:
		return w.binaryString(v)
	default:
		return fmt.Errorf("ferricstore compact payload value has type %T, expected string or []byte", value)
	}
}

func (w *nativeCompactPayloadWriter) binaryBytes(value []byte) error {
	if len(value) > math.MaxUint32 {
		return nativeEncodeLimitError{limit: w.buf.limit}
	}
	if err := w.buf.ensure(4 + len(value)); err != nil {
		return err
	}
	if err := w.uint32(uint32(len(value))); err != nil {
		return err
	}
	return w.buf.write(value)
}

func (w *nativeCompactPayloadWriter) binaryString(value string) error {
	if len(value) > math.MaxUint32 {
		return nativeEncodeLimitError{limit: w.buf.limit}
	}
	if err := w.buf.ensure(4 + len(value)); err != nil {
		return err
	}
	if err := w.uint32(uint32(len(value))); err != nil {
		return err
	}
	return w.buf.writeString(value)
}

func (w *nativeCompactPayloadWriter) optional(value any) error {
	if value == nil {
		return w.uint32(nativeCompactNilU32)
	}
	return w.binary(value)
}

func (w *nativeCompactPayloadWriter) bytes() []byte {
	return w.buf.Bytes()
}

func isCompactPayloadValue(value any) bool {
	switch value.(type) {
	case string, []byte:
		return true
	default:
		return false
	}
}

type nativeFlowCreateManyPayload struct {
	kind         byte
	flowType     any
	state        any
	partition    any
	now          int64
	runAt        int64
	independent  byte
	itemArgs     []any
	itemWidth    int
	typedItems   []CreateItem
	payloadCodec Codec
	mixed        bool
}

func (p nativeFlowCreateManyPayload) encodeNativeCustomPayload(limit int) ([]byte, error) {
	w, err := newNativeCompactPayloadWriter(limit)
	if err != nil {
		return nil, err
	}
	if err := w.byte(p.kind); err != nil {
		return nil, err
	}
	if err := w.binary(p.flowType); err != nil {
		return nil, err
	}
	if err := w.binary(p.state); err != nil {
		return nil, err
	}
	if p.kind == nativeCompactFlowCreateManyPartitionRequest {
		if err := w.optional(p.partition); err != nil {
			return nil, err
		}
	}
	if err := w.int64(p.now); err != nil {
		return nil, err
	}
	if err := w.int64(p.runAt); err != nil {
		return nil, err
	}
	if err := w.byte(p.independent); err != nil {
		return nil, err
	}
	if err := w.byte(0); err != nil {
		return nil, err
	}
	count := 0
	if p.itemWidth > 0 {
		count = len(p.itemArgs) / p.itemWidth
	}
	if p.typedItems != nil {
		count = len(p.typedItems)
	}
	if count > math.MaxUint32 {
		return nil, nativeEncodeLimitError{limit: limit}
	}
	if err := w.uint32(uint32(count)); err != nil {
		return nil, err
	}
	if p.typedItems != nil {
		for _, item := range p.typedItems {
			if err := p.writeTypedItem(w, item); err != nil {
				return nil, err
			}
		}
		return w.bytes(), nil
	}
	for offset := 0; offset < len(p.itemArgs); offset += p.itemWidth {
		if err := w.binary(p.itemArgs[offset]); err != nil {
			return nil, err
		}
		if p.mixed {
			if err := w.binary(p.itemArgs[offset+1]); err != nil {
				return nil, err
			}
		}
		if err := w.binary(p.itemArgs[offset+p.itemWidth-1]); err != nil {
			return nil, err
		}
	}
	return w.bytes(), nil
}

func (p nativeFlowCreateManyPayload) writeTypedItem(w *nativeCompactPayloadWriter, item CreateItem) error {
	if err := w.binaryString(item.ID); err != nil {
		return err
	}
	if p.mixed {
		if item.PartitionKey == "" {
			return fmt.Errorf("mixed create_many items require partition key")
		}
		if err := w.binaryString(item.PartitionKey); err != nil {
			return err
		}
	}
	encoded, err := p.payloadCodec.Encode(item.Payload)
	if err != nil {
		return err
	}
	return w.binary(encoded)
}

type nativeFlowClaimDuePayload struct {
	flowType         any
	state            any
	worker           any
	leaseMS          int64
	limit            int64
	blockMS          int64
	reclaimExpired   byte
	reclaimRatio     int64
	priority         int64
	returnMode       byte
	partitionMode    byte
	partition        any
	partitions       []any
	partitionStrings []string
}

func (p nativeFlowClaimDuePayload) encodeNativeCustomPayload(limit int) ([]byte, error) {
	w, err := newNativeCompactPayloadWriter(limit)
	if err != nil {
		return nil, err
	}
	if err := w.byte(nativeCompactFlowClaimDueRequest); err != nil {
		return nil, err
	}
	if err := w.binary(p.flowType); err != nil {
		return nil, err
	}
	if err := w.optional(p.state); err != nil {
		return nil, err
	}
	if err := w.binary(p.worker); err != nil {
		return nil, err
	}
	if err := w.int64(p.leaseMS); err != nil {
		return nil, err
	}
	if err := w.int64(p.limit); err != nil {
		return nil, err
	}
	if err := w.int64(p.blockMS); err != nil {
		return nil, err
	}
	if err := w.byte(p.reclaimExpired); err != nil {
		return nil, err
	}
	if err := w.int64(p.reclaimRatio); err != nil {
		return nil, err
	}
	if err := w.int64(p.priority); err != nil {
		return nil, err
	}
	if err := w.byte(p.returnMode); err != nil {
		return nil, err
	}
	if err := w.byte(p.partitionMode); err != nil {
		return nil, err
	}
	if err := p.writePartitions(w); err != nil {
		return nil, err
	}
	return w.bytes(), nil
}

func (p nativeFlowClaimDuePayload) writePartitions(w *nativeCompactPayloadWriter) error {
	switch p.partitionMode {
	case 0:
		return nil
	case 1:
		return w.binary(p.partition)
	case 2:
		count := len(p.partitions)
		if p.partitionStrings != nil {
			count = len(p.partitionStrings)
		}
		if count > math.MaxUint32 {
			return nativeEncodeLimitError{limit: w.buf.limit}
		}
		if err := w.uint32(uint32(count)); err != nil {
			return err
		}
		if p.partitionStrings != nil {
			for _, partition := range p.partitionStrings {
				if err := w.binaryString(partition); err != nil {
					return err
				}
			}
			return nil
		}
		for _, partition := range p.partitions {
			if err := w.binary(partition); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("ferricstore compact claim has invalid partition mode %d", p.partitionMode)
	}
}

type nativeFlowCompleteManyPayload struct {
	partition   any
	now         int64
	independent byte
	itemArgs    []any
	itemWidth   int
	typedItems  []ClaimedItem
	mixed       bool
}

func (p nativeFlowCompleteManyPayload) encodeNativeCustomPayload(limit int) ([]byte, error) {
	w, err := newNativeCompactPayloadWriter(limit)
	if err != nil {
		return nil, err
	}
	if err := w.byte(nativeCompactFlowCompleteManyOKRequest); err != nil {
		return nil, err
	}
	if err := w.optional(p.partition); err != nil {
		return nil, err
	}
	if err := w.int64(p.now); err != nil {
		return nil, err
	}
	if err := w.byte(p.independent); err != nil {
		return nil, err
	}
	count := 0
	if p.itemWidth > 0 {
		count = len(p.itemArgs) / p.itemWidth
	}
	if p.typedItems != nil {
		count = len(p.typedItems)
	}
	if count > math.MaxUint32 {
		return nil, nativeEncodeLimitError{limit: limit}
	}
	if err := w.uint32(uint32(count)); err != nil {
		return nil, err
	}
	if p.typedItems != nil {
		for _, item := range p.typedItems {
			if err := p.writeTypedItem(w, item); err != nil {
				return nil, err
			}
		}
		return w.bytes(), nil
	}
	for offset := 0; offset < len(p.itemArgs); offset += p.itemWidth {
		leaseOffset := 1
		var partition any
		if p.mixed {
			partition = p.itemArgs[offset+1]
			leaseOffset = 2
		}
		fencing, _ := responseInt64(p.itemArgs[offset+leaseOffset+1], nil)
		if err := p.writeItem(
			w,
			p.itemArgs[offset],
			partition,
			p.itemArgs[offset+leaseOffset],
			fencing,
		); err != nil {
			return nil, err
		}
	}
	return w.bytes(), nil
}

func (p nativeFlowCompleteManyPayload) writeTypedItem(w *nativeCompactPayloadWriter, item ClaimedItem) error {
	if p.mixed && item.PartitionKey == "" {
		return errorsNewMixedCompletePartition()
	}
	return p.writeItem(w, item.ID, item.PartitionKey, item.LeaseToken, item.FencingToken)
}

func (p nativeFlowCompleteManyPayload) writeItem(w *nativeCompactPayloadWriter, id, partition, lease any, fencing int64) error {
	if err := w.binary(id); err != nil {
		return err
	}
	if p.mixed {
		if err := w.optional(partition); err != nil {
			return err
		}
	} else if err := w.optional(nil); err != nil {
		return err
	}
	if err := w.binary(lease); err != nil {
		return err
	}
	return w.int64(fencing)
}

func errorsNewMixedCompletePartition() error {
	return fmt.Errorf("FLOW.COMPLETE_MANY mixed items require partition key")
}
