package ferricstore

import (
	"bytes"
	"context"
	"errors"
	"math"
)

func (c *Client) tryCreateManyNativeCompact(ctx context.Context, opt CreateManyOptions, state string, now, runAt int64, mixed bool, wirePartition string) ([]FlowRecord, bool, error) {
	native, ok := c.exec.(*NativeExecutor)
	if !ok || !createManyCompactEligible(opt) {
		return nil, false, nil
	}
	buf := bytes.NewBuffer(make([]byte, 0, createManyCompactCapacity(opt, state, wirePartition, mixed)))
	switch {
	case mixed:
		buf.WriteByte(nativeCompactFlowCreateManyMixedRequest)
	case wirePartition == "" || wirePartition == "AUTO":
		buf.WriteByte(nativeCompactFlowCreateManyRequest)
	default:
		buf.WriteByte(nativeCompactFlowCreateManyPartitionRequest)
	}
	writeCompactString(buf, opt.Type)
	writeCompactString(buf, state)
	if buf.Bytes()[0] == nativeCompactFlowCreateManyPartitionRequest {
		writeCompactOptionalString(buf, wirePartition)
	}
	writeCompactInt64(buf, now)
	writeCompactInt64(buf, runAt)
	buf.WriteByte(boolPtrMarker(opt.Independent))
	buf.WriteByte(0)
	writeCompactU32(buf, uint32(len(opt.Items)))
	for _, item := range opt.Items {
		writeCompactString(buf, item.ID)
		if mixed {
			if item.PartitionKey == "" {
				return nil, true, errors.New("mixed create_many items require partition key")
			}
			writeCompactString(buf, item.PartitionKey)
		}
		encoded, err := c.encode(item.Payload)
		if err != nil {
			return nil, true, err
		}
		payload, ok := compactBytes(encoded)
		if !ok {
			return nil, false, nil
		}
		writeCompactBinary(buf, payload)
	}
	value, err := native.request(ctx, nativeOpFlowCreateMany, 1, buf.Bytes(), nativeFlagCustomPayload)
	if err != nil {
		return nil, true, err
	}
	records, err := recordsOrNil(value, c.codec)
	return records, true, err
}

func (c *Client) tryClaimDueNativeCompact(ctx context.Context, opt ClaimDueOptions, leaseMS int64, limit int) (any, bool, error) {
	native, ok := c.exec.(*NativeExecutor)
	if !ok || !claimDueCompactEligible(opt) {
		return nil, false, nil
	}
	if opt.NowMS != 0 && absInt64(opt.NowMS-nowMS()) > 10_000 {
		return nil, false, nil
	}
	blockMS := int64(-1)
	if opt.BlockMS != nil {
		blockMS = *opt.BlockMS
	}
	reclaimRatio := int64(0)
	if opt.ReclaimRatio != nil {
		reclaimRatio = *opt.ReclaimRatio
	}
	priority := int64(math.MinInt64)
	if opt.Priority != nil {
		priority = *opt.Priority
	}
	reclaimExpired := byte(0)
	if opt.ReclaimExpired != nil && *opt.ReclaimExpired {
		reclaimExpired = 1
	}
	returnMode := byte(1)
	if opt.IncludeState {
		if boolDefault(opt.IncludeAttributes, true) {
			returnMode = 4
		} else {
			returnMode = 2
		}
	} else if boolDefault(opt.IncludeAttributes, true) {
		returnMode = 3
	}

	buf := bytes.NewBuffer(make([]byte, 0, claimDueCompactCapacity(opt)))
	buf.WriteByte(nativeCompactFlowClaimDueRequest)
	writeCompactString(buf, opt.Type)
	if opt.State != "" {
		writeCompactOptionalString(buf, opt.State)
	} else {
		writeCompactOptionalBinary(buf, nil)
	}
	writeCompactString(buf, opt.Worker)
	writeCompactInt64(buf, leaseMS)
	writeCompactInt64(buf, int64(limit))
	writeCompactInt64(buf, blockMS)
	buf.WriteByte(reclaimExpired)
	writeCompactInt64(buf, reclaimRatio)
	writeCompactInt64(buf, priority)
	buf.WriteByte(returnMode)
	writeClaimPartitions(buf, opt.PartitionKey, opt.PartitionKeys)
	value, err := native.request(ctx, nativeOpFlowClaimDue, 1, buf.Bytes(), nativeFlagCustomPayload)
	return value, true, err
}

func (c *Client) tryCompleteManyNativeCompact(ctx context.Context, opt CompleteManyOptions, now int64) ([]FlowRecord, bool, error) {
	native, ok := c.exec.(*NativeExecutor)
	if !ok || !completeManyCompactEligible(opt) {
		return nil, false, nil
	}
	mixed := opt.PartitionKey == ""
	buf := bytes.NewBuffer(make([]byte, 0, completeManyCompactCapacity(opt)))
	buf.WriteByte(nativeCompactFlowCompleteManyOKRequest)
	if mixed {
		writeCompactOptionalBinary(buf, nil)
	} else {
		writeCompactOptionalBinary(buf, []byte(opt.PartitionKey))
	}
	writeCompactInt64(buf, now)
	buf.WriteByte(boolPtrMarker(opt.Independent))
	writeCompactU32(buf, uint32(len(opt.Items)))
	for _, item := range opt.Items {
		writeCompactString(buf, item.ID)
		if mixed {
			if item.PartitionKey == "" {
				return nil, true, errors.New("FLOW.COMPLETE_MANY mixed items require partition key")
			}
			writeCompactOptionalString(buf, item.PartitionKey)
		} else {
			writeCompactOptionalBinary(buf, nil)
		}
		writeCompactString(buf, item.LeaseToken)
		writeCompactInt64(buf, item.FencingToken)
	}
	value, err := native.request(ctx, nativeOpFlowCompleteMany, 1, buf.Bytes(), nativeFlagCustomPayload)
	if err != nil {
		return nil, true, err
	}
	records, err := recordsOrNil(value, c.codec)
	return records, true, err
}

func createManyCompactEligible(opt CreateManyOptions) bool {
	return opt.Priority == nil &&
		opt.Idempotent == nil &&
		opt.RetentionTTLMS == nil &&
		len(opt.Values) == 0 &&
		len(opt.ValueRefs) == 0 &&
		len(opt.Attributes) == 0 &&
		!anyCreateItemValues(opt.Items)
}

func claimDueCompactEligible(opt ClaimDueOptions) bool {
	return opt.JobOnly &&
		len(opt.States) == 0 &&
		opt.Payload == nil &&
		opt.PayloadMaxBytes == nil &&
		len(opt.Values) == 0 &&
		opt.ValueMaxBytes == nil
}

func completeManyCompactEligible(opt CompleteManyOptions) bool {
	return opt.Result == nil &&
		opt.Payload == nil &&
		opt.TTLMS == nil &&
		namedValuesEmpty(opt.NamedValues)
}

func namedValuesEmpty(values NamedValues) bool {
	return len(values.Values) == 0 &&
		len(values.ValueRefs) == 0 &&
		len(values.DropValues) == 0 &&
		len(values.OverrideValues) == 0 &&
		len(values.AttributesMerge) == 0 &&
		len(values.AttributesDelete) == 0
}

func boolPtrMarker(value *bool) byte {
	if value == nil {
		return 0
	}
	if *value {
		return 2
	}
	return 1
}

func writeClaimPartitions(buf *bytes.Buffer, partitionKey string, partitionKeys []string) {
	switch {
	case partitionKey != "":
		buf.WriteByte(1)
		writeCompactString(buf, partitionKey)
	case len(partitionKeys) > 0:
		buf.WriteByte(2)
		writeCompactU32(buf, uint32(len(partitionKeys)))
		for _, partition := range partitionKeys {
			writeCompactString(buf, partition)
		}
	default:
		buf.WriteByte(0)
	}
}

func writeCompactString(buf *bytes.Buffer, value string) {
	writeCompactU32(buf, uint32(len(value)))
	buf.WriteString(value)
}

func writeCompactOptionalString(buf *bytes.Buffer, value string) {
	writeCompactString(buf, value)
}

func createManyCompactCapacity(opt CreateManyOptions, state, wirePartition string, mixed bool) int {
	size := 64 + len(opt.Type) + len(state) + len(wirePartition)
	for _, item := range opt.Items {
		size += 12 + len(item.ID) + compactPayloadSize(item.Payload)
		if mixed {
			size += 4 + len(item.PartitionKey)
		}
	}
	return size
}

func claimDueCompactCapacity(opt ClaimDueOptions) int {
	size := 96 + len(opt.Type) + len(opt.State) + len(opt.Worker) + len(opt.PartitionKey)
	for _, partition := range opt.PartitionKeys {
		size += 4 + len(partition)
	}
	return size
}

func completeManyCompactCapacity(opt CompleteManyOptions) int {
	size := 48 + len(opt.PartitionKey)
	mixed := opt.PartitionKey == ""
	for _, item := range opt.Items {
		size += 24 + len(item.ID) + len(item.LeaseToken)
		if mixed {
			size += 4 + len(item.PartitionKey)
		}
	}
	return size
}

func compactPayloadSize(value any) int {
	switch v := value.(type) {
	case nil:
		return 4
	case []byte:
		return 4 + len(v)
	case string:
		return 4 + len(v)
	default:
		return 64
	}
}
