package ferricstore

import (
	"context"
	"errors"
	"math"
	"time"
)

func (c *Client) tryCreateManyNativeCompact(ctx context.Context, opt CreateManyOptions, state string, now, runAt int64, mixed bool, wirePartition string) ([]FlowRecord, bool, error) {
	native, ok := c.exec.(*NativeExecutor)
	if !ok || !createManyCompactEligible(opt) || !createManyCompactCodecEligible(c.codec, opt.Items) {
		return nil, false, nil
	}
	if err := c.legacyGate.readLock(ctx); err != nil {
		return nil, true, err
	}
	defer c.legacyGate.readUnlock()
	if c.currentLegacySession() != nil {
		return nil, false, nil
	}
	var kind byte
	switch {
	case mixed:
		kind = nativeCompactFlowCreateManyMixedRequest
	case wirePartition == "" || wirePartition == "AUTO":
		kind = nativeCompactFlowCreateManyRequest
	default:
		kind = nativeCompactFlowCreateManyPartitionRequest
	}
	for _, item := range opt.Items {
		if mixed && item.PartitionKey == "" {
			return nil, true, errors.New("mixed create_many items require partition key")
		}
	}
	payload := nativeFlowCreateManyPayload{
		kind: kind, flowType: opt.Type, state: state, partition: wirePartition,
		now: now, runAt: runAt, independent: boolPtrMarker(opt.Independent),
		typedItems: opt.Items, payloadCodec: c.codec, mixed: mixed,
	}
	value, err := native.request(ctx, nativeOpFlowCreateMany, nativeAutoLaneID, payload, nativeFlagCustomPayload)
	if err != nil {
		return nil, true, err
	}
	records, err := recordsOrNil(value, c.codec, len(opt.Items))
	return records, true, err
}

func createManyCompactCodecEligible(codec Codec, items []CreateItem) bool {
	switch codec.(type) {
	case JSONCodec, *JSONCodec:
		return true
	case StringCodec, *StringCodec:
		for _, item := range items {
			if item.Payload == nil {
				return false
			}
		}
		return true
	case RawCodec, *RawCodec:
		for _, item := range items {
			if !isCompactPayloadValue(item.Payload) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func (c *Client) tryClaimDueNativeCompact(ctx context.Context, opt ClaimDueOptions, leaseMS int64, limit int) (any, bool, error) {
	native, ok := c.exec.(*NativeExecutor)
	if !ok || !claimDueCompactEligible(opt) {
		return nil, false, nil
	}
	if err := c.legacyGate.readLock(ctx); err != nil {
		return nil, true, err
	}
	defer c.legacyGate.readUnlock()
	if c.currentLegacySession() != nil {
		return nil, false, nil
	}
	blockMS := int64(-1)
	if opt.BlockMS != nil {
		blockMS = *opt.BlockMS
	}
	reclaimRatio := nativeFlowClaimDefaultReclaimRatio
	if opt.ReclaimRatio != nil {
		reclaimRatio = *opt.ReclaimRatio
	}
	priority := int64(math.MinInt64)
	if opt.Priority != nil {
		priority = *opt.Priority
	}
	reclaimExpired := nativeFlowClaimDefaultReclaimExpired
	if opt.ReclaimExpired != nil {
		reclaimExpired = 0
		if *opt.ReclaimExpired {
			reclaimExpired = 1
		}
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

	payload := nativeFlowClaimDuePayload{
		flowType: opt.Type, worker: opt.Worker, leaseMS: leaseMS, limit: int64(limit),
		blockMS: blockMS, reclaimExpired: reclaimExpired, reclaimRatio: reclaimRatio,
		priority: priority, returnMode: returnMode,
	}
	if opt.State != "" {
		payload.state = opt.State
	}
	switch {
	case opt.PartitionKey != "":
		payload.partitionMode = 1
		payload.partition = opt.PartitionKey
	case len(opt.PartitionKeys) > 0:
		payload.partitionMode = 2
		payload.partitionStrings = opt.PartitionKeys
	}
	budget := nativeBlockingMillisecondsBudget(blockMS)
	value, err := native.requestWithBudget(ctx, nativeOpFlowClaimDue, nativeAutoLaneID, payload, nativeFlagCustomPayload, budget)
	return value, true, err
}

func (c *Client) tryCompleteManyNativeCompact(ctx context.Context, opt CompleteManyOptions, now int64) ([]FlowRecord, bool, error) {
	native, ok := c.exec.(*NativeExecutor)
	if !ok || !completeManyCompactEligible(opt) {
		return nil, false, nil
	}
	if err := c.legacyGate.readLock(ctx); err != nil {
		return nil, true, err
	}
	defer c.legacyGate.readUnlock()
	if c.currentLegacySession() != nil {
		return nil, false, nil
	}
	mixed := opt.PartitionKey == ""
	for _, item := range opt.Items {
		if mixed && item.PartitionKey == "" {
			return nil, true, errors.New("FLOW.COMPLETE_MANY mixed items require partition key")
		}
	}
	var partition any
	if !mixed {
		partition = opt.PartitionKey
	}
	payload := nativeFlowCompleteManyPayload{
		partition: partition, now: now, independent: boolPtrMarker(opt.Independent),
		typedItems: opt.Items, mixed: mixed,
	}
	value, err := native.request(ctx, nativeOpFlowCompleteMany, nativeAutoLaneID, payload, nativeFlagCustomPayload)
	if err != nil {
		return nil, true, err
	}
	records, err := recordsOrNil(value, c.codec, len(opt.Items))
	return records, true, err
}

func createManyCompactEligible(opt CreateManyOptions) bool {
	return opt.Priority == nil &&
		opt.Idempotent == nil &&
		opt.RetentionTTLMS == nil &&
		opt.MaxActiveMS == nil &&
		len(opt.Values) == 0 &&
		len(opt.ValueRefs) == 0 &&
		len(opt.Attributes) == 0 &&
		!anyCreateItemAttributes(opt.Items) &&
		len(opt.StateMeta) == 0 &&
		!anyCreateItemStateMeta(opt.Items) &&
		!anyCreateItemValues(opt.Items) &&
		!anyCreateItemMaxActive(opt.Items)
}

func claimDueCompactEligible(opt ClaimDueOptions) bool {
	return opt.JobOnly &&
		opt.NowMS == 0 &&
		len(opt.States) == 0 &&
		opt.Payload == nil &&
		opt.PayloadMaxBytes == nil &&
		len(opt.Values) == 0
}

func nativeBlockingMillisecondsBudget(blockMS int64) nativeRequestBudget {
	if blockMS <= 0 {
		return nativeRequestBudget{}
	}
	if blockMS > int64(time.Duration(1<<63-1)/time.Millisecond) {
		return nativeRequestBudget{disableDefault: true}
	}
	return nativeRequestBudget{extension: time.Duration(blockMS) * time.Millisecond}
}

func completeManyCompactEligible(opt CompleteManyOptions) bool {
	return opt.Result == nil &&
		opt.Payload == nil &&
		opt.TTLMS == nil &&
		len(opt.StateMeta) == 0 &&
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
