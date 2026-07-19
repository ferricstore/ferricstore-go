package ferricstore

import (
	"context"
)

func (c *Client) Create(ctx context.Context, opt CreateOptions) (*FlowRecord, error) {
	if err := validateCreateOptions(opt); err != nil {
		return nil, err
	}
	state := opt.State
	if state == "" {
		state = "queued"
	}
	now := opt.NowMS
	if now == 0 {
		now = nowMS()
	}
	runAt := opt.RunAtMS
	if runAt == 0 {
		runAt = now
	}
	args := []any{"FLOW.CREATE", opt.ID, "TYPE", opt.Type, "STATE", state, "NOW", now}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	if err := c.appendEncoded(&args, "PAYLOAD", opt.Payload); err != nil {
		return nil, err
	}
	appendOpt(&args, "PARENT_FLOW_ID", opt.ParentFlowID)
	appendOpt(&args, "ROOT_FLOW_ID", opt.RootFlowID)
	appendOpt(&args, "CORRELATION_ID", opt.CorrelationID)
	appendOpt(&args, "RUN_AT", runAt)
	appendInt64Ptr(&args, "PRIORITY", opt.Priority)
	appendBoolPtr(&args, "IDEMPOTENT", opt.Idempotent)
	appendInt64Ptr(&args, "RETENTION_TTL_MS", opt.RetentionTTLMS)
	if err := appendFlowMaxActiveMS(&args, opt.MaxActiveMS); err != nil {
		return nil, err
	}
	appendAttributes(&args, opt.Attributes, nil, nil)
	appendStateMeta(&args, opt.StateMeta)
	if err := c.appendNamedValues(&args, NamedValues{Values: opt.Values, ValueRefs: opt.ValueRefs}); err != nil {
		return nil, err
	}
	value, err := c.typedReplyOrQueue(ctx, !opt.ReturnRecord, args...)
	if err != nil || !opt.ReturnRecord {
		return nil, err
	}
	record, err := recordOrNil(value, c.codec)
	return c.recordOrGet(ctx, record, err, opt.ID, opt.PartitionKey)
}

func (c *Client) Enqueue(ctx context.Context, opt CreateOptions) (*FlowRecord, error) {
	if opt.State == "" {
		opt.State = "queued"
	}
	return c.Create(ctx, opt)
}

func (c *Client) CreateMany(ctx context.Context, opt CreateManyOptions) ([]FlowRecord, error) {
	if len(opt.Items) == 0 {
		return nil, nil
	}
	if err := validateCreateManyOptions(opt); err != nil {
		return nil, err
	}
	mixed, err := createManyPartitionMode(opt.PartitionKey, opt.Items)
	if err != nil {
		return nil, err
	}
	state := opt.State
	if state == "" {
		state = "queued"
	}
	now := opt.NowMS
	if now == 0 {
		now = nowMS()
	}
	runAt := opt.RunAtMS
	if runAt == 0 {
		runAt = now
	}
	wirePartition := opt.PartitionKey
	if mixed {
		wirePartition = "MIXED"
	} else if wirePartition == "" {
		wirePartition = "AUTO"
	}
	if records, ok, err := c.tryCreateManyNativeCompact(ctx, opt, state, now, runAt, mixed, wirePartition); ok || err != nil {
		return records, err
	}
	args := []any{"FLOW.CREATE_MANY", wirePartition, "TYPE", opt.Type, "STATE", state, "NOW", now}
	appendOpt(&args, "RUN_AT", runAt)
	appendInt64Ptr(&args, "PRIORITY", opt.Priority)
	appendBoolPtr(&args, "IDEMPOTENT", opt.Idempotent)
	appendBoolPtr(&args, "INDEPENDENT", opt.Independent)
	appendInt64Ptr(&args, "RETENTION_TTL_MS", opt.RetentionTTLMS)
	if err := appendFlowMaxActiveMS(&args, opt.MaxActiveMS); err != nil {
		return nil, err
	}
	appendAttributes(&args, opt.Attributes, nil, nil)
	appendStateMeta(&args, opt.StateMeta)
	mappedItems := anyCreateItemMaxActive(opt.Items) ||
		anyCreateItemAttributes(opt.Items) || anyCreateItemStateMeta(opt.Items)
	extended := anyCreateItemValues(opt.Items)
	if mappedItems {
		args = append(args, "ITEMS_MAPS", len(opt.Items))
		for _, item := range opt.Items {
			mapped, err := c.createManyItemMap(item, opt)
			if err != nil {
				return nil, err
			}
			args = append(args, mapped)
		}
	} else if extended {
		args = append(args, "ITEMS_EXT", len(opt.Items))
		for _, item := range opt.Items {
			partition := "-"
			if mixed {
				partition = item.PartitionKey
			}
			encoded, err := c.encode(item.Payload)
			if err != nil {
				return nil, err
			}
			args = append(args, item.ID, partition, encoded)
			if err := c.appendNamedCounts(&args, mergeValues(opt.Values, item.Values), mergeRefs(opt.ValueRefs, item.ValueRefs)); err != nil {
				return nil, err
			}
		}
	} else {
		if err := c.appendNamedValues(&args, NamedValues{Values: opt.Values, ValueRefs: opt.ValueRefs}); err != nil {
			return nil, err
		}
		args = append(args, "ITEMS")
		for _, item := range opt.Items {
			encoded, err := c.encode(item.Payload)
			if err != nil {
				return nil, err
			}
			if mixed {
				args = append(args, item.ID, item.PartitionKey, encoded)
			} else {
				args = append(args, item.ID, encoded)
			}
		}
	}
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsOrNil(value, c.codec, len(opt.Items))
}

func (c *Client) EnqueueMany(ctx context.Context, opt CreateManyOptions) ([]FlowRecord, error) {
	if opt.State == "" {
		opt.State = "queued"
	}
	return c.CreateMany(ctx, opt)
}

func (c *Client) appendNamedCounts(args *[]any, values map[string]any, refs map[string]string) error {
	*args = append(*args, len(values))
	if keys := deterministicMapKeysForCodec(values, c.codec); keys != nil {
		for _, name := range keys {
			encoded, err := c.encode(values[name])
			if err != nil {
				return err
			}
			*args = append(*args, name, encoded)
		}
	} else {
		for name, value := range values {
			encoded, err := c.encode(value)
			if err != nil {
				return err
			}
			*args = append(*args, name, encoded)
		}
	}
	*args = append(*args, len(refs))
	for name, ref := range refs {
		*args = append(*args, name, ref)
	}
	return nil
}

func (c *Client) Signal(ctx context.Context, opt SignalOptions) (any, error) {
	if err := validateSignalOptions(opt); err != nil {
		return nil, err
	}
	args := []any{"FLOW.SIGNAL", opt.ID, "SIGNAL", opt.Signal}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendOpt(&args, "IDEMPOTENCY", opt.IdempotencyKey)
	for _, state := range opt.IfStates {
		appendOpt(&args, "IF_STATE", state)
	}
	appendOpt(&args, "TRANSITION_TO", opt.TransitionTo)
	if opt.RunAtMS != 0 {
		appendOpt(&args, "RUN_AT", opt.RunAtMS)
	}
	now := opt.NowMS
	if now == 0 {
		now = nowMS()
	}
	appendOpt(&args, "NOW", now)
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	return c.typedReply(ctx, args...)
}

func (c *Client) FlowSignal(ctx context.Context, opt SignalOptions) (any, error) {
	return c.Signal(ctx, opt)
}

func (c *Client) StartAndClaim(ctx context.Context, opt StartAndClaimOptions) (*FlowRecord, error) {
	if err := validateStartAndClaimOptions(opt); err != nil {
		return nil, err
	}
	leaseMS := opt.LeaseMS
	if leaseMS == 0 {
		leaseMS = 30000
	}
	now := opt.NowMS
	if now == 0 {
		now = nowMS()
	}
	args := []any{
		"FLOW.START_AND_CLAIM", opt.ID,
		"TYPE", opt.Type,
		"INITIAL_STATE", opt.InitialState,
		"WORKER", opt.Worker,
		"LEASE_MS", leaseMS,
		"NOW", now,
	}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	if err := c.appendEncoded(&args, "PAYLOAD", opt.Payload); err != nil {
		return nil, err
	}
	appendOpt(&args, "PARENT_FLOW_ID", opt.ParentFlowID)
	appendOpt(&args, "ROOT_FLOW_ID", opt.RootFlowID)
	appendOpt(&args, "CORRELATION_ID", opt.CorrelationID)
	appendInt64Ptr(&args, "PRIORITY", opt.Priority)
	appendInt64Ptr(&args, "RETENTION_TTL_MS", opt.RetentionTTLMS)
	if err := appendFlowMaxActiveMS(&args, opt.MaxActiveMS); err != nil {
		return nil, err
	}
	appendAttributes(&args, opt.Attributes, nil, nil)
	appendStateMeta(&args, opt.StateMeta)
	if err := c.appendNamedValues(&args, NamedValues{Values: opt.Values, ValueRefs: opt.ValueRefs}); err != nil {
		return nil, err
	}
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	record, err := recordOrNil(value, c.codec)
	return c.recordOrGet(ctx, record, err, opt.ID, opt.PartitionKey)
}

func (c *Client) ClaimDue(ctx context.Context, opt ClaimDueOptions) ([]FlowRecord, error) {
	value, err := c.claimDue(ctx, opt)
	if err != nil {
		return nil, err
	}
	if err := validateFlowResponseLimit("FLOW.CLAIM_DUE", value, opt.Limit); err != nil {
		return nil, err
	}
	return recordsFromNative(value, c.codec)
}

func (c *Client) ClaimJobs(ctx context.Context, opt ClaimDueOptions) ([]ClaimedItem, error) {
	opt.JobOnly = true
	value, err := c.claimDue(ctx, opt)
	if err != nil {
		return nil, err
	}
	if err := validateFlowResponseLimit("FLOW.CLAIM_DUE", value, opt.Limit); err != nil {
		return nil, err
	}
	return claimedItemsFromNative(value, c.codec)
}

func (c *Client) claimDue(ctx context.Context, opt ClaimDueOptions) (any, error) {
	if err := validateClaimDueOptions(opt); err != nil {
		return nil, err
	}
	leaseMS := opt.LeaseMS
	if leaseMS == 0 {
		leaseMS = 30000
	}
	limit := opt.Limit
	if limit == 0 {
		limit = 1
	}
	now := opt.NowMS
	if now == 0 {
		now = nowMS()
	}
	args := []any{"FLOW.CLAIM_DUE", opt.Type}
	if len(opt.States) > 0 {
		for _, state := range opt.States {
			appendOpt(&args, "STATE", state)
		}
	} else {
		appendOpt(&args, "STATE", opt.State)
	}
	args = append(args, "WORKER", opt.Worker, "LEASE_MS", leaseMS, "LIMIT", limit)
	appendOpt(&args, "NOW", now)
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	if len(opt.PartitionKeys) > 0 {
		args = append(args, "PARTITIONS", len(opt.PartitionKeys))
		for _, key := range opt.PartitionKeys {
			args = append(args, key)
		}
	}
	appendInt64Ptr(&args, "PRIORITY", opt.Priority)
	if value, ok, err := c.tryClaimDueNativeCompact(ctx, opt, leaseMS, limit); ok || err != nil {
		return value, err
	}
	if opt.JobOnly {
		if opt.IncludeState {
			if boolDefault(opt.IncludeAttributes, true) {
				appendOpt(&args, "RETURN", "JOBS_COMPACT_STATE_ATTRS")
			} else {
				appendOpt(&args, "RETURN", "JOBS_COMPACT_STATE")
			}
		} else if boolDefault(opt.IncludeAttributes, true) {
			appendOpt(&args, "RETURN", "JOBS_COMPACT_ATTRS")
		} else {
			appendOpt(&args, "RETURN", "JOBS_COMPACT")
		}
	}
	appendInt64Ptr(&args, "BLOCK", opt.BlockMS)
	appendPayloadRead(&args, opt.Payload, opt.PayloadMaxBytes)
	appendValueReturn(&args, opt.Values)
	appendBoolPtr(&args, "RECLAIM_EXPIRED", opt.ReclaimExpired)
	appendInt64Ptr(&args, "RECLAIM_RATIO", opt.ReclaimRatio)
	return c.typedReply(ctx, args...)
}

func (c *Client) Reclaim(ctx context.Context, opt ReclaimOptions) ([]FlowRecord, error) {
	value, err := c.reclaim(ctx, opt)
	if err != nil {
		return nil, err
	}
	if err := validateFlowResponseLimit("FLOW.RECLAIM", value, opt.Limit); err != nil {
		return nil, err
	}
	return recordsFromNative(value, c.codec)
}

func (c *Client) ReclaimJobs(ctx context.Context, opt ReclaimOptions) ([]ClaimedItem, error) {
	opt.JobOnly = true
	value, err := c.reclaim(ctx, opt)
	if err != nil {
		return nil, err
	}
	if err := validateFlowResponseLimit("FLOW.RECLAIM", value, opt.Limit); err != nil {
		return nil, err
	}
	return claimedItemsFromNative(value, c.codec)
}

func (c *Client) reclaim(ctx context.Context, opt ReclaimOptions) (any, error) {
	if err := validateReclaimOptions(opt); err != nil {
		return nil, err
	}
	leaseMS := opt.LeaseMS
	if leaseMS == 0 {
		leaseMS = 30000
	}
	limit := opt.Limit
	if limit == 0 {
		limit = 1
	}
	now := opt.NowMS
	if now == 0 {
		now = nowMS()
	}
	args := []any{"FLOW.RECLAIM", opt.Type, "WORKER", opt.Worker, "LEASE_MS", leaseMS, "LIMIT", limit, "NOW", now}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	if len(opt.PartitionKeys) > 0 {
		args = append(args, "PARTITIONS", len(opt.PartitionKeys))
		for _, key := range opt.PartitionKeys {
			args = append(args, key)
		}
	}
	appendInt64Ptr(&args, "PRIORITY", opt.Priority)
	if opt.JobOnly {
		if boolDefault(opt.IncludeAttributes, true) {
			appendOpt(&args, "RETURN", "JOBS_COMPACT_ATTRS")
		} else {
			appendOpt(&args, "RETURN", "JOBS_COMPACT")
		}
	}
	appendPayloadRead(&args, opt.Payload, opt.PayloadMaxBytes)
	appendValueReturn(&args, opt.Values)
	return c.typedReply(ctx, args...)
}
