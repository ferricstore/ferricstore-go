package ferricstore

import (
	"context"
	"fmt"
)

func (c *Client) Create(ctx context.Context, opt CreateOptions) (*FlowRecord, error) {
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
	attrs, err := sharedCreateManyAttributes(opt.Items, opt.Attributes)
	if err != nil {
		return nil, err
	}
	appendAttributes(&args, attrs, nil, nil)
	stateMeta, err := sharedCreateManyStateMeta(opt.Items, opt.StateMeta)
	if err != nil {
		return nil, err
	}
	appendStateMeta(&args, stateMeta)
	extended := anyCreateItemValues(opt.Items)
	if extended {
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
	return recordsOrNil(value, c.codec)
}

func (c *Client) EnqueueMany(ctx context.Context, opt CreateManyOptions) ([]FlowRecord, error) {
	if opt.State == "" {
		opt.State = "queued"
	}
	return c.CreateMany(ctx, opt)
}

func (c *Client) appendNamedCounts(args *[]any, values map[string]any, refs map[string]string) error {
	*args = append(*args, len(values))
	for name, value := range values {
		encoded, err := c.encode(value)
		if err != nil {
			return err
		}
		*args = append(*args, name, encoded)
	}
	*args = append(*args, len(refs))
	for name, ref := range refs {
		*args = append(*args, name, ref)
	}
	return nil
}

func (c *Client) ValuePut(ctx context.Context, value any, opt ValuePutOptions) (any, error) {
	encoded, err := c.encode(value)
	if err != nil {
		return nil, err
	}
	args := []any{"FLOW.VALUE.PUT", encoded, "NOW", valueOrNow(opt.NowMS)}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendOpt(&args, "OWNER_FLOW_ID", opt.OwnerFlowID)
	appendOpt(&args, "NAME", opt.Name)
	appendBoolPtr(&args, "OVERRIDE", opt.Override)
	appendInt64Ptr(&args, "TTL", opt.TTLMS)
	return c.typedReply(ctx, args...)
}

func (c *Client) PutValue(ctx context.Context, name string, value any, opt ValuePutOptions) (any, error) {
	opt.Name = name
	return c.ValuePut(ctx, value, opt)
}

func (c *Client) ValueMGet(ctx context.Context, refs []string, maxBytes *int64) ([]any, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	args := []any{"FLOW.VALUE.MGET"}
	for _, ref := range refs {
		args = append(args, ref)
	}
	appendInt64Ptr(&args, "MAX_BYTES", maxBytes)
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected value array, got %T", value)
	}
	if len(items) != len(refs) {
		return nil, fmt.Errorf("FLOW.VALUE.MGET returned %d values, expected %d", len(items), len(refs))
	}
	out := make([]any, 0, len(items))
	for _, item := range items {
		decoded, err := decodeValue(c.codec, item)
		if err != nil {
			return nil, err
		}
		out = append(out, decoded)
	}
	return out, nil
}

func (c *Client) Signal(ctx context.Context, opt SignalOptions) (any, error) {
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
	appendInt64Ptr(&args, "PRIORITY", opt.Priority)
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	return c.typedReply(ctx, args...)
}

func (c *Client) FlowSignal(ctx context.Context, opt SignalOptions) (any, error) {
	return c.Signal(ctx, opt)
}

func (c *Client) StartAndClaim(ctx context.Context, opt StartAndClaimOptions) (*FlowRecord, error) {
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
	return recordsFromNative(value, c.codec)
}

func (c *Client) ClaimJobs(ctx context.Context, opt ClaimDueOptions) ([]ClaimedItem, error) {
	opt.JobOnly = true
	value, err := c.claimDue(ctx, opt)
	if err != nil {
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
	appendValueReturn(&args, opt.Values, opt.ValueMaxBytes)
	appendBoolPtr(&args, "RECLAIM_EXPIRED", opt.ReclaimExpired)
	appendInt64Ptr(&args, "RECLAIM_RATIO", opt.ReclaimRatio)
	return c.typedReply(ctx, args...)
}

func (c *Client) Reclaim(ctx context.Context, opt ReclaimOptions) ([]FlowRecord, error) {
	value, err := c.reclaim(ctx, opt)
	if err != nil {
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
	appendValueReturn(&args, opt.Values, opt.ValueMaxBytes)
	return c.typedReply(ctx, args...)
}

func (c *Client) ExtendLease(ctx context.Context, id, leaseToken string, fencingToken, leaseMS int64, partitionKey string) (*FlowRecord, error) {
	args := []any{"FLOW.EXTEND_LEASE", id, leaseToken, "FENCING", fencingToken, "LEASE_MS", leaseMS, "NOW", nowMS()}
	appendOpt(&args, "PARTITION", partitionKey)
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordOrNil(value, c.codec)
}

func (c *Client) Transition(ctx context.Context, opt TransitionOptions) (*FlowRecord, error) {
	now := opt.NowMS
	if now == 0 {
		now = nowMS()
	}
	runAt := opt.RunAtMS
	if runAt == 0 {
		runAt = now
	}
	args := []any{"FLOW.TRANSITION", opt.ID, opt.FromState, opt.ToState, "LEASE_TOKEN", opt.LeaseToken, "FENCING", opt.FencingToken, "NOW", now}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	if err := c.appendEncoded(&args, "PAYLOAD", opt.Payload); err != nil {
		return nil, err
	}
	appendOpt(&args, "RUN_AT", runAt)
	appendInt64Ptr(&args, "PRIORITY", opt.Priority)
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	appendAttributes(&args, nil, opt.AttributesMerge, opt.AttributesDelete)
	appendStateMeta(&args, opt.StateMeta)
	value, err := c.typedReplyOrQueue(ctx, !opt.ReturnRecord, args...)
	if err != nil || !opt.ReturnRecord {
		return nil, err
	}
	record, err := recordOrNil(value, c.codec)
	return c.recordOrGet(ctx, record, err, opt.ID, opt.PartitionKey)
}

func (c *Client) StepContinue(ctx context.Context, opt StepContinueOptions) (*FlowRecord, error) {
	leaseMS := opt.LeaseMS
	if leaseMS == 0 {
		leaseMS = 30000
	}
	now := opt.NowMS
	if now == 0 {
		now = nowMS()
	}
	args := []any{
		"FLOW.STEP_CONTINUE", opt.ID, opt.LeaseToken, opt.FromState, opt.ToState,
		"FENCING", opt.FencingToken,
		"LEASE_MS", leaseMS,
		"NOW", now,
	}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendOpt(&args, "WORKER", opt.Worker)
	if err := c.appendEncoded(&args, "PAYLOAD", opt.Payload); err != nil {
		return nil, err
	}
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	appendAttributes(&args, nil, opt.AttributesMerge, opt.AttributesDelete)
	appendStateMeta(&args, opt.StateMeta)
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordOrNil(value, c.codec)
}

func (c *Client) Complete(ctx context.Context, opt CompleteOptions) (*FlowRecord, error) {
	args := []any{"FLOW.COMPLETE", opt.ID, opt.LeaseToken, "FENCING", opt.FencingToken, "NOW", valueOrNow(opt.NowMS)}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	if err := c.appendEncoded(&args, "RESULT", opt.Result); err != nil {
		return nil, err
	}
	if err := c.appendEncoded(&args, "PAYLOAD", opt.Payload); err != nil {
		return nil, err
	}
	appendInt64Ptr(&args, "TTL", opt.TTLMS)
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	appendAttributes(&args, nil, opt.AttributesMerge, opt.AttributesDelete)
	appendStateMeta(&args, opt.StateMeta)
	value, err := c.typedReplyOrQueue(ctx, !opt.ReturnRecord, args...)
	if err != nil || !opt.ReturnRecord {
		return nil, err
	}
	return recordOrNil(value, c.codec)
}

func (c *Client) Retry(ctx context.Context, opt RetryOptions) (*FlowRecord, error) {
	args := []any{"FLOW.RETRY", opt.ID, opt.LeaseToken, "FENCING", opt.FencingToken, "NOW", valueOrNow(opt.NowMS)}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	if err := c.appendEncoded(&args, "ERROR", opt.Error); err != nil {
		return nil, err
	}
	if err := c.appendEncoded(&args, "PAYLOAD", opt.Payload); err != nil {
		return nil, err
	}
	if opt.RunAtMS != 0 {
		appendOpt(&args, "RUN_AT", opt.RunAtMS)
	}
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	appendAttributes(&args, nil, opt.AttributesMerge, opt.AttributesDelete)
	appendStateMeta(&args, opt.StateMeta)
	value, err := c.typedReplyOrQueue(ctx, !opt.ReturnRecord, args...)
	if err != nil || !opt.ReturnRecord {
		return nil, err
	}
	return recordOrNil(value, c.codec)
}

func (c *Client) Fail(ctx context.Context, opt FailOptions) (*FlowRecord, error) {
	args := []any{"FLOW.FAIL", opt.ID, opt.LeaseToken, "FENCING", opt.FencingToken, "NOW", valueOrNow(opt.NowMS)}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	if err := c.appendEncoded(&args, "ERROR", opt.Error); err != nil {
		return nil, err
	}
	if err := c.appendEncoded(&args, "PAYLOAD", opt.Payload); err != nil {
		return nil, err
	}
	appendInt64Ptr(&args, "TTL", opt.TTLMS)
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	appendAttributes(&args, nil, opt.AttributesMerge, opt.AttributesDelete)
	appendStateMeta(&args, opt.StateMeta)
	value, err := c.typedReplyOrQueue(ctx, !opt.ReturnRecord, args...)
	if err != nil || !opt.ReturnRecord {
		return nil, err
	}
	return recordOrNil(value, c.codec)
}

func (c *Client) Cancel(ctx context.Context, opt CancelOptions) (*FlowRecord, error) {
	args := []any{"FLOW.CANCEL", opt.ID, "FENCING", opt.FencingToken, "NOW", valueOrNow(opt.NowMS)}
	appendOpt(&args, "LEASE_TOKEN", opt.LeaseToken)
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	if err := c.appendEncoded(&args, "REASON", opt.Reason); err != nil {
		return nil, err
	}
	appendInt64Ptr(&args, "TTL", opt.TTLMS)
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	appendAttributes(&args, nil, opt.AttributesMerge, opt.AttributesDelete)
	appendStateMeta(&args, opt.StateMeta)
	value, err := c.typedReplyOrQueue(ctx, !opt.ReturnRecord, args...)
	if err != nil || !opt.ReturnRecord {
		return nil, err
	}
	return recordOrNil(value, c.codec)
}

func (c *Client) Rewind(ctx context.Context, opt RewindOptions) (*FlowRecord, error) {
	args := []any{"FLOW.REWIND", opt.ID, "TO_EVENT", opt.ToEvent, "NOW", valueOrNow(opt.NowMS)}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendOpt(&args, "EXPECT_STATE", opt.ExpectState)
	if opt.RunAtMS != 0 {
		appendOpt(&args, "RUN_AT", opt.RunAtMS)
	}
	appendOpt(&args, "REASON_REF", opt.ReasonRef)
	value, err := c.typedReplyOrQueue(ctx, !opt.ReturnRecord, args...)
	if err != nil || !opt.ReturnRecord {
		return nil, err
	}
	record, err := recordOrNil(value, c.codec)
	return c.recordOrGet(ctx, record, err, opt.ID, opt.PartitionKey)
}
