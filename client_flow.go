package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
	value, err := c.Command(ctx, args...)
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
	mixed := opt.PartitionKey == "" && anyItemPartition(opt.Items)
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
				if item.PartitionKey == "" {
					return nil, errors.New("mixed create_many items require partition key")
				}
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
				if item.PartitionKey == "" {
					return nil, errors.New("mixed create_many items require partition key")
				}
				args = append(args, item.ID, item.PartitionKey, encoded)
			} else {
				args = append(args, item.ID, encoded)
			}
		}
	}
	value, err := c.Command(ctx, args...)
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
	return c.Command(ctx, args...)
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
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected value array, got %T", value)
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
	return c.Command(ctx, args...)
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
	value, err := c.Command(ctx, args...)
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
	return claimedItemsFromNative(value)
}

func (c *Client) claimDue(ctx context.Context, opt ClaimDueOptions) (any, error) {
	if opt.State != "" && len(opt.States) > 0 {
		return nil, errors.New("state and states are mutually exclusive")
	}
	if opt.PartitionKey != "" && len(opt.PartitionKeys) > 0 {
		return nil, errors.New("partition key and partition keys are mutually exclusive")
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
	if opt.IncludeState && !opt.JobOnly {
		return nil, errors.New("include state requires job only")
	}
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
	return c.Command(ctx, args...)
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
	return claimedItemsFromNative(value)
}

func (c *Client) reclaim(ctx context.Context, opt ReclaimOptions) (any, error) {
	if opt.State != "" && opt.State != "running" {
		return nil, errors.New("FLOW.RECLAIM only supports running state")
	}
	if opt.PartitionKey != "" && len(opt.PartitionKeys) > 0 {
		return nil, errors.New("partition key and partition keys are mutually exclusive")
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
	return c.Command(ctx, args...)
}

func (c *Client) ExtendLease(ctx context.Context, id, leaseToken string, fencingToken, leaseMS int64, partitionKey string) (*FlowRecord, error) {
	args := []any{"FLOW.EXTEND_LEASE", id, leaseToken, "FENCING", fencingToken, "LEASE_MS", leaseMS, "NOW", nowMS()}
	appendOpt(&args, "PARTITION", partitionKey)
	value, err := c.Command(ctx, args...)
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
	value, err := c.Command(ctx, args...)
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
	value, err := c.Command(ctx, args...)
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
	value, err := c.Command(ctx, args...)
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
	value, err := c.Command(ctx, args...)
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
	value, err := c.Command(ctx, args...)
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
	value, err := c.Command(ctx, args...)
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
	value, err := c.Command(ctx, args...)
	if err != nil || !opt.ReturnRecord {
		return nil, err
	}
	record, err := recordOrNil(value, c.codec)
	return c.recordOrGet(ctx, record, err, opt.ID, opt.PartitionKey)
}

func (c *Client) CompleteMany(ctx context.Context, opt CompleteManyOptions) ([]FlowRecord, error) {
	if len(opt.Items) == 0 {
		return nil, nil
	}
	now := valueOrNow(opt.NowMS)
	if records, ok, err := c.tryCompleteManyNativeCompact(ctx, opt, now); ok || err != nil {
		return records, err
	}
	args := []any{"FLOW.COMPLETE_MANY", mixedPartition(opt.PartitionKey)}
	if err := c.appendEncoded(&args, "RESULT", opt.Result); err != nil {
		return nil, err
	}
	if err := c.appendEncoded(&args, "PAYLOAD", opt.Payload); err != nil {
		return nil, err
	}
	appendInt64Ptr(&args, "TTL", opt.TTLMS)
	appendOpt(&args, "NOW", now)
	appendBoolPtr(&args, "INDEPENDENT", opt.Independent)
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	appendAttributes(&args, nil, opt.AttributesMerge, opt.AttributesDelete)
	appendStateMeta(&args, opt.StateMeta)
	if err := appendClaimedItems(&args, opt.PartitionKey, opt.Items, "FLOW.COMPLETE_MANY"); err != nil {
		return nil, err
	}
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsOrNil(value, c.codec)
}

func (c *Client) TransitionMany(ctx context.Context, opt TransitionManyOptions) ([]FlowRecord, error) {
	if len(opt.Items) == 0 {
		return nil, nil
	}
	args := []any{"FLOW.TRANSITION_MANY", mixedPartition(opt.PartitionKey), opt.FromState, opt.ToState}
	if err := c.appendEncoded(&args, "PAYLOAD", opt.Payload); err != nil {
		return nil, err
	}
	if opt.RunAtMS != 0 {
		appendOpt(&args, "RUN_AT", opt.RunAtMS)
	}
	appendInt64Ptr(&args, "PRIORITY", opt.Priority)
	appendOpt(&args, "NOW", valueOrNow(opt.NowMS))
	appendBoolPtr(&args, "INDEPENDENT", opt.Independent)
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	appendAttributes(&args, nil, opt.AttributesMerge, opt.AttributesDelete)
	appendStateMeta(&args, opt.StateMeta)
	if err := appendFencedItems(&args, opt.PartitionKey, opt.Items, "FLOW.TRANSITION_MANY", true); err != nil {
		return nil, err
	}
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsOrNil(value, c.codec)
}

func (c *Client) RetryMany(ctx context.Context, opt RetryManyOptions) ([]FlowRecord, error) {
	if len(opt.Items) == 0 {
		return nil, nil
	}
	args := []any{"FLOW.RETRY_MANY", mixedPartition(opt.PartitionKey)}
	if err := c.appendEncoded(&args, "ERROR", opt.Error); err != nil {
		return nil, err
	}
	if err := c.appendEncoded(&args, "PAYLOAD", opt.Payload); err != nil {
		return nil, err
	}
	if opt.RunAtMS != 0 {
		appendOpt(&args, "RUN_AT", opt.RunAtMS)
	}
	appendOpt(&args, "NOW", valueOrNow(opt.NowMS))
	appendBoolPtr(&args, "INDEPENDENT", opt.Independent)
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	appendAttributes(&args, nil, opt.AttributesMerge, opt.AttributesDelete)
	appendStateMeta(&args, opt.StateMeta)
	if err := appendClaimedItems(&args, opt.PartitionKey, opt.Items, "FLOW.RETRY_MANY"); err != nil {
		return nil, err
	}
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsOrNil(value, c.codec)
}

func (c *Client) FailMany(ctx context.Context, opt FailManyOptions) ([]FlowRecord, error) {
	if len(opt.Items) == 0 {
		return nil, nil
	}
	args := []any{"FLOW.FAIL_MANY", mixedPartition(opt.PartitionKey)}
	if err := c.appendEncoded(&args, "ERROR", opt.Error); err != nil {
		return nil, err
	}
	if err := c.appendEncoded(&args, "PAYLOAD", opt.Payload); err != nil {
		return nil, err
	}
	appendInt64Ptr(&args, "TTL", opt.TTLMS)
	appendOpt(&args, "NOW", valueOrNow(opt.NowMS))
	appendBoolPtr(&args, "INDEPENDENT", opt.Independent)
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	appendAttributes(&args, nil, opt.AttributesMerge, opt.AttributesDelete)
	appendStateMeta(&args, opt.StateMeta)
	if err := appendClaimedItems(&args, opt.PartitionKey, opt.Items, "FLOW.FAIL_MANY"); err != nil {
		return nil, err
	}
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsOrNil(value, c.codec)
}

func (c *Client) CancelMany(ctx context.Context, opt CancelManyOptions) ([]FlowRecord, error) {
	if len(opt.Items) == 0 {
		return nil, nil
	}
	args := []any{"FLOW.CANCEL_MANY", mixedPartition(opt.PartitionKey)}
	if err := c.appendEncoded(&args, "REASON", opt.Reason); err != nil {
		return nil, err
	}
	appendInt64Ptr(&args, "TTL", opt.TTLMS)
	appendOpt(&args, "NOW", valueOrNow(opt.NowMS))
	appendBoolPtr(&args, "INDEPENDENT", opt.Independent)
	if err := c.appendNamedValues(&args, opt.NamedValues); err != nil {
		return nil, err
	}
	appendAttributes(&args, nil, opt.AttributesMerge, opt.AttributesDelete)
	appendStateMeta(&args, opt.StateMeta)
	if err := appendFencedItems(&args, opt.PartitionKey, opt.Items, "FLOW.CANCEL_MANY", false); err != nil {
		return nil, err
	}
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsOrNil(value, c.codec)
}

func (c *Client) RunStepsMany(ctx context.Context, opt RunStepsManyOptions) error {
	if len(opt.Items) == 0 {
		return nil
	}
	if (len(opt.States) == 0) == (opt.Steps == 0) {
		return errors.New("run_steps_many requires exactly one of states or steps")
	}
	if opt.Steps < 0 {
		return errors.New("run_steps_many steps must be positive")
	}
	leaseMS := opt.LeaseMS
	if leaseMS == 0 {
		leaseMS = 30000
	}
	args := []any{"FLOW.RUN_STEPS_MANY", "TYPE", opt.Type}
	if len(opt.States) > 0 {
		args = append(args, "STATES", opt.States)
	} else {
		args = append(args, "STEPS", opt.Steps)
	}
	args = append(args, "WORKER", opt.Worker, "LEASE_MS", leaseMS, "NOW", valueOrNow(opt.NowMS))
	if err := c.appendEncoded(&args, "PAYLOAD", opt.Payload); err != nil {
		return err
	}
	if err := c.appendEncoded(&args, "RESULT", opt.Result); err != nil {
		return err
	}
	appendInt64Ptr(&args, "RETENTION_TTL_MS", opt.RetentionTTLMS)
	args = append(args, "ITEMS", runStepsItems(opt.Items, opt.PartitionKey))
	_, err := c.Command(ctx, args...)
	return err
}

func (c *Client) Get(ctx context.Context, id string, partitionKey string, values []string, valueMaxBytes *int64) (*FlowRecord, error) {
	args := []any{"FLOW.GET", id}
	appendOpt(&args, "PARTITION", partitionKey)
	appendValueReturn(&args, values, valueMaxBytes)
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordOrNil(value, c.codec)
}

func (c *Client) recordOrGet(ctx context.Context, record *FlowRecord, err error, id, partitionKey string) (*FlowRecord, error) {
	if err != nil || record != nil {
		return record, err
	}
	record, err = c.Get(ctx, id, partitionKey, nil, nil)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, fmt.Errorf("FLOW command succeeded but record %q was not found", id)
	}
	return record, nil
}

func (c *Client) List(ctx context.Context, flowType string, opt ReadOptions) ([]FlowRecord, error) {
	args := []any{"FLOW.LIST", flowType}
	appendReadOptions(&args, opt)
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsFromNative(value, c.codec)
}

func (c *Client) Search(ctx context.Context, opt SearchOptions) ([]FlowRecord, error) {
	args := []any{"FLOW.SEARCH"}
	appendOpt(&args, "TYPE", opt.Type)
	appendOpt(&args, "STATE", opt.State)
	appendIntPtr(&args, "COUNT", opt.Count)
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendInt64Ptr(&args, "FROM_MS", opt.FromMS)
	appendInt64Ptr(&args, "TO_MS", opt.ToMS)
	appendBoolPtr(&args, "REV", opt.Rev)
	appendBoolPtr(&args, "TERMINAL_ONLY", opt.TerminalOnly)
	appendBoolPtr(&args, "INCLUDE_COLD", opt.IncludeCold)
	appendBoolPtr(&args, "CONSISTENT_PROJECTION", opt.ConsistentProjection)
	appendAttributes(&args, opt.Attributes, nil, nil)
	appendSearchStateMeta(&args, opt.StateMeta)
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsFromNative(value, c.codec)
}

func (c *Client) Exists(ctx context.Context, flowType string, opt ReadOptions) (bool, error) {
	opt.Count = nil

	stats, err := c.Stats(ctx, flowType, opt)
	if err != nil {
		return false, err
	}
	count, err := statsCount(stats)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (c *Client) Terminals(ctx context.Context, flowType string, opt ReadOptions) ([]FlowRecord, error) {
	return c.indexRead(ctx, "FLOW.TERMINALS", flowType, opt)
}

func (c *Client) Failures(ctx context.Context, flowType string, opt ReadOptions) ([]FlowRecord, error) {
	return c.indexRead(ctx, "FLOW.FAILURES", flowType, opt)
}

func (c *Client) ByParent(ctx context.Context, parentFlowID string, opt ReadOptions) ([]FlowRecord, error) {
	return c.indexRead(ctx, "FLOW.BY_PARENT", parentFlowID, opt)
}

func (c *Client) ByRoot(ctx context.Context, rootFlowID string, opt ReadOptions) ([]FlowRecord, error) {
	return c.indexRead(ctx, "FLOW.BY_ROOT", rootFlowID, opt)
}

func (c *Client) ByCorrelation(ctx context.Context, correlationID string, opt ReadOptions) ([]FlowRecord, error) {
	return c.indexRead(ctx, "FLOW.BY_CORRELATION", correlationID, opt)
}

func (c *Client) indexRead(ctx context.Context, command, key string, opt ReadOptions) ([]FlowRecord, error) {
	args := []any{command, key}
	appendReadOptions(&args, opt)
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsFromNative(value, c.codec)
}

func appendReadOptions(args *[]any, opt ReadOptions) {
	appendIntPtr(args, "COUNT", opt.Count)
	appendOpt(args, "PARTITION", opt.PartitionKey)
	appendInt64Ptr(args, "FROM_MS", opt.FromMS)
	appendInt64Ptr(args, "TO_MS", opt.ToMS)
	appendBoolPtr(args, "REV", opt.Rev)
	appendOpt(args, "STATE", opt.State)
	appendBoolPtr(args, "TERMINAL_ONLY", opt.TerminalOnly)
	appendBoolPtr(args, "INCLUDE_COLD", opt.IncludeCold)
	appendBoolPtr(args, "CONSISTENT_PROJECTION", opt.ConsistentProjection)
	appendAttributes(args, opt.Attributes, nil, nil)
}

func (c *Client) Info(ctx context.Context, flowType, partitionKey string, includeCold, consistentProjection *bool) (map[string]any, error) {
	args := []any{"FLOW.INFO", flowType}
	appendOpt(&args, "PARTITION", partitionKey)
	appendBoolPtr(&args, "INCLUDE_COLD", includeCold)
	appendBoolPtr(&args, "CONSISTENT_PROJECTION", consistentProjection)
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return nativeMap(value)
}

func (c *Client) Stuck(ctx context.Context, flowType string, partitionKey string, count *int, olderThanMS, now *int64) ([]FlowRecord, error) {
	args := []any{"FLOW.STUCK", flowType}
	appendOpt(&args, "PARTITION", partitionKey)
	appendIntPtr(&args, "COUNT", count)
	appendInt64Ptr(&args, "OLDER_THAN", olderThanMS)
	appendInt64Ptr(&args, "NOW", now)
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsFromNative(value, c.codec)
}

func (c *Client) History(ctx context.Context, opt HistoryOptions) ([]any, error) {
	count := opt.Count
	if count == 0 {
		count = 100
	}
	args := []any{"FLOW.HISTORY", opt.ID, "COUNT", count}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendOpt(&args, "FROM_EVENT", opt.FromEvent)
	appendOpt(&args, "TO_EVENT", opt.ToEvent)
	appendInt64Ptr(&args, "FROM_MS", opt.FromMS)
	appendInt64Ptr(&args, "TO_MS", opt.ToMS)
	appendInt64Ptr(&args, "FROM_VERSION", opt.FromVersion)
	appendInt64Ptr(&args, "TO_VERSION", opt.ToVersion)
	appendBoolPtr(&args, "REV", opt.Rev)
	appendOpt(&args, "EVENT", opt.Event)
	appendOpt(&args, "WORKER", opt.Worker)
	appendBoolPtr(&args, "INCLUDE_COLD", opt.IncludeCold)
	appendBoolPtr(&args, "CONSISTENT_PROJECTION", opt.ConsistentProjection)
	appendBoolPtr(&args, "VALUES", opt.Values)
	appendInt64Ptr(&args, "PAYLOAD_MAX_BYTES", opt.PayloadMaxBytes)
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected history array, got %T", value)
	}
	return items, nil
}

func (c *Client) SpawnChildren(ctx context.Context, opt SpawnChildrenOptions) (any, error) {
	group := opt.GroupID
	if group == "" {
		group = "default"
	}
	wait := opt.Wait
	if wait == "" {
		wait = "all"
	}
	args := []any{"FLOW.SPAWN_CHILDREN", opt.ParentID, "GROUP", group, "WAIT", wait, "NOW", valueOrNow(opt.NowMS)}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendOpt(&args, "LEASE_TOKEN", opt.LeaseToken)
	appendInt64Ptr(&args, "FENCING", opt.FencingToken)
	appendOpt(&args, "WAIT_STATE", opt.WaitState)
	appendOpt(&args, "SUCCESS", opt.Success)
	appendOpt(&args, "FAILURE", opt.Failure)
	appendOpt(&args, "FROM_STATE", opt.FromState)
	appendOpt(&args, "ON_CHILD_FAILED", opt.OnChildFailed)
	appendOpt(&args, "ON_PARENT_CLOSED", opt.OnParentClosed)
	mixed := anyChildPartition(opt.Children)
	extended := anyChildValues(opt.Children)
	if extended {
		args = append(args, "ITEMS_EXT", len(opt.Children))
		for _, child := range opt.Children {
			if mixed && child.PartitionKey == "" {
				return nil, errors.New("mixed spawn children require partition key")
			}
			partition := child.PartitionKey
			if partition == "" {
				partition = "-"
			}
			encoded, err := c.encode(child.Payload)
			if err != nil {
				return nil, err
			}
			args = append(args, child.ID, partition, child.Type, encoded)
			if err := c.appendNamedCounts(&args, mergeValues(opt.Values, child.Values), mergeRefs(opt.ValueRefs, child.ValueRefs)); err != nil {
				return nil, err
			}
		}
	} else {
		if err := c.appendNamedValues(&args, NamedValues{Values: opt.Values, ValueRefs: opt.ValueRefs}); err != nil {
			return nil, err
		}
		args = append(args, "ITEMS")
		if mixed {
			args = append(args, "MIXED")
		}
		for _, child := range opt.Children {
			encoded, err := c.encode(child.Payload)
			if err != nil {
				return nil, err
			}
			if mixed {
				if child.PartitionKey == "" {
					return nil, errors.New("mixed spawn children require partition key")
				}
				args = append(args, child.ID, child.PartitionKey, child.Type, encoded)
			} else {
				args = append(args, child.ID, child.Type, encoded)
			}
		}
	}
	return c.Command(ctx, args...)
}

func (c *Client) InstallPolicy(ctx context.Context, flowType string, opt PolicyOptions) (any, error) {
	return c.SetPolicy(ctx, flowType, opt)
}

func (c *Client) InstallRetryPolicy(ctx context.Context, flowType string, retry *RetryPolicy, states map[string]RetryPolicy) (any, error) {
	return c.SetPolicy(ctx, flowType, PolicyOptions{Retry: retry, States: states})
}

func (c *Client) SetPolicy(ctx context.Context, flowType string, opt PolicyOptions) (any, error) {
	args := []any{"FLOW.POLICY.SET", flowType}
	if len(opt.IndexedAttributes) > 0 {
		appendOpt(&args, "INDEXED_ATTRIBUTES", opt.IndexedAttributes)
	}
	appendOpt(&args, "INDEXED_STATE_META", opt.IndexedStateMeta)
	if opt.Retry != nil {
		appendRetryPolicy(&args, *opt.Retry)
	}
	for state, policy := range opt.States {
		if _, exists := opt.StatePolicies[state]; exists {
			return nil, fmt.Errorf("flow state %q appears in both States and StatePolicies", state)
		}
		args = append(args, "STATE", state)
		appendRetryPolicy(&args, policy)
	}
	for state, policy := range opt.StatePolicies {
		args = append(args, "STATE", state)
		if policy.Mode != "" {
			mode, err := flowStateModeCommandToken(policy.Mode)
			if err != nil {
				return nil, err
			}
			appendOpt(&args, "MODE", mode)
		}
		if policy.Retry != nil {
			appendRetryPolicy(&args, *policy.Retry)
		}
	}
	return c.Command(ctx, args...)
}

func (c *Client) PolicyGet(ctx context.Context, flowType, state string) (map[string]any, error) {
	args := []any{"FLOW.POLICY.GET", flowType}
	appendOpt(&args, "STATE", state)
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return nativeMap(value)
}

func (c *Client) RetentionCleanup(ctx context.Context, opt RetentionCleanupOptions) (map[string]any, error) {
	args := []any{"FLOW.RETENTION_CLEANUP"}
	appendIntPtr(&args, "LIMIT", opt.Limit)
	appendInt64Ptr(&args, "NOW", opt.NowMS)
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return nativeMap(value)
}

func appendRetryPolicy(args *[]any, policy RetryPolicy) {
	if policy.MaxRetries != 0 {
		appendOpt(args, "MAX_RETRIES", policy.MaxRetries)
	}
	appendOpt(args, "BACKOFF", policy.Backoff)
	if policy.BaseMS != 0 {
		appendOpt(args, "BASE_MS", policy.BaseMS)
	}
	if policy.MaxMS != 0 {
		appendOpt(args, "MAX_MS", policy.MaxMS)
	}
	if policy.JitterPct != 0 {
		appendOpt(args, "JITTER_PCT", policy.JitterPct)
	}
	appendOpt(args, "EXHAUSTED_TO", policy.ExhaustedTo)
}

func flowStateModeCommandToken(mode FlowStateMode) (string, error) {
	switch strings.ToUpper(string(mode)) {
	case string(FlowStateModeParallel):
		return string(FlowStateModeParallel), nil
	case string(FlowStateModeFIFO):
		return string(FlowStateModeFIFO), nil
	default:
		return "", errors.New("ERR flow state mode must be parallel or fifo")
	}
}
