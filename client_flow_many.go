package ferricstore

import (
	"context"
)

func (c *Client) CompleteMany(ctx context.Context, opt CompleteManyOptions) ([]FlowRecord, error) {
	if len(opt.Items) == 0 {
		return nil, nil
	}
	if err := validateClaimedItemPartitions(opt.PartitionKey, opt.Items, "FLOW.COMPLETE_MANY"); err != nil {
		return nil, err
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
	appendClaimedItems(&args, opt.PartitionKey, opt.Items)
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsOrNil(value, c.codec)
}

func (c *Client) TransitionMany(ctx context.Context, opt TransitionManyOptions) ([]FlowRecord, error) {
	if len(opt.Items) == 0 {
		return nil, nil
	}
	if err := validateFencedItemPartitions(opt.PartitionKey, opt.Items, "FLOW.TRANSITION_MANY"); err != nil {
		return nil, err
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
	appendFencedItems(&args, opt.PartitionKey, opt.Items, true)
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsOrNil(value, c.codec)
}

func (c *Client) RetryMany(ctx context.Context, opt RetryManyOptions) ([]FlowRecord, error) {
	if len(opt.Items) == 0 {
		return nil, nil
	}
	if err := validateClaimedItemPartitions(opt.PartitionKey, opt.Items, "FLOW.RETRY_MANY"); err != nil {
		return nil, err
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
	appendClaimedItems(&args, opt.PartitionKey, opt.Items)
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsOrNil(value, c.codec)
}

func (c *Client) FailMany(ctx context.Context, opt FailManyOptions) ([]FlowRecord, error) {
	if len(opt.Items) == 0 {
		return nil, nil
	}
	if err := validateClaimedItemPartitions(opt.PartitionKey, opt.Items, "FLOW.FAIL_MANY"); err != nil {
		return nil, err
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
	appendClaimedItems(&args, opt.PartitionKey, opt.Items)
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsOrNil(value, c.codec)
}

func (c *Client) CancelMany(ctx context.Context, opt CancelManyOptions) ([]FlowRecord, error) {
	if len(opt.Items) == 0 {
		return nil, nil
	}
	if err := validateFencedItemPartitions(opt.PartitionKey, opt.Items, "FLOW.CANCEL_MANY"); err != nil {
		return nil, err
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
	appendFencedItems(&args, opt.PartitionKey, opt.Items, false)
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsOrNil(value, c.codec)
}

func (c *Client) RunStepsMany(ctx context.Context, opt RunStepsManyOptions) error {
	if len(opt.Items) == 0 {
		return nil
	}
	if err := validateRunStepsManyOptions(opt); err != nil {
		return err
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
	return c.typedStatus(ctx, args...)
}
