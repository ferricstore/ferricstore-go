package ferricstore

import "context"

func (c *Client) ExtendLease(ctx context.Context, id, leaseToken string, fencingToken, leaseMS int64, partitionKey string) (*FlowRecord, error) {
	if err := validateExtendLease(id, leaseToken, fencingToken, leaseMS); err != nil {
		return nil, err
	}
	args := []any{"FLOW.EXTEND_LEASE", id, leaseToken, "FENCING", fencingToken, "LEASE_MS", leaseMS, "NOW", nowMS()}
	appendOpt(&args, "PARTITION", partitionKey)
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordOrNil(value, c.codec)
}

func (c *Client) Transition(ctx context.Context, opt TransitionOptions) (*FlowRecord, error) {
	if err := validateTransitionOptions(opt); err != nil {
		return nil, err
	}
	now := opt.NowMS
	if now == 0 {
		now = nowMS()
	}
	runAt := opt.RunAtMS
	if runAt == 0 {
		runAt = now
	}
	args := []any{"FLOW.TRANSITION", opt.ID, opt.FromState, opt.ToState}
	appendOpt(&args, "LEASE_TOKEN", opt.LeaseToken)
	args = append(args, "FENCING", opt.FencingToken, "NOW", now)
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
	if err := validateStepContinueOptions(opt); err != nil {
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
	if err := validateCompleteOptions(opt); err != nil {
		return nil, err
	}
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
	if err := validateRetryOptions(opt); err != nil {
		return nil, err
	}
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
	appendAttributes(&args, nil, opt.AttributesMerge, opt.AttributesDelete)
	appendStateMeta(&args, opt.StateMeta)
	value, err := c.typedReplyOrQueue(ctx, !opt.ReturnRecord, args...)
	if err != nil || !opt.ReturnRecord {
		return nil, err
	}
	return recordOrNil(value, c.codec)
}

func (c *Client) Fail(ctx context.Context, opt FailOptions) (*FlowRecord, error) {
	if err := validateFailOptions(opt); err != nil {
		return nil, err
	}
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
	if err := validateCancelOptions(opt); err != nil {
		return nil, err
	}
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
	if err := validateRewindOptions(opt); err != nil {
		return nil, err
	}
	args := []any{"FLOW.REWIND", opt.ID, "TO_EVENT", opt.ToEvent, "NOW", valueOrNow(opt.NowMS)}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendOpt(&args, "EXPECT_STATE", opt.ExpectState)
	if opt.RunAtMS != 0 {
		appendOpt(&args, "RUN_AT", opt.RunAtMS)
	}
	value, err := c.typedReplyOrQueue(ctx, !opt.ReturnRecord, args...)
	if err != nil || !opt.ReturnRecord {
		return nil, err
	}
	record, err := recordOrNil(value, c.codec)
	return c.recordOrGet(ctx, record, err, opt.ID, opt.PartitionKey)
}
