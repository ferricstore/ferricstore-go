package ferricstore

import (
	"context"
	"fmt"
)

func (c *Client) Get(ctx context.Context, id string, partitionKey string, values []string, valueMaxBytes *int64) (*FlowRecord, error) {
	if err := validateFlowGet(id, values, valueMaxBytes); err != nil {
		return nil, err
	}
	args := []any{"FLOW.GET", id}
	appendOpt(&args, "PARTITION", partitionKey)
	appendValueReturn(&args, values, valueMaxBytes)
	value, err := c.typedReply(ctx, args...)
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
	if err := validateFlowReadKey("flow type", flowType, opt); err != nil {
		return nil, err
	}
	args := []any{"FLOW.LIST", flowType}
	appendReadOptions(&args, opt)
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsFromNative(value, c.codec)
}

func (c *Client) Search(ctx context.Context, opt SearchOptions) ([]FlowRecord, error) {
	if err := validateFlowSearch(opt); err != nil {
		return nil, err
	}
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
	value, err := c.typedReply(ctx, args...)
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
	if err := validateFlowReadKey("flow query key", key, opt); err != nil {
		return nil, err
	}
	args := []any{command, key}
	appendReadOptions(&args, opt)
	value, err := c.typedReply(ctx, args...)
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
	if err := validateRequiredText("flow type", flowType); err != nil {
		return nil, err
	}
	args := []any{"FLOW.INFO", flowType}
	appendOpt(&args, "PARTITION", partitionKey)
	appendBoolPtr(&args, "INCLUDE_COLD", includeCold)
	appendBoolPtr(&args, "CONSISTENT_PROJECTION", consistentProjection)
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	return nativeMap(value)
}

func (c *Client) Stuck(ctx context.Context, flowType string, partitionKey string, count *int, olderThanMS, now *int64) ([]FlowRecord, error) {
	if err := validateFlowStuck(flowType, count, olderThanMS, now); err != nil {
		return nil, err
	}
	args := []any{"FLOW.STUCK", flowType}
	appendOpt(&args, "PARTITION", partitionKey)
	appendIntPtr(&args, "COUNT", count)
	appendInt64Ptr(&args, "OLDER_THAN", olderThanMS)
	appendInt64Ptr(&args, "NOW", now)
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	return recordsFromNative(value, c.codec)
}

func (c *Client) History(ctx context.Context, opt HistoryOptions) ([]any, error) {
	if err := validateFlowHistory(opt); err != nil {
		return nil, err
	}
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
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected history array, got %T", value)
	}
	return items, nil
}
