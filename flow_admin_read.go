package ferricstore

import "context"

// GovernanceLedgerOptions is the exact FerricStore v0.8.0 ledger query
// contract. Limit maps to the server's LIMIT option; generic Flow read filters
// do not apply to governance ledger events.
type GovernanceLedgerOptions struct {
	PartitionKey string
	Limit        *int
	FromMS       *int64
	ToMS         *int64
	Rev          *bool
}

func (c *Client) Stats(ctx context.Context, flowType string, opt ReadOptions) (map[string]any, error) {
	if err := validatePublicFlowType("flow type", flowType); err != nil {
		return nil, err
	}
	if err := validateFlowReadOptions(opt); err != nil {
		return nil, err
	}
	args := []any{"FLOW.STATS", flowType}
	appendReadOptions(&args, opt)
	return mapResult(c.typedReply(ctx, args...))
}

func (c *Client) CountByState(ctx context.Context, flowType, state string, opt ReadOptions) (int64, error) {
	if err := validateRequiredText("state", state); err != nil {
		return 0, err
	}
	opt.State = state
	opt.Count = nil

	stats, err := c.Stats(ctx, flowType, opt)
	if err != nil {
		return 0, err
	}
	return statsCount(stats)
}

func (c *Client) Attributes(ctx context.Context, flowType string, opt ReadOptions) ([]map[string]any, error) {
	if err := validatePublicFlowType("flow type", flowType); err != nil {
		return nil, err
	}
	if err := validateFlowReadOptions(opt); err != nil {
		return nil, err
	}
	args := []any{"FLOW.ATTRIBUTES", flowType}
	appendReadOptions(&args, opt)
	value, err := c.typedReply(ctx, args...)
	return mapListWithLimit("FLOW.ATTRIBUTES", opt.Count, defaultFlowResponseLimitV080, 0, value, err)
}

func (c *Client) AttributeValues(ctx context.Context, flowType, attribute string, opt ReadOptions) ([]map[string]any, error) {
	if err := validatePublicFlowType("flow type", flowType); err != nil {
		return nil, err
	}
	if err := validateRequiredText("attribute", attribute); err != nil {
		return nil, err
	}
	if err := validateFlowReadOptions(opt); err != nil {
		return nil, err
	}
	args := []any{"FLOW.ATTRIBUTE_VALUES", flowType, attribute}
	appendReadOptions(&args, opt)
	value, err := c.typedReply(ctx, args...)
	return mapListWithLimit("FLOW.ATTRIBUTE_VALUES", opt.Count, defaultFlowResponseLimitV080, 0, value, err)
}

func (c *Client) GovernanceLedger(ctx context.Context, id string, opt GovernanceLedgerOptions) ([]map[string]any, error) {
	if err := validateGovernanceLedger(id, opt); err != nil {
		return nil, err
	}
	args := []any{"FLOW.GOVERNANCE.LEDGER", id}
	appendOpt(&args, "PARTITION", opt.PartitionKey)
	appendIntPtr(&args, "LIMIT", opt.Limit)
	appendInt64Ptr(&args, "FROM_MS", opt.FromMS)
	appendInt64Ptr(&args, "TO_MS", opt.ToMS)
	appendBoolPtr(&args, "REV", opt.Rev)
	value, err := c.typedReply(ctx, args...)
	return mapListWithLimit(
		"FLOW.GOVERNANCE.LEDGER", opt.Limit,
		defaultFlowResponseLimitV080, maxClampedFlowListItemsV080,
		value, err,
	)
}
