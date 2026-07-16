package ferricstore

import "context"

func (c *Client) Stats(ctx context.Context, flowType string, opt ReadOptions) (map[string]any, error) {
	if err := validateRequiredText("flow type", flowType); err != nil {
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
	if err := validateRequiredText("flow type", flowType); err != nil {
		return nil, err
	}
	if err := validateFlowReadOptions(opt); err != nil {
		return nil, err
	}
	args := []any{"FLOW.ATTRIBUTES", flowType}
	appendReadOptions(&args, opt)
	return mapList(c.typedReply(ctx, args...))
}

func (c *Client) AttributeValues(ctx context.Context, flowType, attribute string, opt ReadOptions) ([]map[string]any, error) {
	if err := validateRequiredText("flow type", flowType); err != nil {
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
	return mapList(c.typedReply(ctx, args...))
}

func (c *Client) GovernanceLedger(ctx context.Context, id string, opt ReadOptions) ([]map[string]any, error) {
	if err := validateRequiredText("id", id); err != nil {
		return nil, err
	}
	if err := validateFlowReadOptions(opt); err != nil {
		return nil, err
	}
	args := []any{"FLOW.GOVERNANCE.LEDGER", id}
	appendReadOptions(&args, opt)
	return mapList(c.typedReply(ctx, args...))
}
