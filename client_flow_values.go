package ferricstore

import (
	"context"
	"fmt"
)

func (c *Client) ValuePut(ctx context.Context, value any, opt ValuePutOptions) (any, error) {
	if err := validateValuePutOptions(opt); err != nil {
		return nil, err
	}
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
	if err := validateFlowMutationText("flow value name", name); err != nil {
		return nil, err
	}
	opt.Name = name
	return c.ValuePut(ctx, value, opt)
}

func (c *Client) ValueMGet(ctx context.Context, refs []string, maxBytes *int64) ([]any, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	if err := validateValueMGet(refs, maxBytes); err != nil {
		return nil, err
	}
	args := make([]any, 1, len(refs)+3)
	args[0] = "FLOW.VALUE.MGET"
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
