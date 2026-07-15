package ferricstore

import (
	"context"
	"errors"
)

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
	return c.typedReply(ctx, args...)
}
