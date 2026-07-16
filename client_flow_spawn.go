package ferricstore

import (
	"context"
)

func (c *Client) SpawnChildren(ctx context.Context, opt SpawnChildrenOptions) (any, error) {
	opt = normalizeSpawnChildrenOptions(opt)
	if err := validateSpawnChildrenOptions(opt); err != nil {
		return nil, err
	}
	args := []any{"FLOW.SPAWN_CHILDREN", opt.ParentID, "GROUP", opt.GroupID, "WAIT", opt.Wait, "NOW", valueOrNow(opt.NowMS)}
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
			partition := child.PartitionKey
			if partition == "" {
				if mixed {
					partition = opt.PartitionKey
				} else {
					partition = "-"
				}
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
				partition := child.PartitionKey
				if partition == "" {
					partition = opt.PartitionKey
				}
				args = append(args, child.ID, partition, child.Type, encoded)
			} else {
				args = append(args, child.ID, child.Type, encoded)
			}
		}
	}
	return c.typedReply(ctx, args...)
}
