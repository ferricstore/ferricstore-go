package ferricstore

import (
	"context"
	"errors"
	"fmt"
)

type topologyScatterCommandResult struct {
	count    int64
	failures []error
}

func (e *TopologyNativeExecutor) scatterCommandWithContext(
	ctx context.Context,
	name string,
	keys []any,
	requestContext any,
	hasRequestContext bool,
) (any, error) {
	return e.scatterCommandAttempt(ctx, name, keys, requestContext, hasRequestContext, true)
}

func (e *TopologyNativeExecutor) scatterCommandAttempt(
	ctx context.Context,
	name string,
	keys []any,
	requestContext any,
	hasRequestContext bool,
	allowSafeReroute bool,
) (any, error) {
	snapshot, groups, err := e.planScatterRoutes(ctx, keys)
	if err != nil {
		return nil, fmt.Errorf("plan %s routes: %w", name, err)
	}
	destructive := destructiveScatterCommand(name)
	if len(groups) > 1 && destructive && e.crossShardWrites != CrossShardWritePerShard {
		return nil, fmt.Errorf(
			"cross-shard %s is disabled; opt in with WithTopologyCrossShardWritePolicy(CrossShardWritePerShard)",
			name,
		)
	}

	values := topologyScatterValueSlots(name, len(keys))
	results := make(chan topologyScatterCommandResult, len(groups))
	tasks := make([]func(), 0, len(groups))
	for _, group := range groups {
		group := group
		tasks = append(tasks, func() {
			result := e.executeScatterCommandGroup(
				ctx, name, group, snapshot, requestContext, hasRequestContext, allowSafeReroute,
			)
			if len(result.values) > 0 {
				for index, position := range group.positions {
					values[position] = result.values[index]
				}
			}
			results <- topologyScatterCommandResult{count: result.count, failures: result.failures}
		})
	}
	runBoundedTopologyTasks(maxTopologyConcurrentTasks, tasks)
	close(results)

	var total int64
	var failures []error
	for result := range results {
		total += result.count
		failures = append(failures, result.failures...)
	}
	if len(failures) > 0 {
		if destructive {
			return nil, newTopologyPartialWriteError(name, total, failures)
		}
		return nil, failures[0]
	}
	if name == "MGET" {
		return values, nil
	}
	return total, nil
}

type topologyScatterGroupResult struct {
	values   []any
	count    int64
	failures []error
}

func (e *TopologyNativeExecutor) executeScatterCommandGroup(
	ctx context.Context,
	name string,
	group *scatterRouteGroup,
	snapshot topologyRoutingSnapshot,
	requestContext any,
	hasRequestContext bool,
	allowSafeReroute bool,
) topologyScatterGroupResult {
	destructive := destructiveScatterCommand(name)
	adapter, err := e.adapterForTopologyRoute(group.route, snapshot)
	if err != nil {
		return failedScatterGroup(destructive, group, err)
	}
	args := scatterCommandArgs(name, group.keys, requestContext, hasRequestContext)
	command, err := buildNativeCommand(args)
	if err != nil {
		return failedScatterGroup(destructive, group, err)
	}
	groupCtx, cancel := nativeContextWithBudget(ctx, adapter.opts.Timeout, command.budget)
	if cancel != nil {
		defer cancel()
	}
	value, err := adapter.doNativeCommandOnLane(groupCtx, command, group.route.LaneID)
	if err != nil {
		if allowSafeReroute {
			retry, retryErr := e.refreshAndCanRetrySafeReroute(groupCtx, err, 0)
			if retryErr != nil {
				return failedScatterGroup(destructive, group, retryErr)
			}
			if retry {
				return e.retryScatterCommandGroup(groupCtx, name, group, requestContext, hasRequestContext)
			}
		}
		if !allowSafeReroute && isRetryableRouteError(err) {
			_ = e.RefreshTopology(groupCtx)
		}
		return failedScatterGroup(destructive, group, err)
	}
	return decodedScatterGroup(name, group, value, destructive)
}

func (e *TopologyNativeExecutor) retryScatterCommandGroup(
	ctx context.Context,
	name string,
	group *scatterRouteGroup,
	requestContext any,
	hasRequestContext bool,
) topologyScatterGroupResult {
	value, err := e.scatterCommandAttempt(
		ctx, name, group.keys, requestContext, hasRequestContext, false,
	)
	if err != nil {
		var partial *TopologyPartialWriteError
		if errors.As(err, &partial) {
			return topologyScatterGroupResult{count: partial.Succeeded, failures: partial.Failures}
		}
		return topologyScatterGroupResult{failures: []error{err}}
	}
	if name == "MGET" {
		items, ok := value.([]any)
		if !ok || len(items) != len(group.keys) {
			return failedScatterGroup(false, group, fmt.Errorf(
				"MGET retry returned %T with %d values, expected %d", value, len(items), len(group.keys),
			))
		}
		return topologyScatterGroupResult{values: items}
	}
	count, err := responseInt64(value, nil)
	if err != nil || count < 0 || count > int64(len(group.keys)) {
		if err == nil {
			err = fmt.Errorf("%s retry count %d is outside valid range 0..%d", name, count, len(group.keys))
		}
		return failedScatterGroup(destructiveScatterCommand(name), group, err)
	}
	return topologyScatterGroupResult{count: count}
}

func scatterCommandArgs(name string, keys []any, requestContext any, hasRequestContext bool) []any {
	prefix := 1
	if hasRequestContext {
		prefix = 2
	}
	args := make([]any, prefix, prefix+len(keys)+2)
	if hasRequestContext {
		args[0], args[1] = "COMMAND_EXEC", name
	} else {
		args[0] = name
	}
	args = append(args, keys...)
	if hasRequestContext {
		args = appendNativeRequestContext(args, requestContext)
	}
	return args
}

func decodedScatterGroup(
	name string,
	group *scatterRouteGroup,
	value any,
	destructive bool,
) topologyScatterGroupResult {
	if name == "MGET" {
		items, ok := value.([]any)
		if !ok || len(items) != len(group.positions) {
			return failedScatterGroup(destructive, group, fmt.Errorf(
				"MGET shard returned %T with %d values, expected %d", value, len(items), len(group.positions),
			))
		}
		return topologyScatterGroupResult{values: items}
	}
	count, err := responseInt64(value, nil)
	if err != nil {
		return failedScatterGroup(destructive, group, err)
	}
	if count < 0 || count > int64(len(group.keys)) {
		return failedScatterGroup(destructive, group, fmt.Errorf(
			"%s shard count %d is outside valid range 0..%d", name, count, len(group.keys),
		))
	}
	return topologyScatterGroupResult{count: count}
}

func failedScatterGroup(
	attributed bool,
	group *scatterRouteGroup,
	err error,
) topologyScatterGroupResult {
	return topologyScatterGroupResult{
		failures: []error{topologyKeyWriteFailure(attributed, group.route, group.keys, err)},
	}
}
