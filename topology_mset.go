package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

var errTopologyCrossSlotMSet = errors.New("ferricstore topology MSET spans hash slots")

type topologyMSetGroup struct {
	route      RoutingRoute
	command    nativeCommand
	adapter    *NativeExecutor
	prepareErr error
	stringKeys []string
	keys       []any
	values     []any
}

func topologyMSetCommand(args []any) (
	keys []any,
	values []any,
	requestContext any,
	hasRequestContext bool,
	matched bool,
	err error,
) {
	commandArgs := topologyCommandArgs(args)
	if len(commandArgs) == 0 || commandName(commandArgs) != "MSET" {
		return nil, nil, nil, false, false, nil
	}
	matched = true
	if len(commandArgs) < 3 || (len(commandArgs)-1)%2 != 0 {
		return nil, nil, nil, false, true, errors.New("MSET requires at least one key/value pair")
	}
	keys = make([]any, 0, (len(commandArgs)-1)/2)
	values = make([]any, 0, (len(commandArgs)-1)/2)
	for index := 1; index < len(commandArgs); index += 2 {
		if !isRouteKey(commandArgs[index]) {
			return nil, nil, nil, false, true, fmt.Errorf("MSET key %d has unsupported type %T", len(keys), commandArgs[index])
		}
		keys = append(keys, commandArgs[index])
		values = append(values, commandArgs[index+1])
	}
	requestContext, hasRequestContext = topologyRequestContext(args)
	return keys, values, requestContext, hasRequestContext, true, nil
}

func (e *TopologyNativeExecutor) scatterStringMSet(ctx context.Context, keys []string, values []any) (any, error) {
	if len(keys) != len(values) {
		return nil, fmt.Errorf("MSET received %d keys and %d values", len(keys), len(values))
	}
	groups := make(map[int]*topologyMSetGroup)
	for index, key := range keys {
		slot := routeSlotForKey(key)
		group := groups[slot]
		if group == nil {
			group = &topologyMSetGroup{}
			groups[slot] = group
		}
		group.stringKeys = append(group.stringKeys, key)
		group.values = append(group.values, values[index])
	}
	return e.executeTopologyMSet(ctx, groups, nil, false)
}

func (e *TopologyNativeExecutor) scatterMSet(
	ctx context.Context,
	keys []any,
	values []any,
	requestContext any,
	hasRequestContext bool,
) (any, error) {
	if len(keys) != len(values) {
		return nil, fmt.Errorf("MSET received %d keys and %d values", len(keys), len(values))
	}
	// scatterMSet is reached only after routeData has identified a cross-slot
	// MSET. Reject the policy before invoking a caller-provided codec.
	if e.crossShardWrites != CrossShardWritePerShard {
		return nil, topologyCrossSlotMSetDisabledError()
	}
	groups := make(map[int]*topologyMSetGroup)
	for index, key := range keys {
		slot := routeSlotForKey(key)
		group := groups[slot]
		if group == nil {
			group = &topologyMSetGroup{}
			groups[slot] = group
		}
		group.keys = append(group.keys, key)
		group.values = append(group.values, values[index])
	}
	return e.executeTopologyMSet(ctx, groups, requestContext, hasRequestContext)
}

func (e *TopologyNativeExecutor) executeTopologyMSet(
	ctx context.Context,
	groups map[int]*topologyMSetGroup,
	requestContext any,
	hasRequestContext bool,
) (any, error) {
	if len(groups) == 0 {
		return nil, errors.New("MSET requires at least one key/value pair")
	}
	if len(groups) > 1 && e.crossShardWrites != CrossShardWritePerShard {
		return nil, topologyCrossSlotMSetDisabledError()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	preflightLimit := e.maxMSetPreflightBytes
	if preflightLimit <= 0 {
		preflightLimit = nativeMaxFrameBytes
	}
	// Every shard body must be validated before the first write. Cap the bodies
	// retained for that guarantee to one native-frame budget in aggregate.
	preflightBytes := 0
	for _, group := range orderedTopologyMSetGroups(groups) {
		command, err := topologyMSetNativeCommand(group, requestContext, hasRequestContext)
		if err != nil {
			return nil, err
		}
		body, err := encodeNativeValueWithLimit(command.payload, nativeMaxFrameBytes)
		if err != nil {
			return nil, fmt.Errorf("preflight MSET native payload: %w", err)
		}
		if len(body) > preflightLimit-preflightBytes {
			return nil, fmt.Errorf("aggregate MSET preflight payload exceeds %d-byte limit", preflightLimit)
		}
		preflightBytes += len(body)
		command.payload = nativePreencodedPayload{body: body}
		group.command = command
	}
	snapshot, err := e.planTopologyMSetRoutes(ctx, groups)
	if err != nil {
		return nil, err
	}
	if err := e.prepareTopologyMSetGroups(ctx, groups, snapshot); err != nil {
		return nil, err
	}

	type msetResult struct {
		count int64
		err   error
	}
	results := make(chan msetResult, len(groups))
	tasks := make([]func(), 0, len(groups))
	for _, group := range groups {
		group := group
		tasks = append(tasks, func() {
			if group.prepareErr != nil {
				results <- msetResult{err: topologyMSetWriteFailure(group, group.prepareErr)}
				return
			}
			value, err := group.adapter.doNativeCommandOnLane(ctx, group.command, group.route.LaneID)
			if err != nil {
				if isRetryableRouteError(err) {
					_ = e.RefreshTopology(ctx)
				}
				results <- msetResult{err: topologyMSetWriteFailure(group, err)}
				return
			}
			if _, err := responseOK(value, nil); err != nil {
				results <- msetResult{err: topologyMSetWriteFailure(group, err)}
				return
			}
			results <- msetResult{count: int64(len(group.values))}
		})
	}
	runBoundedTopologyTasks(maxTopologyConcurrentTasks, tasks)
	close(results)

	var succeeded int64
	var failures []error
	for result := range results {
		succeeded += result.count
		if result.err != nil {
			failures = append(failures, result.err)
		}
	}
	if len(failures) > 0 {
		return nil, &TopologyPartialWriteError{Command: "MSET", Succeeded: succeeded, Failures: failures}
	}
	return []byte("OK"), nil
}

func orderedTopologyMSetGroups(groups map[int]*topologyMSetGroup) []*topologyMSetGroup {
	ordered := make([]*topologyMSetGroup, 0, len(groups))
	for _, group := range groups {
		ordered = append(ordered, group)
	}
	sort.Slice(ordered, func(left, right int) bool {
		return topologyMSetGroupOrderKey(ordered[left]) < topologyMSetGroupOrderKey(ordered[right])
	})
	return ordered
}

func topologyMSetGroupOrderKey(group *topologyMSetGroup) string {
	if len(group.stringKeys) > 0 {
		return group.stringKeys[0]
	}
	if len(group.keys) > 0 {
		return asString(group.keys[0])
	}
	return ""
}

func (e *TopologyNativeExecutor) prepareTopologyMSetGroups(
	ctx context.Context,
	groups map[int]*topologyMSetGroup,
	snapshot topologyRoutingSnapshot,
) error {
	type prepareResult struct {
		group   *topologyMSetGroup
		adapter *NativeExecutor
		err     error
		fatal   bool
	}
	results := make(chan prepareResult, len(groups))
	tasks := make([]func(), 0, len(groups))
	for _, group := range groups {
		group := group
		tasks = append(tasks, func() {
			adapter, err := e.adapterForTopologyRoute(group.route, snapshot)
			if err != nil {
				results <- prepareResult{group: group, err: err, fatal: errors.Is(err, errTopologyStaleRoute)}
				return
			}
			if err := adapter.ensureConnectedLocked(ctx); err != nil {
				results <- prepareResult{group: group, err: err}
				return
			}
			preencoded, ok := group.command.payload.(nativePreencodedPayload)
			if !ok {
				results <- prepareResult{
					group: group,
					err:   errors.New("ferricstore internal MSET payload was not preencoded"),
					fatal: true,
				}
				return
			}
			adapter.mu.Lock()
			maxFrameBytes := adapter.maxRequestFrameBytes
			adapter.mu.Unlock()
			if maxFrameBytes <= 0 {
				maxFrameBytes = nativeDefaultRequestFrameBytes
			}
			if len(preencoded.body) > maxFrameBytes {
				results <- prepareResult{
					group: group,
					err: fmt.Errorf(
						"ferricstore native request body exceeds server-advertised %d-byte frame limit",
						maxFrameBytes,
					),
					fatal: true,
				}
				return
			}
			results <- prepareResult{group: group, adapter: adapter}
		})
	}
	runBoundedTopologyTasks(maxTopologyConcurrentTasks, tasks)
	close(results)

	var fatalErr error
	for result := range results {
		result.group.adapter = result.adapter
		result.group.prepareErr = result.err
		if result.fatal && fatalErr == nil {
			fatalErr = result.err
		}
	}
	return fatalErr
}

func (e *TopologyNativeExecutor) planTopologyMSetRoutes(
	ctx context.Context,
	groups map[int]*topologyMSetGroup,
) (topologyRoutingSnapshot, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	refreshed := false
	for range maxTopologyPlanningAttempts {
		snapshot, err := e.captureRoutingTopology()
		if err != nil {
			return topologyRoutingSnapshot{}, err
		}
		planned := make(map[*topologyMSetGroup]RoutingRoute, len(groups))
		var planErr error
		for _, group := range groups {
			var key any
			if group.stringKeys != nil {
				key = group.stringKeys[0]
			} else {
				key = group.keys[0]
			}
			route, err := e.routeInSnapshot(snapshot, key)
			if err != nil {
				planErr = err
				break
			}
			planned[group] = route
		}
		if !e.routingSnapshotCurrent(snapshot) {
			continue
		}
		if planErr == nil {
			for group, route := range planned {
				group.route = route
			}
			return snapshot, nil
		}
		if refreshed {
			return topologyRoutingSnapshot{}, planErr
		}
		if err := e.refreshTopologyAtVersion(ctx, snapshot.version); err != nil {
			return topologyRoutingSnapshot{}, err
		}
		refreshed = true
	}
	return topologyRoutingSnapshot{}, fmt.Errorf("%w after %d MSET planning attempts", errTopologyStaleRoute, maxTopologyPlanningAttempts)
}

func topologyMSetWriteFailure(group *topologyMSetGroup, err error) error {
	if group.stringKeys != nil {
		return topologyStringKeyWriteFailure(true, group.route, group.stringKeys, err)
	}
	return topologyKeyWriteFailure(true, group.route, group.keys, err)
}

func topologyCrossSlotMSetDisabledError() error {
	return errors.New("cross-slot MSET is disabled; opt in with WithTopologyCrossShardWritePolicy(CrossShardWritePerShard)")
}

func topologyMSetNativeCommand(
	group *topologyMSetGroup,
	requestContext any,
	hasRequestContext bool,
) (nativeCommand, error) {
	if group.stringKeys != nil && !hasRequestContext {
		return newNativeMSetCommand(group.stringKeys, group.values)
	}
	prefix := 1
	if hasRequestContext {
		prefix = 2
	}
	args := make([]any, prefix, prefix+2*len(group.values)+2)
	if hasRequestContext {
		args[0], args[1] = "COMMAND_EXEC", "MSET"
	} else {
		args[0] = "MSET"
	}
	for index, key := range group.keys {
		args = append(args, key, group.values[index])
	}
	if hasRequestContext {
		args = appendNativeRequestContext(args, requestContext)
	}
	command, err := buildNativeCommand(args)
	if err != nil {
		return nativeCommand{}, err
	}
	command.budget = blockingCommandBudget(args)
	return command, nil
}
