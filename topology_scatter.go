package ferricstore

import (
	"context"
	"errors"
	"fmt"
)

type topologyRouteIdentity struct {
	endpointKey string
	laneID      uint32
}

func routeIdentity(route RoutingRoute) topologyRouteIdentity {
	key := route.EndpointKey
	if key == "" {
		key = endpointKey(route.Endpoint)
	}
	return topologyRouteIdentity{endpointKey: key, laneID: route.LaneID}
}

func (e *TopologyNativeExecutor) Pipeline(ctx context.Context, commands [][]any) ([]any, error) {
	results, err := e.pipelineDetailed(ctx, commands)
	if err != nil {
		return nil, err
	}
	return pipelineResultValues(results)
}

func (e *TopologyNativeExecutor) pipelineDetailed(ctx context.Context, commands [][]any) ([]pipelineItemResult, error) {
	return e.pipelineDetailedUnlocked(ctx, commands)
}

func (e *TopologyNativeExecutor) pipelineDetailedUnlocked(ctx context.Context, commands [][]any) ([]pipelineItemResult, error) {
	if err := e.assertOpen(); err != nil {
		return nil, err
	}
	if len(commands) == 0 {
		return nil, nil
	}
	for _, command := range commands {
		if name, mutates := connectionStateMutationCommand(command); mutates {
			return nil, fmt.Errorf("%s is connection-local and cannot be used in a topology pipeline", name)
		}
	}
	type pipelineGroup struct {
		route    RoutingRoute
		commands [][]any
		indices  []int
	}
	type pipelineCommand struct {
		args  []any
		index int
	}
	results := make([]pipelineItemResult, len(commands))
	groups := make(map[topologyRouteIdentity]*pipelineGroup)
	var waveSnapshot topologyRoutingSnapshot
	var waveCommands []pipelineCommand
	buildWaveGroups := func(snapshot topologyRoutingSnapshot) (map[topologyRouteIdentity]*pipelineGroup, error) {
		rebuilt := make(map[topologyRouteIdentity]*pipelineGroup)
		for _, pending := range waveCommands {
			routeData, err := e.routeDataInSnapshot(pending.args, snapshot)
			if err != nil {
				return nil, err
			}
			if routeData == nil || routeData.command.flags != 0 {
				return nil, errors.New("ferricstore topology pipeline wave changed routing shape")
			}
			key := routeIdentity(routeData.route)
			group := rebuilt[key]
			if group == nil {
				group = &pipelineGroup{route: routeData.route}
				rebuilt[key] = group
			}
			group.commands = append(group.commands, pending.args)
			group.indices = append(group.indices, pending.index)
		}
		return rebuilt, nil
	}
	flushRoutedWave := func() {
		if len(groups) == 0 {
			return
		}
		var planningErr error
		for attempt := 0; !e.routingSnapshotCurrent(waveSnapshot) && attempt < maxTopologyPlanningAttempts; attempt++ {
			snapshot, err := e.captureRoutingTopology()
			if err != nil {
				planningErr = err
				break
			}
			rebuilt, err := buildWaveGroups(snapshot)
			if err != nil {
				planningErr = err
				break
			}
			groups, waveSnapshot = rebuilt, snapshot
		}
		if planningErr == nil && !e.routingSnapshotCurrent(waveSnapshot) {
			planningErr = errTopologyStaleRoute
		}
		if planningErr != nil {
			for _, pending := range waveCommands {
				results[pending.index].err = planningErr
			}
			groups = make(map[topologyRouteIdentity]*pipelineGroup)
			waveSnapshot = topologyRoutingSnapshot{}
			waveCommands = waveCommands[:0]
			return
		}
		snapshot := waveSnapshot
		singleRouteWave := len(groups) == 1
		tasks := make([]func(), 0, len(groups))
		for _, group := range groups {
			group := group
			tasks = append(tasks, func() {
				setGroupError := func(err error) {
					for _, index := range group.indices {
						results[index].err = err
					}
				}
				adapter, err := e.adapterForTopologyRoute(group.route, snapshot)
				if err != nil {
					setGroupError(err)
					return
				}
				items, err := adapter.pipelineDetailedOnLane(ctx, group.commands, group.route.LaneID)
				routeErr, safeToRetryAll := topologyPipelineRouteDisposition(items)
				retry := false
				switch {
				case err != nil && singleRouteWave:
					retry = e.refreshAndCanRetrySafeReroute(ctx, err, 0)
				case err != nil && isRetryableRouteError(err):
					_ = e.RefreshTopology(ctx)
				case routeErr != nil && singleRouteWave && safeToRetryAll:
					retry = e.refreshAndCanRetrySafeReroute(ctx, routeErr, 0)
				case routeErr != nil:
					_ = e.RefreshTopology(ctx)
				}
				if retry {
					retrySnapshot, retryErr := e.captureRoutingTopology()
					if retryErr == nil {
						var retryGroups map[topologyRouteIdentity]*pipelineGroup
						retryGroups, retryErr = buildWaveGroups(retrySnapshot)
						if retryErr == nil && len(retryGroups) != 1 {
							retryErr = errTopologyStaleRoute
						}
						if retryErr == nil {
							for _, retryGroup := range retryGroups {
								group = retryGroup
							}
							adapter, retryErr = e.adapterForTopologyRoute(group.route, retrySnapshot)
						}
						if retryErr == nil {
							items, retryErr = adapter.pipelineDetailedOnLane(ctx, group.commands, group.route.LaneID)
						}
					}
					err = retryErr
				}
				if err != nil {
					setGroupError(err)
					return
				}
				if len(items) != len(group.indices) {
					setGroupError(fmt.Errorf("ferricstore routed pipeline returned %d results for %d commands", len(items), len(group.indices)))
					return
				}
				for i, index := range group.indices {
					results[index] = validateTopologyPipelineScatterResult(
						group.commands[i],
						group.route,
						items[i],
					)
				}
			})
		}
		groups = make(map[topologyRouteIdentity]*pipelineGroup)
		runBoundedTopologyTasks(maxTopologyConcurrentTasks, tasks)
		waveSnapshot = topologyRoutingSnapshot{}
		waveCommands = waveCommands[:0]
	}

	for index, args := range commands {
		if len(args) == 0 {
			results[index].err = errors.New("ferricstore command requires at least a command name")
			continue
		}
		_, scatterKeys, scatter := safeScatterCommand(args)
		scatter = scatter && len(scatterKeys) > 0
		invalidScatterKey := false
		if scatter {
			for _, key := range scatterKeys {
				if !isRouteKey(key) {
					invalidScatterKey = true
					break
				}
			}
		}
		var routeData *topologyRouteData
		var err error
		if !invalidScatterKey {
			if waveSnapshot.topology == nil {
				routeData, err = e.routeData(ctx, args)
				if routeData != nil {
					waveSnapshot = routeData.snapshot
				}
			} else {
				routeData, err = e.routeDataInSnapshot(args, waveSnapshot)
				if err != nil && !e.routingSnapshotCurrent(waveSnapshot) {
					flushRoutedWave()
					routeData, err = e.routeData(ctx, args)
					if routeData != nil {
						waveSnapshot = routeData.snapshot
					}
				}
			}
		}
		if err != nil {
			if err == errTopologyCrossSlotMSet {
				flushRoutedWave()
				value, commandErr := e.doUnlocked(ctx, args...)
				results[index] = pipelineItemResult{value: value, err: commandErr}
				continue
			}
			results[index].err = err
			continue
		}
		if scatter && (invalidScatterKey || routeData == nil) {
			// Cross-route scatter can observe earlier writes on any participating
			// route, so it is an ordering barrier. Single-route scatter remains in
			// the routed batch below and keeps the fast path fully pipelined.
			flushRoutedWave()
			value, err := e.doUnlocked(ctx, args...)
			results[index] = pipelineItemResult{value: value, err: err}
			continue
		}
		if routeData == nil || routeData.command.flags != 0 {
			flushRoutedWave()
			value, err := e.doUnlocked(ctx, args...)
			results[index] = pipelineItemResult{value: value, err: err}
			continue
		}
		key := routeIdentity(routeData.route)
		group := groups[key]
		if group == nil {
			group = &pipelineGroup{route: routeData.route}
			groups[key] = group
		}
		group.commands = append(group.commands, args)
		group.indices = append(group.indices, index)
		waveCommands = append(waveCommands, pipelineCommand{args: args, index: index})
	}
	flushRoutedWave()
	return results, nil
}

func safeScatterCommand(args []any) (string, []any, bool) {
	args = topologyCommandArgs(args)
	if len(args) == 0 {
		return "", nil, false
	}
	name := commandName(args)
	keys, policy, ok := topologyPolicyKeys(name, args)
	if !ok || !policy.scatter {
		return "", nil, false
	}
	return name, keys, true
}

type scatterRouteGroup struct {
	route     RoutingRoute
	keys      []any
	positions []int
}

type stringKeyRouteGroup struct {
	route     RoutingRoute
	keys      []string
	positions []int
}

func (e *TopologyNativeExecutor) scatterStringMGet(
	ctx context.Context,
	keys []string,
	groups map[topologyRouteIdentity]*stringKeyRouteGroup,
	snapshot topologyRoutingSnapshot,
) (any, error) {
	values := make([]any, len(keys))
	results := make(chan error, len(groups))
	tasks := make([]func(), 0, len(groups))
	for _, group := range groups {
		group := group
		tasks = append(tasks, func() {
			adapter, err := e.adapterForTopologyRoute(group.route, snapshot)
			if err != nil {
				results <- err
				return
			}
			value, err := adapter.doNativeCommandOnLane(ctx, newNativeMGetCommand(group.keys), group.route.LaneID)
			if err != nil {
				if isRetryableRouteError(err) {
					_ = e.RefreshTopology(ctx)
				}
				results <- err
				return
			}
			items, ok := value.([]any)
			if !ok || len(items) != len(group.positions) {
				results <- fmt.Errorf("MGET shard returned %T with %d values, expected %d", value, len(items), len(group.positions))
				return
			}
			for index, position := range group.positions {
				values[position] = items[index]
			}
			results <- nil
		})
	}
	runBoundedTopologyTasks(maxTopologyConcurrentTasks, tasks)
	close(results)
	for err := range results {
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}

func (e *TopologyNativeExecutor) scatterStringCountCommand(
	ctx context.Context,
	name string,
	groups map[topologyRouteIdentity]*stringKeyRouteGroup,
	snapshot topologyRoutingSnapshot,
) (any, error) {
	destructive := destructiveScatterCommand(name)
	if len(groups) > 1 && destructive && e.crossShardWrites != CrossShardWritePerShard {
		return nil, fmt.Errorf("cross-shard %s is disabled; opt in with WithTopologyCrossShardWritePolicy(CrossShardWritePerShard)", name)
	}

	type countResult struct {
		count int64
		err   error
	}
	results := make(chan countResult, len(groups))
	tasks := make([]func(), 0, len(groups))
	for _, group := range groups {
		group := group
		tasks = append(tasks, func() {
			adapter, err := e.adapterForTopologyRoute(group.route, snapshot)
			if err != nil {
				results <- countResult{err: topologyStringKeyWriteFailure(destructive, group.route, group.keys, err)}
				return
			}
			var command nativeCommand
			switch name {
			case "DEL":
				command = newNativeDelCommand(group.keys)
			case "EXISTS":
				command = newNativeExistsCommand(group.keys)
			default:
				err := fmt.Errorf("unsupported typed key-count command %s", name)
				results <- countResult{err: topologyStringKeyWriteFailure(destructive, group.route, group.keys, err)}
				return
			}
			value, err := adapter.doNativeCommandOnLane(ctx, command, group.route.LaneID)
			if err != nil {
				if isRetryableRouteError(err) {
					_ = e.RefreshTopology(ctx)
				}
				results <- countResult{err: topologyStringKeyWriteFailure(destructive, group.route, group.keys, err)}
				return
			}
			count, err := responseInt64(value, nil)
			if err != nil {
				results <- countResult{err: topologyStringKeyWriteFailure(destructive, group.route, group.keys, err)}
				return
			}
			if count < 0 || count > int64(len(group.keys)) {
				err := fmt.Errorf("%s shard count %d is outside valid range 0..%d", name, count, len(group.keys))
				results <- countResult{err: topologyStringKeyWriteFailure(destructive, group.route, group.keys, err)}
				return
			}
			results <- countResult{count: count}
		})
	}
	runBoundedTopologyTasks(maxTopologyConcurrentTasks, tasks)
	close(results)

	var total int64
	var failures []error
	for result := range results {
		total += result.count
		if result.err != nil {
			failures = append(failures, result.err)
		}
	}
	if len(failures) > 0 {
		if destructive {
			return nil, newTopologyPartialWriteError(name, total, failures)
		}
		return nil, failures[0]
	}
	return total, nil
}

func (e *TopologyNativeExecutor) scatterCommandWithContext(ctx context.Context, name string, keys []any, requestContext any, hasRequestContext bool) (any, error) {
	snapshot, groups, err := e.planScatterRoutes(ctx, keys)
	if err != nil {
		return nil, fmt.Errorf("plan %s routes: %w", name, err)
	}
	destructive := destructiveScatterCommand(name)
	if len(groups) > 1 && destructive && e.crossShardWrites != CrossShardWritePerShard {
		return nil, fmt.Errorf("cross-shard %s is disabled; opt in with WithTopologyCrossShardWritePolicy(CrossShardWritePerShard)", name)
	}

	values := topologyScatterValueSlots(name, len(keys))
	type scatterResult struct {
		count int64
		err   error
	}
	results := make(chan scatterResult, len(groups))
	tasks := make([]func(), 0, len(groups))
	for _, group := range groups {
		group := group
		tasks = append(tasks, func() {
			adapter, err := e.adapterForTopologyRoute(group.route, snapshot)
			if err != nil {
				results <- scatterResult{err: topologyKeyWriteFailure(destructive, group.route, group.keys, err)}
				return
			}
			prefix := 1
			if hasRequestContext {
				prefix = 2
			}
			args := make([]any, prefix, prefix+len(group.keys)+2)
			if hasRequestContext {
				args[0], args[1] = "COMMAND_EXEC", name
			} else {
				args[0] = name
			}
			args = append(args, group.keys...)
			if hasRequestContext {
				args = appendNativeRequestContext(args, requestContext)
			}
			command, err := buildNativeCommand(args)
			if err != nil {
				results <- scatterResult{err: topologyKeyWriteFailure(destructive, group.route, group.keys, err)}
				return
			}
			command.budget = blockingCommandBudget(args)
			value, err := adapter.doNativeCommandOnLane(ctx, command, group.route.LaneID)
			if err != nil {
				if isRetryableRouteError(err) {
					_ = e.RefreshTopology(ctx)
				}
				results <- scatterResult{err: topologyKeyWriteFailure(destructive, group.route, group.keys, err)}
				return
			}
			if name == "MGET" {
				items, ok := value.([]any)
				if !ok || len(items) != len(group.positions) {
					err := fmt.Errorf("MGET shard returned %T with %d values, expected %d", value, len(items), len(group.positions))
					results <- scatterResult{err: topologyKeyWriteFailure(destructive, group.route, group.keys, err)}
					return
				}
				for i, position := range group.positions {
					values[position] = items[i]
				}
				results <- scatterResult{}
				return
			}
			count, err := responseInt64(value, nil)
			if err != nil {
				results <- scatterResult{err: topologyKeyWriteFailure(destructive, group.route, group.keys, err)}
				return
			}
			if count < 0 || count > int64(len(group.keys)) {
				err := fmt.Errorf("%s shard count %d is outside valid range 0..%d", name, count, len(group.keys))
				results <- scatterResult{err: topologyKeyWriteFailure(destructive, group.route, group.keys, err)}
				return
			}
			results <- scatterResult{count: count}
		})
	}
	runBoundedTopologyTasks(maxTopologyConcurrentTasks, tasks)
	close(results)
	var total int64
	var failures []error
	for result := range results {
		total += result.count
		if result.err != nil {
			failures = append(failures, result.err)
		}
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

func topologyScatterValueSlots(name string, count int) []any {
	if name == "MGET" {
		return make([]any, count)
	}
	return nil
}

func destructiveScatterCommand(name string) bool {
	policy, ok := topologyCommandPolicies[name]
	return ok && policy.destructive
}
