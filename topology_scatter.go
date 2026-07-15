package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
				if err != nil {
					if isRetryableRouteError(err) {
						_ = e.RefreshTopology(ctx)
					}
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

func runBoundedTopologyTasks(limit int, tasks []func()) {
	if len(tasks) == 0 {
		return
	}
	if len(tasks) == 1 {
		tasks[0]()
		return
	}
	if limit <= 0 || limit > len(tasks) {
		limit = len(tasks)
	}
	jobs := make(chan func(), limit)
	var workers sync.WaitGroup
	workers.Add(limit)
	for range limit {
		go func() {
			defer workers.Done()
			for task := range jobs {
				task()
			}
		}()
	}
	for _, task := range tasks {
		jobs <- task
	}
	close(jobs)
	workers.Wait()
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
			return nil, &TopologyPartialWriteError{Command: name, Succeeded: total, Failures: failures}
		}
		return nil, failures[0]
	}
	return total, nil
}

func (e *TopologyNativeExecutor) scatterCommand(ctx context.Context, name string, keys []any) (any, error) {
	return e.scatterCommandWithContext(ctx, name, keys, nil, false)
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
			return nil, &TopologyPartialWriteError{Command: name, Succeeded: total, Failures: failures}
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

// TopologyWriteFailure identifies the route and keys affected by one failed
// shard operation within a partially completed cross-shard write.
type TopologyWriteFailure struct {
	Route RoutingRoute
	Keys  []string
	Err   error
}

func (e *TopologyWriteFailure) Error() string {
	if e == nil {
		return ""
	}
	endpoint := e.Route.EndpointKey
	if endpoint == "" {
		endpoint = endpointKey(e.Route.Endpoint)
	}
	return fmt.Sprintf(
		"topology write to shard %d lane %d endpoint %s failed for %d keys: %v",
		e.Route.Shard, e.Route.LaneID, endpoint, len(e.Keys), e.Err,
	)
}

func (e *TopologyWriteFailure) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func topologyStringKeyWriteFailure(
	attributed bool,
	route RoutingRoute,
	keys []string,
	err error,
) error {
	if !attributed || err == nil {
		return err
	}
	return &TopologyWriteFailure{
		Route: route,
		Keys:  append([]string(nil), keys...),
		Err:   err,
	}
}

func topologyKeyWriteFailure(
	attributed bool,
	route RoutingRoute,
	keys []any,
	err error,
) error {
	if !attributed || err == nil {
		return err
	}
	stringKeys := make([]string, len(keys))
	for index, key := range keys {
		stringKeys[index] = asString(key)
	}
	return &TopologyWriteFailure{Route: route, Keys: stringKeys, Err: err}
}

// TopologyPartialWriteError reports the observable result of an explicitly
// enabled cross-shard destructive command when one or more shards fail.
type TopologyPartialWriteError struct {
	Command   string
	Succeeded int64
	Failures  []error
}

func (e *TopologyPartialWriteError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("cross-shard %s partially completed: %d successful mutations, %d shard failures", e.Command, e.Succeeded, len(e.Failures))
}

func (e *TopologyPartialWriteError) Unwrap() []error {
	if e == nil {
		return nil
	}
	return e.Failures
}
