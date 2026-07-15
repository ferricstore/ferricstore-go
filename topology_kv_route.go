package ferricstore

import (
	"context"
	"fmt"
)

func (e *TopologyNativeExecutor) singleRouteForStringKeys(
	ctx context.Context,
	keys []string,
) (RoutingRoute, bool, error) {
	if len(keys) == 0 {
		return RoutingRoute{}, false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	topology, version, err := e.routingTopologySnapshot()
	if err != nil {
		return RoutingRoute{}, false, err
	}
	route, same, _, err := e.singleRouteInTopology(topology, keys)
	if err == nil {
		return route, same, nil
	}
	if refreshErr := e.refreshTopologyAtVersion(ctx, version); refreshErr != nil {
		return RoutingRoute{}, false, refreshErr
	}
	topology, _, err = e.routingTopologySnapshot()
	if err != nil {
		return RoutingRoute{}, false, err
	}
	route, same, _, err = e.singleRouteInTopology(topology, keys)
	return route, same, err
}

func (e *TopologyNativeExecutor) planStringKeyRoutes(
	ctx context.Context,
	keys []string,
	includePositions bool,
) (RoutingRoute, map[topologyRouteIdentity]*stringKeyRouteGroup, error) {
	route, _, groups, err := e.planStringKeyRoutesSnapshot(ctx, keys, includePositions)
	return route, groups, err
}

func (e *TopologyNativeExecutor) planStringKeyRoutesSnapshot(
	ctx context.Context,
	keys []string,
	includePositions bool,
) (RoutingRoute, topologyRoutingSnapshot, map[topologyRouteIdentity]*stringKeyRouteGroup, error) {
	if len(keys) == 0 {
		return RoutingRoute{}, topologyRoutingSnapshot{}, nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	refreshed := false
	for range maxTopologyPlanningAttempts {
		snapshot, err := e.captureRoutingTopology()
		if err != nil {
			return RoutingRoute{}, topologyRoutingSnapshot{}, nil, err
		}
		route, groups, err := e.planStringKeyRoutesInTopology(snapshot.topology, keys, includePositions)
		if !e.routingSnapshotCurrent(snapshot) {
			continue
		}
		if err == nil {
			return route, snapshot, groups, nil
		}
		if refreshed {
			return RoutingRoute{}, topologyRoutingSnapshot{}, nil, err
		}
		if refreshErr := e.refreshTopologyAtVersion(ctx, snapshot.version); refreshErr != nil {
			return RoutingRoute{}, topologyRoutingSnapshot{}, nil, refreshErr
		}
		refreshed = true
	}
	return RoutingRoute{}, topologyRoutingSnapshot{}, nil, fmt.Errorf(
		"%w after %d typed-key planning attempts",
		errTopologyStaleRoute,
		maxTopologyPlanningAttempts,
	)
}

func (e *TopologyNativeExecutor) routingTopologySnapshot() (*RoutingTopology, uint64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, 0, errTopologyClosed
	}
	return e.topology, e.topologyVersion, nil
}

func (e *TopologyNativeExecutor) singleRouteInTopology(
	topology *RoutingTopology,
	keys []string,
) (RoutingRoute, bool, int, error) {
	first, err := routeStringKeyInTopology(topology, keys[0])
	if err != nil {
		return RoutingRoute{}, false, -1, err
	}
	if err := e.validateEndpoint(first.Endpoint); err != nil {
		return RoutingRoute{}, false, -1, err
	}
	identity := routeIdentity(first)
	for offset, key := range keys[1:] {
		route, err := routeStringKeyInTopology(topology, key)
		if err != nil {
			return RoutingRoute{}, false, -1, err
		}
		if routeIdentity(route) != identity {
			return first, false, offset + 1, nil
		}
	}
	return first, true, -1, nil
}

func (e *TopologyNativeExecutor) planStringKeyRoutesInTopology(
	topology *RoutingTopology,
	keys []string,
	includePositions bool,
) (RoutingRoute, map[topologyRouteIdentity]*stringKeyRouteGroup, error) {
	first, same, divergence, err := e.singleRouteInTopology(topology, keys)
	if err != nil || same {
		return first, nil, err
	}
	divergentRoute, err := routeStringKeyInTopology(topology, keys[divergence])
	if err != nil {
		return RoutingRoute{}, nil, err
	}
	if err := e.validateEndpoint(divergentRoute.Endpoint); err != nil {
		return RoutingRoute{}, nil, err
	}
	firstIdentity := routeIdentity(first)
	divergentIdentity := routeIdentity(divergentRoute)
	groups := make(map[topologyRouteIdentity]*stringKeyRouteGroup, 2)
	firstGroup := &stringKeyRouteGroup{
		route: first,
		keys:  append([]string(nil), keys[:divergence]...),
	}
	divergentGroup := &stringKeyRouteGroup{
		route: divergentRoute,
		keys:  []string{keys[divergence]},
	}
	if includePositions {
		firstGroup.positions = make([]int, divergence)
		for index := range firstGroup.positions {
			firstGroup.positions[index] = index
		}
		divergentGroup.positions = []int{divergence}
	}
	groups[firstIdentity] = firstGroup
	groups[divergentIdentity] = divergentGroup

	for position := divergence + 1; position < len(keys); position++ {
		route, err := routeStringKeyInTopology(topology, keys[position])
		if err != nil {
			return RoutingRoute{}, nil, err
		}
		identity := routeIdentity(route)
		group := groups[identity]
		if group == nil {
			if err := e.validateEndpoint(route.Endpoint); err != nil {
				return RoutingRoute{}, nil, err
			}
			group = &stringKeyRouteGroup{route: route}
			groups[identity] = group
		}
		group.keys = append(group.keys, keys[position])
		if includePositions {
			group.positions = append(group.positions, position)
		}
	}
	return first, groups, nil
}

func routeStringKeyInTopology(topology *RoutingTopology, key string) (RoutingRoute, error) {
	if topology == nil {
		return RoutingRoute{}, fmt.Errorf("ferricstore topology is empty")
	}
	slot := routeSlotForString(key)
	route := topology.slots[slot]
	if route == nil {
		return RoutingRoute{}, fmt.Errorf("no route for slot %d", slot)
	}
	out := *route
	out.Slot = slot
	return out, nil
}

func (e *TopologyNativeExecutor) keyValueMGetOnRoute(
	ctx context.Context,
	keys []string,
	route RoutingRoute,
	snapshot topologyRoutingSnapshot,
) (any, error) {
	adapter, err := e.adapterForTopologyRoute(route, snapshot)
	if err != nil {
		return nil, err
	}
	value, err := adapter.doNativeCommandOnLane(ctx, newNativeMGetCommand(keys), route.LaneID)
	if err != nil {
		if isRetryableRouteError(err) {
			_ = e.RefreshTopology(ctx)
		}
		return nil, err
	}
	items, ok := value.([]any)
	if !ok || len(items) != len(keys) {
		return nil, fmt.Errorf("MGET shard returned %T with %d values, expected %d", value, len(items), len(keys))
	}
	return items, nil
}

func (e *TopologyNativeExecutor) keyValueCountOnRoute(
	ctx context.Context,
	name string,
	keys []string,
	route RoutingRoute,
	snapshot topologyRoutingSnapshot,
) (any, error) {
	adapter, err := e.adapterForTopologyRoute(route, snapshot)
	if err != nil {
		return nil, err
	}
	var command nativeCommand
	switch name {
	case "DEL":
		command = newNativeDelCommand(keys)
	case "EXISTS":
		command = newNativeExistsCommand(keys)
	default:
		return nil, fmt.Errorf("unsupported typed key-count command %s", name)
	}
	value, err := adapter.doNativeCommandOnLane(ctx, command, route.LaneID)
	if err != nil {
		if isRetryableRouteError(err) {
			_ = e.RefreshTopology(ctx)
		}
		return nil, err
	}
	count, err := responseInt64(value, nil)
	if err != nil {
		return nil, err
	}
	if count < 0 || count > int64(len(keys)) {
		return nil, fmt.Errorf("%s shard count %d is outside valid range 0..%d", name, count, len(keys))
	}
	return count, nil
}
