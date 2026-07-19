package ferricstore

import (
	"context"
	"errors"
	"fmt"
)

const maxTopologyPlanningAttempts = 8

var errTopologyStaleRoute = errors.New("ferricstore topology route became stale before dispatch")

type topologyRouteLookupError struct{ err error }

func (e *topologyRouteLookupError) Error() string { return e.err.Error() }

func (e *topologyRouteLookupError) Unwrap() error { return e.err }

type topologyRoutingSnapshot struct {
	topology *RoutingTopology
	version  uint64
}

func (e *TopologyNativeExecutor) captureRoutingTopology() (topologyRoutingSnapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return topologyRoutingSnapshot{}, errTopologyClosed
	}
	return topologyRoutingSnapshot{topology: e.topology, version: e.topologyVersion}, nil
}

func (e *TopologyNativeExecutor) routingSnapshotCurrent(snapshot topologyRoutingSnapshot) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return !e.closed && e.topology == snapshot.topology && e.topologyVersion == snapshot.version
}

func (e *TopologyNativeExecutor) routingSnapshotErrorLocked(snapshot topologyRoutingSnapshot) error {
	if e.closed {
		return errTopologyClosed
	}
	if snapshot.topology != nil && (e.topology != snapshot.topology || e.topologyVersion != snapshot.version) {
		return errTopologyStaleRoute
	}
	return nil
}

func (e *TopologyNativeExecutor) routeInSnapshot(
	snapshot topologyRoutingSnapshot,
	key any,
) (RoutingRoute, error) {
	if snapshot.topology == nil {
		return RoutingRoute{}, errors.New("ferricstore topology is empty")
	}
	var route RoutingRoute
	var err error
	if slot, ok := key.(topologyRouteSlot); ok {
		route, err = snapshot.topology.routeSlot(int(slot))
	} else {
		route, err = snapshot.topology.RouteKey(key)
	}
	if err != nil {
		return RoutingRoute{}, err
	}
	if err := e.validateEndpoint(route.Endpoint); err != nil {
		return RoutingRoute{}, err
	}
	return route, nil
}

func (e *TopologyNativeExecutor) routeWithRefreshSnapshot(
	ctx context.Context,
	key any,
) (RoutingRoute, topologyRoutingSnapshot, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	refreshed := false
	for range maxTopologyPlanningAttempts {
		snapshot, err := e.captureRoutingTopology()
		if err != nil {
			return RoutingRoute{}, topologyRoutingSnapshot{}, err
		}
		route, err := e.routeInSnapshot(snapshot, key)
		if err != nil {
			if refreshed {
				return RoutingRoute{}, topologyRoutingSnapshot{}, err
			}
			if refreshErr := e.refreshTopologyAtVersion(ctx, snapshot.version); refreshErr != nil {
				return RoutingRoute{}, topologyRoutingSnapshot{}, refreshErr
			}
			refreshed = true
			continue
		}
		if e.routingSnapshotCurrent(snapshot) {
			return route, snapshot, nil
		}
	}
	return RoutingRoute{}, topologyRoutingSnapshot{}, fmt.Errorf("%w after %d planning attempts", errTopologyStaleRoute, maxTopologyPlanningAttempts)
}

func (e *TopologyNativeExecutor) routeDataInSnapshot(
	args []any,
	snapshot topologyRoutingSnapshot,
) (*topologyRouteData, error) {
	if len(args) == 0 {
		return nil, nil
	}
	command, err := buildNativeCommand(args)
	if err != nil {
		return nil, err
	}
	command.budget = blockingCommandBudget(args)
	if keys, requiresSameSlot := sameSlotCommandKeys(args); requiresSameSlot {
		if _, sameSlot := singleShardKey(keys); !sameSlot {
			return nil, fmt.Errorf("%s requires keys in one hash slot", command.name)
		}
	}
	key, ok := routingKeyForBuiltCommand(args, command)
	if !ok {
		return nil, nil
	}
	route, err := e.routeInSnapshot(snapshot, key)
	if err != nil {
		return nil, &topologyRouteLookupError{err: err}
	}
	return &topologyRouteData{command: command, route: route, snapshot: snapshot}, nil
}

func (e *TopologyNativeExecutor) planScatterRoutes(
	ctx context.Context,
	keys []any,
) (topologyRoutingSnapshot, map[topologyRouteIdentity]*scatterRouteGroup, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for position, key := range keys {
		if !isRouteKey(key) {
			return topologyRoutingSnapshot{}, nil, fmt.Errorf("scatter key %d has unsupported type %T", position, key)
		}
	}
	refreshed := false
	for range maxTopologyPlanningAttempts {
		snapshot, err := e.captureRoutingTopology()
		if err != nil {
			return topologyRoutingSnapshot{}, nil, err
		}
		groups := make(map[topologyRouteIdentity]*scatterRouteGroup)
		var planErr error
		for position, key := range keys {
			route, err := e.routeInSnapshot(snapshot, key)
			if err != nil {
				planErr = err
				break
			}
			identity := routeIdentity(route)
			group := groups[identity]
			if group == nil {
				group = &scatterRouteGroup{route: route}
				groups[identity] = group
			}
			group.keys = append(group.keys, key)
			group.positions = append(group.positions, position)
		}
		if !e.routingSnapshotCurrent(snapshot) {
			continue
		}
		if planErr == nil {
			return snapshot, groups, nil
		}
		if refreshed {
			return topologyRoutingSnapshot{}, nil, planErr
		}
		if err := e.refreshTopologyAtVersion(ctx, snapshot.version); err != nil {
			return topologyRoutingSnapshot{}, nil, err
		}
		refreshed = true
	}
	return topologyRoutingSnapshot{}, nil, fmt.Errorf("%w after %d scatter planning attempts", errTopologyStaleRoute, maxTopologyPlanningAttempts)
}
