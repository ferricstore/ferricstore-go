package ferricstore

import (
	"context"
	"fmt"
)

func (e *TopologyNativeExecutor) doForKey(ctx context.Context, key any, args ...any) (any, error) {
	if err := e.assertOpen(); err != nil {
		return nil, err
	}
	if !isRouteKey(key) {
		return nil, fmt.Errorf("unsupported explicit routing key type %T", key)
	}
	if name, mutates := connectionStateMutationCommand(args); mutates {
		return nil, fmt.Errorf("%s is connection-local and cannot be applied safely to a topology executor", name)
	}
	command, err := buildNativeCommand(args)
	if err != nil {
		return nil, err
	}
	route, snapshot, err := e.routeWithRefreshSnapshot(ctx, key)
	if err != nil {
		return nil, err
	}
	return e.doNativeCommandWithSafeReroute(ctx, key, command, route, snapshot)
}
