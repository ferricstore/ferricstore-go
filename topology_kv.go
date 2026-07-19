package ferricstore

import (
	"context"
	"errors"
	"fmt"
)

func (e *TopologyNativeExecutor) keyValueMGet(ctx context.Context, keys []string) (any, error) {
	if err := e.assertOpen(); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return []any{}, nil
	}
	route, snapshot, groups, err := e.planStringKeyRoutesSnapshot(ctx, keys, true)
	if err != nil {
		return nil, err
	}
	if groups == nil {
		return e.keyValueMGetOnRoute(ctx, keys, route, snapshot)
	}
	return e.scatterStringMGet(ctx, keys, groups, snapshot)
}

func (e *TopologyNativeExecutor) keyValueMSet(ctx context.Context, keys []string, values []any) (any, error) {
	if err := e.assertOpen(); err != nil {
		return nil, err
	}
	if len(keys) != len(values) {
		return nil, fmt.Errorf("MSET received %d keys and %d values", len(keys), len(values))
	}
	if len(keys) == 0 {
		return nil, errors.New("MSET requires at least one key/value pair")
	}
	slot := routeSlotForKey(keys[0])
	for _, key := range keys[1:] {
		if routeSlotForKey(key) != slot {
			return nil, errors.New("MSET requires keys in one hash slot")
		}
	}
	route, snapshot, err := e.routeWithRefreshSnapshot(ctx, keys[0])
	if err != nil {
		return nil, err
	}
	command, err := newNativeMSetCommand(keys, values)
	if err != nil {
		return nil, err
	}
	return e.doNativeCommandWithSafeReroute(ctx, keys[0], command, route, snapshot)
}

func (e *TopologyNativeExecutor) keyValueMSetNX(ctx context.Context, keys []string, values []any) (any, error) {
	if err := e.assertOpen(); err != nil {
		return nil, err
	}
	if len(keys) != len(values) {
		return nil, fmt.Errorf("MSETNX received %d keys and %d values", len(keys), len(values))
	}
	if len(keys) == 0 {
		return nil, errors.New("MSETNX requires at least one key/value pair")
	}
	slot := routeSlotForKey(keys[0])
	for _, key := range keys[1:] {
		if routeSlotForKey(key) != slot {
			return nil, errors.New("MSETNX requires keys in one hash slot")
		}
	}
	route, snapshot, err := e.routeWithRefreshSnapshot(ctx, keys[0])
	if err != nil {
		return nil, err
	}
	command, err := newNativeMSetNXCommand(keys, values)
	if err != nil {
		return nil, err
	}
	return e.doNativeCommandWithSafeReroute(ctx, keys[0], command, route, snapshot)
}

func (e *TopologyNativeExecutor) keyValueDel(ctx context.Context, keys []string) (any, error) {
	if err := e.assertOpen(); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return int64(0), nil
	}
	route, snapshot, groups, err := e.planStringKeyRoutesSnapshot(ctx, keys, false)
	if err != nil {
		return nil, err
	}
	if groups == nil {
		return e.keyValueCountOnRoute(ctx, "DEL", keys, route, snapshot)
	}
	return e.scatterStringCountCommand(ctx, "DEL", groups, snapshot)
}

func (e *TopologyNativeExecutor) keyValueExists(ctx context.Context, keys []string) (any, error) {
	if err := e.assertOpen(); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return int64(0), nil
	}
	route, snapshot, groups, err := e.planStringKeyRoutesSnapshot(ctx, keys, false)
	if err != nil {
		return nil, err
	}
	if groups == nil {
		return e.keyValueCountOnRoute(ctx, "EXISTS", keys, route, snapshot)
	}
	return e.scatterStringCountCommand(ctx, "EXISTS", groups, snapshot)
}
