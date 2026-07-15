package ferricstore

import "context"

func (e *NativeExecutor) keyValueMGet(ctx context.Context, keys []string) (any, error) {
	command := newNativeMGetCommand(keys)
	command.laneID = nativeAutoLaneID
	return e.doNativeCommand(ctx, command)
}

func (e *NativeExecutor) keyValueMSet(ctx context.Context, keys []string, values []any) (any, error) {
	command, err := newNativeMSetCommand(keys, values)
	if err != nil {
		return nil, err
	}
	command.laneID = nativeAutoLaneID
	return e.doNativeCommand(ctx, command)
}

func (e *NativeExecutor) keyValueMSetNX(ctx context.Context, keys []string, values []any) (any, error) {
	command, err := newNativeMSetNXCommand(keys, values)
	if err != nil {
		return nil, err
	}
	command.laneID = nativeAutoLaneID
	return e.doNativeCommand(ctx, command)
}

func (e *NativeExecutor) keyValueDel(ctx context.Context, keys []string) (any, error) {
	command := newNativeDelCommand(keys)
	command.laneID = nativeAutoLaneID
	return e.doNativeCommand(ctx, command)
}

func (e *NativeExecutor) keyValueExists(ctx context.Context, keys []string) (any, error) {
	command := newNativeExistsCommand(keys)
	command.laneID = nativeAutoLaneID
	return e.doNativeCommand(ctx, command)
}
