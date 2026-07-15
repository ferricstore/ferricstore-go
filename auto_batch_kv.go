package ferricstore

import (
	"context"
	"errors"
	"fmt"
)

func (e *AutoBatchExecutor) keyValueMGet(ctx context.Context, keys []string) (any, error) {
	return e.submitTypedKVAndWait(ctx, autoBatchTypedKVMGet, keys, nil, false)
}

func (e *AutoBatchExecutor) keyValueMSet(ctx context.Context, keys []string, values []any) (any, error) {
	if len(keys) != len(values) {
		return nil, fmt.Errorf("MSET received %d keys and %d values", len(keys), len(values))
	}
	return e.submitTypedKVAndWait(ctx, autoBatchTypedKVMSet, keys, values, true)
}

func (e *AutoBatchExecutor) keyValueMSetNX(ctx context.Context, keys []string, values []any) (any, error) {
	if len(keys) != len(values) {
		return nil, fmt.Errorf("MSETNX received %d keys and %d values", len(keys), len(values))
	}
	return e.submitTypedKVAndWait(ctx, autoBatchTypedKVMSetNX, keys, values, false)
}

func (e *AutoBatchExecutor) keyValueDel(ctx context.Context, keys []string) (any, error) {
	return e.submitTypedKVAndWait(ctx, autoBatchTypedKVDel, keys, nil, false)
}

func (e *AutoBatchExecutor) keyValueExists(ctx context.Context, keys []string) (any, error) {
	return e.submitTypedKVAndWait(ctx, autoBatchTypedKVExists, keys, nil, false)
}

func (e *AutoBatchExecutor) submitTypedKVAndWait(
	ctx context.Context,
	kind autoBatchTypedKVKind,
	keys []string,
	values []any,
	allowQueued bool,
) (any, error) {
	if e == nil || e.client == nil {
		return nil, errors.New("ferricstore autobatch executor requires a client")
	}
	if e.isClosed.Load() {
		return nil, errAutoBatchClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := e.reserveQueueSlot(ctx); err != nil {
		return nil, err
	}
	preparedValues, err := materializeDeferredCodecValuesForExecutor(
		values,
		e.client.exec,
		&e.codecMu,
	)
	if err != nil {
		e.queueSlots <- struct{}{}
		return nil, fmt.Errorf("encode autobatch %s values: %w", kind.commandName(), err)
	}
	ownedValues, err := snapshotDeferredCodecInputs(preparedValues)
	if err != nil {
		e.queueSlots <- struct{}{}
		return nil, fmt.Errorf("snapshot autobatch %s values: %w", kind.commandName(), err)
	}
	request := autoBatchRequest{
		ctx:    ctx,
		result: make(chan autoBatchResult, 1),
		control: &autoBatchRequestControl{
			typedKind: kind, typedKeys: append([]string(nil), keys...), typedValues: ownedValues,
			allowQueued: allowQueued,
		},
	}
	if err := e.sendReserved(request); err != nil {
		return nil, err
	}
	select {
	case result := <-request.result:
		if result.err != nil {
			return nil, result.err
		}
		value, queued := unwrapTypedCommandState(result.value)
		return typedCommandStateValue{value: value, queued: queued}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (kind autoBatchTypedKVKind) commandName() string {
	switch kind {
	case autoBatchTypedKVMGet:
		return "MGET"
	case autoBatchTypedKVMSet:
		return "MSET"
	case autoBatchTypedKVMSetNX:
		return "MSETNX"
	case autoBatchTypedKVDel:
		return "DEL"
	case autoBatchTypedKVExists:
		return "EXISTS"
	default:
		return "typed KV"
	}
}

func keyValuePairCommandArgs(command string, keys []string, values []any) []any {
	args := make([]any, 1, 1+2*len(keys))
	args[0] = command
	for index, key := range keys {
		args = append(args, key, values[index])
	}
	return args
}
