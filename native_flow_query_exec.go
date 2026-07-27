package ferricstore

import (
	"context"
	"time"
)

var _ flowQueryExecutor = (*NativeExecutor)(nil)

func (e *NativeExecutor) executePreparedFlowQuery(ctx context.Context, query preparedFlowQuery) (any, error) {
	requestCtx, cancel, command := prepareNativeFlowQueryCommand(ctx, e.opts.Timeout, query)
	if cancel != nil {
		defer cancel()
	}
	command.laneID = nativeAutoLaneID
	value, err := e.doNativeCommand(requestCtx, command)
	if err != nil {
		return nil, err
	}
	if mapping, ok := value.(map[string]any); ok {
		return ownedNativeFlowQueryResponse(mapping), nil
	}
	return value, nil
}

func prepareNativeFlowQueryCommand(
	ctx context.Context,
	timeout time.Duration,
	query preparedFlowQuery,
) (context.Context, context.CancelFunc, nativeCommand) {
	requestCtx, cancel := nativeContextWithBudget(ctx, timeout, nativeRequestBudget{})
	return requestCtx, cancel, newNativeFlowQueryCommandForContext(requestCtx, query)
}
