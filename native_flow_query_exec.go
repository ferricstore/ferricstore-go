package ferricstore

import "context"

var _ flowQueryExecutor = (*NativeExecutor)(nil)

func (e *NativeExecutor) executePreparedFlowQuery(ctx context.Context, query preparedFlowQuery) (any, error) {
	command := newNativeFlowQueryCommandForContext(ctx, query)
	command.laneID = nativeAutoLaneID
	value, err := e.doNativeCommand(ctx, command)
	if err != nil {
		return nil, err
	}
	if mapping, ok := value.(map[string]any); ok {
		return ownedNativeFlowQueryResponse(mapping), nil
	}
	return value, nil
}
