package ferricstore

import "context"

var _ flowQueryExecutor = (*TopologyNativeExecutor)(nil)

func (e *TopologyNativeExecutor) executePreparedFlowQuery(ctx context.Context, query preparedFlowQuery) (any, error) {
	adapter, err := e.controlAdapter(ctx)
	if err != nil {
		return nil, err
	}
	return adapter.executePreparedFlowQuery(ctx, query)
}
