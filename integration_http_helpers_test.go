//go:build integration

package ferricstore

import (
	"context"
	"fmt"
)

type integrationHTTPTrackingExecutor struct {
	inner *HTTPExecutor
}

func (e *integrationHTTPTrackingExecutor) Do(ctx context.Context, args ...any) (any, error) {
	value, err := e.inner.Do(ctx, args...)
	if err == nil {
		recordIntegrationCommand(args)
	}
	return value, err
}

func (e *integrationHTTPTrackingExecutor) Pipeline(ctx context.Context, commands [][]any) ([]any, error) {
	values, err := e.inner.Pipeline(ctx, commands)
	recordSuccessfulIntegrationPipelineCommands(commands, values)
	return values, err
}

func (e *integrationHTTPTrackingExecutor) pipelineDetailed(ctx context.Context, commands [][]any) ([]pipelineItemResult, error) {
	results, err := e.inner.pipelineDetailed(ctx, commands)
	if err == nil && len(results) == len(commands) {
		for index, result := range results {
			if result.err == nil {
				recordIntegrationCommand(commands[index])
			}
		}
	}
	return results, err
}

func newIntegrationHTTPTrackedClient(rawURL string, codec Codec) *Client {
	exec, err := NewHTTPExecutorFromURL(rawURL, integrationHTTPOptions()...)
	if err != nil {
		panic(fmt.Sprintf("create HTTP integration executor: %v", err))
	}
	client := NewClientWithExecutor(&integrationHTTPTrackingExecutor{inner: exec}, WithCodec(codec))
	client.closer = exec.Close
	return client
}

var (
	_ pipelineExecutor         = (*integrationHTTPTrackingExecutor)(nil)
	_ detailedPipelineExecutor = (*integrationHTTPTrackingExecutor)(nil)
)
