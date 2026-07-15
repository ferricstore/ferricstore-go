package ferricstore

import (
	"context"
	"errors"
	"testing"
)

type wrongCardinalityDetailedExecutor struct {
	resultCount int
}

func (*wrongCardinalityDetailedExecutor) Do(context.Context, ...any) (any, error) {
	return nil, errors.New("unexpected direct command")
}

func (e *wrongCardinalityDetailedExecutor) pipelineDetailed(context.Context, [][]any) ([]pipelineItemResult, error) {
	return make([]pipelineItemResult, e.resultCount), nil
}

func TestClientRejectsDetailedPipelineCardinalityMismatch(t *testing.T) {
	commands := [][]any{{"GET", "one"}, {"GET", "two"}}
	for _, resultCount := range []int{1, 3} {
		t.Run(string(rune('0'+resultCount)), func(t *testing.T) {
			client := NewClientWithExecutor(&wrongCardinalityDetailedExecutor{resultCount: resultCount})
			results, err := client.Pipeline(context.Background(), commands)
			if err == nil {
				t.Fatalf("accepted %d detailed results for %d commands: %#v", resultCount, len(commands), results)
			}
			if results != nil {
				t.Fatalf("cardinality error returned partial results %#v", results)
			}
		})
	}
}
