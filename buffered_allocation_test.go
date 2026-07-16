package ferricstore

import (
	"context"
	"errors"
	"testing"
)

type bufferedStaticPipelineExecutor struct {
	results []pipelineItemResult
}

var bufferedRetainedSizeSink int

func TestBufferedCommonCommandCapacitySizingDoesNotAllocate(t *testing.T) {
	command := []any{"SET", "key", []byte("value")}
	allocs := testing.AllocsPerRun(1000, func() {
		size, ok := bufferedCommandRetainedSize(command, defaultBufferedMaxBytes)
		if !ok {
			t.Fatal("common buffered command did not fit the default byte capacity")
		}
		bufferedRetainedSizeSink = size
	})
	if allocs != 0 {
		t.Fatalf("buffered capacity sizing allocations = %.0f; want 0", allocs)
	}
}

func BenchmarkBufferedCommandCapacitySizing(b *testing.B) {
	command := []any{"SET", "key", []byte("value")}
	b.ReportAllocs()
	for b.Loop() {
		bufferedRetainedSizeSink, _ = bufferedCommandRetainedSize(command, defaultBufferedMaxBytes)
	}
}

func (*bufferedStaticPipelineExecutor) Do(context.Context, ...any) (any, error) {
	return nil, errors.New("unexpected direct Do call")
}

func (e *bufferedStaticPipelineExecutor) pipelineDetailed(context.Context, [][]any) ([]pipelineItemResult, error) {
	return e.results, nil
}

func TestBufferedSuccessfulFlushDoesNotCloneDetachedBatch(t *testing.T) {
	inner := &bufferedStaticPipelineExecutor{results: []pipelineItemResult{{value: []byte("OK")}}}
	exec := NewBufferedExecutor(NewClientWithExecutor(inner))
	commands := [][]any{{"SET", "key", make([]byte, 64*1024)}}

	allocs := testing.AllocsPerRun(100, func() {
		exec.mu.Lock()
		exec.commands = commands
		exec.mu.Unlock()
		if _, err := exec.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
	if allocs > 2 {
		t.Fatalf("successful buffered Flush allocations = %.1f; want <= 2", allocs)
	}
}
