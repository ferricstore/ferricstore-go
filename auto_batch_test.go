package ferricstore

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

type fakePipelineExecutor struct {
	mu       sync.Mutex
	batches  [][][]any
	err      error
	prefix   string
	blocking chan struct{}
}

func (f *fakePipelineExecutor) Do(ctx context.Context, args ...any) (any, error) {
	return nil, errors.New("unexpected direct Do call")
}

func (f *fakePipelineExecutor) Pipeline(ctx context.Context, commands [][]any) ([]any, error) {
	if f.blocking != nil {
		<-f.blocking
	}
	f.mu.Lock()
	f.batches = append(f.batches, cloneCommands(commands))
	f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	results := make([]any, len(commands))
	for i := range commands {
		results[i] = []byte(f.prefix + asString(commands[i][0]))
	}
	return results, nil
}

func (f *fakePipelineExecutor) batchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.batches)
}

func (f *fakePipelineExecutor) firstBatch() [][]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.batches) == 0 {
		return nil
	}
	return cloneCommands(f.batches[0])
}

func TestAutoBatchExecutorFlushesAtMaxSize(t *testing.T) {
	pipeline := &fakePipelineExecutor{prefix: "ok:"}
	base := NewClientWithExecutor(pipeline)
	exec := NewAutoBatchExecutor(base, AutoBatchOptions{MaxSize: 2, FlushInterval: time.Hour})
	defer func() { _ = exec.Close() }()

	type result struct {
		value any
		err   error
	}
	results := make(chan result, 2)
	go func() {
		value, err := exec.Do(context.Background(), "PING")
		results <- result{value: value, err: err}
	}()
	go func() {
		value, err := exec.Do(context.Background(), "GET", "k")
		results <- result{value: value, err: err}
	}()

	for i := 0; i < 2; i++ {
		got := <-results
		if got.err != nil {
			t.Fatal(got.err)
		}
	}
	if pipeline.batchCount() != 1 {
		t.Fatalf("expected one pipeline flush, got %d", pipeline.batchCount())
	}
	want := [][]any{{"PING"}, {"GET", "k"}}
	if !sameCommandSet(pipeline.firstBatch(), want) {
		t.Fatalf("unexpected batch: %#v", pipeline.firstBatch())
	}
}

func TestAutoBatchExecutorFlushesByInterval(t *testing.T) {
	pipeline := &fakePipelineExecutor{prefix: "ok:"}
	base := NewClientWithExecutor(pipeline)
	exec := NewAutoBatchExecutor(base, AutoBatchOptions{MaxSize: 10, FlushInterval: time.Millisecond})
	defer func() { _ = exec.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	value, err := exec.Do(ctx, "PING")

	if err != nil {
		t.Fatal(err)
	}
	if asString(value) != "ok:PING" {
		t.Fatalf("unexpected value: %#v", value)
	}
	if pipeline.batchCount() != 1 {
		t.Fatalf("expected one pipeline flush, got %d", pipeline.batchCount())
	}
}

func TestAutoBatchExecutorPropagatesPipelineError(t *testing.T) {
	wantErr := errors.New("pipeline failed")
	pipeline := &fakePipelineExecutor{err: wantErr}
	base := NewClientWithExecutor(pipeline)
	exec := NewAutoBatchExecutor(base, AutoBatchOptions{MaxSize: 1, FlushInterval: time.Hour})
	defer func() { _ = exec.Close() }()

	_, err := exec.Do(context.Background(), "PING")

	if !errors.Is(err, wantErr) {
		t.Fatalf("expected pipeline error, got %v", err)
	}
}

func TestAutoBatchExecutorCloseRejectsNewCommands(t *testing.T) {
	pipeline := &fakePipelineExecutor{}
	base := NewClientWithExecutor(pipeline)
	exec := NewAutoBatchExecutor(base, AutoBatchOptions{MaxSize: 1, FlushInterval: time.Hour})
	if err := exec.Close(); err != nil {
		t.Fatal(err)
	}

	_, err := exec.Do(context.Background(), "PING")

	if !errors.Is(err, errAutoBatchClosed) {
		t.Fatalf("expected closed error, got %v", err)
	}
}

func cloneCommands(commands [][]any) [][]any {
	out := make([][]any, 0, len(commands))
	for _, command := range commands {
		out = append(out, append([]any(nil), command...))
	}
	return out
}

func sameCommandSet(got, want [][]any) bool {
	if len(got) != len(want) {
		return false
	}
	used := make([]bool, len(want))
	for _, command := range got {
		found := false
		for i, candidate := range want {
			if !used[i] && reflect.DeepEqual(command, candidate) {
				used[i] = true
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
