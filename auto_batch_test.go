package ferricstore

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
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

type itemErrorPipelineExecutor struct {
	wantErr error
}

func (e *itemErrorPipelineExecutor) Do(context.Context, ...any) (any, error) {
	return nil, errors.New("unexpected direct Do call")
}

func (e *itemErrorPipelineExecutor) Pipeline(_ context.Context, commands [][]any) ([]any, error) {
	results := make([]any, len(commands))
	for i, command := range commands {
		if asString(command[0]) == "FAIL" {
			results[i] = e.wantErr
		} else {
			results[i] = []byte("OK")
		}
	}
	return results, nil
}

type gatedPipelineExecutor struct {
	mu      sync.Mutex
	entered chan struct{}
	release chan struct{}
	first   bool
}

type ownershipPipelineExecutor struct {
	entered  chan struct{}
	release  chan struct{}
	captured chan []byte
}

func (e *ownershipPipelineExecutor) Do(context.Context, ...any) (any, error) {
	return nil, errors.New("unexpected direct Do call")
}

func (e *ownershipPipelineExecutor) Pipeline(_ context.Context, commands [][]any) ([]any, error) {
	close(e.entered)
	<-e.release
	value := commands[0][2].([]byte)
	e.captured <- append([]byte(nil), value...)
	return []any{[]byte("OK")}, nil
}

type hungPipelineExecutor struct {
	calls   atomic.Int64
	entered chan struct{}
	release chan struct{}
}

func (e *hungPipelineExecutor) Do(context.Context, ...any) (any, error) {
	return nil, errors.New("unexpected direct Do call")
}

func (e *hungPipelineExecutor) Pipeline(_ context.Context, commands [][]any) ([]any, error) {
	e.calls.Add(1)
	select {
	case e.entered <- struct{}{}:
	default:
	}
	<-e.release
	results := make([]any, len(commands))
	for i := range results {
		results[i] = []byte("OK")
	}
	return results, nil
}

func (e *gatedPipelineExecutor) Do(context.Context, ...any) (any, error) {
	return nil, errors.New("unexpected direct Do call")
}

func (e *gatedPipelineExecutor) Pipeline(_ context.Context, commands [][]any) ([]any, error) {
	e.mu.Lock()
	first := !e.first
	if first {
		e.first = true
		close(e.entered)
	}
	e.mu.Unlock()
	if first {
		<-e.release
	}
	results := make([]any, len(commands))
	for i := range results {
		results[i] = []byte("OK")
	}
	return results, nil
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

func TestAutoBatchExecutorPreservesPerCommandErrors(t *testing.T) {
	wantErr := errors.New("only this command failed")
	pipeline := &itemErrorPipelineExecutor{wantErr: wantErr}
	base := NewClientWithExecutor(pipeline)
	exec := NewAutoBatchExecutor(base, AutoBatchOptions{MaxSize: 2, FlushInterval: time.Hour})
	defer func() { _ = exec.Close() }()

	type result struct {
		name  string
		value any
		err   error
	}
	results := make(chan result, 2)
	for _, name := range []string{"OK", "FAIL"} {
		go func(name string) {
			value, err := exec.Do(context.Background(), name)
			results <- result{name: name, value: value, err: err}
		}(name)
	}

	for range 2 {
		got := <-results
		if got.name == "FAIL" {
			if !errors.Is(got.err, wantErr) {
				t.Fatalf("failed command got value=%#v err=%v", got.value, got.err)
			}
			continue
		}
		if got.err != nil || !isOK(got.value) {
			t.Fatalf("successful command was poisoned by sibling error: value=%#v err=%v", got.value, got.err)
		}
	}
}

func TestAutoBatchExecutorCloseCompletesEveryAcceptedRequest(t *testing.T) {
	pipeline := &gatedPipelineExecutor{entered: make(chan struct{}), release: make(chan struct{})}
	base := NewClientWithExecutor(pipeline)
	exec := NewAutoBatchExecutor(base, AutoBatchOptions{MaxSize: 1, FlushInterval: time.Hour, QueueSize: 128})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const queued = 32
	results := make(chan error, queued+1)
	go func() {
		_, err := exec.Do(ctx, "FIRST")
		results <- err
	}()
	<-pipeline.entered
	for i := 0; i < queued; i++ {
		go func(i int) {
			_, err := exec.Do(ctx, "SET", i, i)
			results <- err
		}(i)
	}
	deadline := time.Now().Add(time.Second)
	for len(exec.requests) != queued && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(exec.requests) != queued {
		cancel()
		t.Fatalf("expected %d accepted queued requests, got %d", queued, len(exec.requests))
	}

	closeErr := make(chan error, 1)
	go func() { closeErr <- exec.Close() }()
	deadline = time.Now().Add(time.Second)
	for !exec.isClosed.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(pipeline.release)

	for i := 0; i < queued+1; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("accepted request failed during close: %v", err)
			}
		case <-time.After(time.Second):
			cancel()
			t.Fatal("accepted autobatch request was stranded during close")
		}
	}
	if err := <-closeErr; err != nil {
		t.Fatal(err)
	}
}

func TestAutoBatchCloseBoundsExecutorThatIgnoresContext(t *testing.T) {
	blocked := make(chan struct{})
	pipeline := &gatedPipelineExecutor{entered: make(chan struct{}), release: blocked}
	base := NewClientWithExecutor(pipeline)
	exec := NewAutoBatchExecutor(base, AutoBatchOptions{
		MaxSize:         1,
		FlushInterval:   time.Hour,
		FlushTimeout:    time.Hour,
		ShutdownTimeout: 50 * time.Millisecond,
	})
	requestDone := make(chan error, 1)
	go func() {
		_, err := exec.Do(context.Background(), "PING")
		requestDone <- err
	}()
	<-pipeline.entered
	started := time.Now()
	err := exec.Close()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("Close remained blocked for %v", elapsed)
	}
	select {
	case err := <-requestDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("accepted request error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("accepted request remained blocked after bounded close")
	}
	close(blocked)
}

func TestAutoBatchTimeoutDoesNotSpawnAnotherHungPipeline(t *testing.T) {
	pipeline := &hungPipelineExecutor{entered: make(chan struct{}, 2), release: make(chan struct{})}
	base := NewClientWithExecutor(pipeline)
	exec := NewAutoBatchExecutor(base, AutoBatchOptions{
		MaxSize: 1, FlushInterval: time.Hour, FlushTimeout: 20 * time.Millisecond,
		ShutdownTimeout: 20 * time.Millisecond,
	})
	defer close(pipeline.release)

	if _, err := exec.Do(context.Background(), "FIRST"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first timed-out flush error = %v", err)
	}
	if _, err := exec.Do(context.Background(), "SECOND"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second timed-out flush error = %v", err)
	}
	if got := pipeline.calls.Load(); got != 1 {
		t.Fatalf("hung downstream pipeline calls = %d, want one bounded worker", got)
	}
	if err := exec.Close(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close with a still-hung downstream pipeline = %v, want deadline", err)
	}
}

func TestAutoBatchCloseUnblocksQueueAdmission(t *testing.T) {
	pipeline := &gatedPipelineExecutor{entered: make(chan struct{}), release: make(chan struct{})}
	base := NewClientWithExecutor(pipeline)
	exec := NewAutoBatchExecutor(base, AutoBatchOptions{
		MaxSize: 1, QueueSize: 1, FlushInterval: time.Hour, FlushTimeout: time.Hour,
		ShutdownTimeout: 30 * time.Millisecond,
	})
	defer close(pipeline.release)

	go func() { _, _ = exec.Do(context.Background(), "FIRST") }()
	<-pipeline.entered
	go func() { _, _ = exec.Do(context.Background(), "QUEUED") }()
	deadline := time.Now().Add(time.Second)
	for len(exec.requests) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(exec.requests) != 1 {
		t.Fatal("failed to fill autobatch admission queue")
	}
	thirdDone := make(chan error, 1)
	go func() {
		_, err := exec.Do(context.Background(), "BLOCKED")
		thirdDone <- err
	}()

	closeDone := make(chan error, 1)
	go func() { closeDone <- exec.Close() }()
	select {
	case err := <-closeDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("close error = %v, want bounded shutdown deadline", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Close deadlocked behind a blocked queue submitter")
	}
	select {
	case err := <-thirdDone:
		if !errors.Is(err, errAutoBatchClosed) {
			t.Fatalf("blocked submitter error = %v, want closed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked queue submitter was not released by Close")
	}
}

func TestAutoBatchExecutorSkipsCanceledRequestsBeforeFlush(t *testing.T) {
	pipeline := &fakePipelineExecutor{}
	base := NewClientWithExecutor(pipeline)
	exec := NewAutoBatchExecutor(base, AutoBatchOptions{MaxSize: 10, FlushInterval: time.Hour})

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := exec.Do(ctx, "SET", "cancelled", "1")
		errc <- err
	}()

	for i := 0; i < 100 && len(exec.requests) == 0; i++ {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	if err := exec.Close(); err != nil {
		t.Fatal(err)
	}
	if pipeline.batchCount() != 0 {
		t.Fatalf("canceled request should not be flushed, got %d batches", pipeline.batchCount())
	}
}

func TestAutoBatchExecutorOwnsMutableArgumentsAfterSubmit(t *testing.T) {
	pipeline := &ownershipPipelineExecutor{
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
		captured: make(chan []byte, 1),
	}
	exec := NewAutoBatchExecutor(NewClientWithExecutor(pipeline), AutoBatchOptions{
		MaxSize:       1,
		FlushInterval: time.Hour,
	})
	defer func() { _ = exec.Close() }()

	value := []byte("before")
	done := make(chan error, 1)
	go func() {
		_, err := exec.Do(context.Background(), "SET", "key", value)
		done <- err
	}()
	<-pipeline.entered
	copy(value, "CHANGED")
	close(pipeline.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := string(<-pipeline.captured); got != "before" {
		t.Fatalf("pipeline observed caller mutation %q; want immutable snapshot %q", got, "before")
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
