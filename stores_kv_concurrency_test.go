package ferricstore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type blockingKVExecutor struct {
	entered chan struct{}
	release chan struct{}
}

func (e *blockingKVExecutor) Do(ctx context.Context, _ ...any) (any, error) {
	select {
	case e.entered <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-e.release:
		return []byte("value"), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestKeyValueDispatchAllowsConcurrentRequests(t *testing.T) {
	exec := &blockingKVExecutor{entered: make(chan struct{}, 2), release: make(chan struct{})}
	store := NewClientWithExecutor(exec).KV()
	results := make(chan error, 2)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(exec.release) }) }
	defer release()

	go func() {
		_, err := store.Get(context.Background(), "first")
		results <- err
	}()
	select {
	case <-exec.entered:
	case <-time.After(time.Second):
		t.Fatal("first KV request did not reach executor")
	}

	go func() {
		_, err := store.Get(context.Background(), "second")
		results <- err
	}()
	select {
	case <-exec.entered:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("second KV request was serialized behind the first")
	}

	release()
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}

func TestKeyValueDispatchWaitHonorsContext(t *testing.T) {
	exec := &blockingKVExecutor{entered: make(chan struct{}, 2), release: make(chan struct{})}
	store := NewClientWithExecutor(exec).KV()
	firstDone := make(chan error, 1)
	go func() {
		_, err := store.Get(context.Background(), "first")
		firstDone <- err
	}()
	select {
	case <-exec.entered:
	case <-time.After(time.Second):
		t.Fatal("first KV request did not reach executor")
	}
	defer func() {
		close(exec.release)
		if err := <-firstDone; err != nil {
			t.Error(err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	secondDone := make(chan error, 1)
	go func() {
		_, err := store.Get(ctx, "second")
		secondDone <- err
	}()

	select {
	case err := <-secondDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("second KV request error = %v; want deadline exceeded", err)
		}
	case <-time.After(150 * time.Millisecond):
		t.Fatal("canceled KV request remained blocked waiting for dispatch")
	}
}

func TestKeyValueAutoBatchCoalescesConcurrentCalls(t *testing.T) {
	pipeline := &fakePipelineExecutor{prefix: "value:"}
	base := NewClientWithExecutor(pipeline)
	exec := NewAutoBatchExecutor(base, AutoBatchOptions{MaxSize: 2, FlushInterval: time.Hour})
	defer func() { _ = exec.Close() }()
	store := NewClientWithExecutor(exec).KV()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	results := make(chan error, 2)
	for _, key := range []string{"first", "second"} {
		go func(key string) {
			_, err := store.Get(ctx, key)
			results <- err
		}(key)
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("typed KV autobatch request failed: %v", err)
		}
	}
	if got := pipeline.batchCount(); got != 1 {
		t.Fatalf("typed KV autobatch flushes = %d; want one", got)
	}
	if got := len(pipeline.firstBatch()); got != 2 {
		t.Fatalf("typed KV autobatch size = %d; want two", got)
	}
}
