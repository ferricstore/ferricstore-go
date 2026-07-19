package ferricstore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type autoBatchBlockingIsolationExecutor struct {
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
}

func (e *autoBatchBlockingIsolationExecutor) releaseBlocking() {
	e.releaseOnce.Do(func() { close(e.release) })
}

func (e *autoBatchBlockingIsolationExecutor) Do(ctx context.Context, args ...any) (any, error) {
	if commandName(canonicalCommandArgs(args)) == "GET" {
		return []byte("value"), nil
	}
	if !autoBatchTestBlockingCommand(args) {
		return nil, errors.New("unexpected direct non-blocking command")
	}
	e.once.Do(func() { close(e.entered) })
	select {
	case <-e.release:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (e *autoBatchBlockingIsolationExecutor) Pipeline(
	ctx context.Context,
	commands [][]any,
) ([]any, error) {
	blocking := false
	for _, command := range commands {
		if autoBatchTestBlockingCommand(command) {
			blocking = true
			break
		}
	}
	if blocking {
		e.once.Do(func() { close(e.entered) })
		select {
		case <-e.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	results := make([]any, len(commands))
	for index, command := range commands {
		if commandName(canonicalCommandArgs(command)) == "GET" {
			results[index] = []byte("value")
		}
	}
	return results, nil
}

func autoBatchTestBlockingCommand(args []any) bool {
	args = canonicalCommandArgs(args)
	if len(args) == 0 {
		return false
	}
	switch commandName(args) {
	case "BLPOP", "XREAD":
		return true
	default:
		return false
	}
}

func TestAutoBatchBlockingCommandDoesNotBlockUnrelatedBatch(t *testing.T) {
	inner := &autoBatchBlockingIsolationExecutor{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	exec := NewAutoBatchExecutor(NewClientWithExecutor(inner), AutoBatchOptions{
		MaxSize: 2, FlushInterval: time.Millisecond, FlushTimeout: time.Second,
	})
	t.Cleanup(func() {
		inner.releaseBlocking()
		_ = exec.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	blockingDone := make(chan error, 1)
	go func() {
		_, err := exec.Do(ctx, "BLPOP", "queue", 0)
		blockingDone <- err
	}()
	select {
	case <-inner.entered:
	case <-ctx.Done():
		inner.releaseBlocking()
		t.Fatal("blocking command did not reach the executor")
	}

	getDone := make(chan error, 1)
	go func() {
		value, err := exec.Do(ctx, "GET", "key")
		if err == nil && string(value.([]byte)) != "value" {
			err = errors.New("GET returned the wrong value")
		}
		getDone <- err
	}()

	unrelatedWasBlocked := false
	select {
	case err := <-getDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(100 * time.Millisecond):
		unrelatedWasBlocked = true
	}
	inner.releaseBlocking()
	if unrelatedWasBlocked {
		if err := <-getDone; err != nil {
			t.Fatal(err)
		}
	}
	if err := <-blockingDone; err != nil {
		t.Fatal(err)
	}
	if err := exec.Close(); err != nil {
		t.Fatal(err)
	}
	if unrelatedWasBlocked {
		t.Fatal("GET was held behind an unrelated blocking command")
	}
}

func TestAutoBatchFlushTimeoutDoesNotCapWrappedBlockingCommand(t *testing.T) {
	inner := &autoBatchBlockingIsolationExecutor{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	exec := NewAutoBatchExecutor(NewClientWithExecutor(inner), AutoBatchOptions{
		MaxSize: 1, FlushInterval: time.Hour, FlushTimeout: 20 * time.Millisecond,
	})
	t.Cleanup(func() {
		inner.releaseBlocking()
		_ = exec.Close()
	})
	done := make(chan error, 1)
	go func() {
		_, err := exec.Do(
			context.Background(),
			"COMMAND_EXEC", "BLPOP", "queue", 0,
		)
		done <- err
	}()
	select {
	case <-inner.entered:
	case <-time.After(time.Second):
		inner.releaseBlocking()
		t.Fatal("wrapped blocking command did not reach the executor")
	}

	flushTimeoutCappedCommand := false
	select {
	case <-done:
		flushTimeoutCappedCommand = true
	case <-time.After(75 * time.Millisecond):
	}
	inner.releaseBlocking()
	if !flushTimeoutCappedCommand {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if err := exec.Close(); err != nil {
		t.Fatal(err)
	}
	if flushTimeoutCappedCommand {
		t.Fatal("autobatch flush timeout capped an indefinite blocking command")
	}
}

func TestAutoBatchExplicitBlockingPipelineDoesNotCaptureUnrelatedCommands(t *testing.T) {
	inner := &autoBatchBlockingIsolationExecutor{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	exec := NewAutoBatchExecutor(NewClientWithExecutor(inner), AutoBatchOptions{
		MaxSize: 2, FlushInterval: time.Millisecond, FlushTimeout: 20 * time.Millisecond,
	})
	t.Cleanup(func() {
		inner.releaseBlocking()
		_ = exec.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	blockingDone := make(chan error, 1)
	go func() {
		_, err := exec.Pipeline(ctx, [][]any{{"XREAD", "BLOCK", 0, "STREAMS", "events", "$"}})
		blockingDone <- err
	}()
	select {
	case <-inner.entered:
	case <-ctx.Done():
		inner.releaseBlocking()
		t.Fatal("explicit blocking pipeline did not reach the executor")
	}

	getDone := make(chan error, 1)
	go func() {
		value, err := exec.Do(ctx, "GET", "key")
		if err == nil && string(value.([]byte)) != "value" {
			err = errors.New("GET returned the wrong value")
		}
		getDone <- err
	}()

	select {
	case err := <-getDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(100 * time.Millisecond):
		inner.releaseBlocking()
		<-blockingDone
		t.Fatal("GET was captured behind an explicit blocking pipeline")
	}

	select {
	case <-blockingDone:
		inner.releaseBlocking()
		t.Fatal("autobatch flush timeout capped an explicit blocking pipeline")
	case <-time.After(75 * time.Millisecond):
	}
	inner.releaseBlocking()
	if err := <-blockingDone; err != nil {
		t.Fatal(err)
	}
	if err := exec.Close(); err != nil {
		t.Fatal(err)
	}
}
