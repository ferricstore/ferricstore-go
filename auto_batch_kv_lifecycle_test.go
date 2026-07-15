package ferricstore

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type coalescingTypedKVExecutor struct {
	directCalls   atomic.Int64
	pipelineCalls atomic.Int64
}

func (*coalescingTypedKVExecutor) Do(context.Context, ...any) (any, error) {
	return nil, errors.New("unexpected direct Do call")
}

func (e *coalescingTypedKVExecutor) Pipeline(_ context.Context, commands [][]any) ([]any, error) {
	e.pipelineCalls.Add(1)
	results := make([]any, len(commands))
	for index := range results {
		results[index] = []any{[]byte("value")}
	}
	return results, nil
}

func (e *coalescingTypedKVExecutor) keyValueMGet(context.Context, []string) (any, error) {
	e.directCalls.Add(1)
	return []any{[]byte("value")}, nil
}

func (*coalescingTypedKVExecutor) keyValueMSet(context.Context, []string, []any) (any, error) {
	return []byte("OK"), nil
}

func TestAutoBatchCoalescesConcurrentTypedBulkKVCalls(t *testing.T) {
	inner := &coalescingTypedKVExecutor{}
	exec := NewAutoBatchExecutor(
		NewClientWithExecutor(inner),
		AutoBatchOptions{MaxSize: 2, FlushInterval: time.Hour},
	)
	defer func() { _ = exec.Close() }()
	store := NewClientWithExecutor(exec).KV()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	results := make(chan error, 2)
	for _, key := range []string{"first", "second"} {
		go func(key string) {
			values, err := store.MGet(ctx, key)
			if err == nil && (len(values) != 1 || asString(values[0]) != "value") {
				err = errors.New("unexpected MGET response")
			}
			results <- err
		}(key)
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if got := inner.pipelineCalls.Load(); got != 1 {
		t.Fatalf("typed KV pipeline calls = %d; want 1", got)
	}
	if got := inner.directCalls.Load(); got != 0 {
		t.Fatalf("typed KV direct calls = %d; want 0 for a coalesced batch", got)
	}
}

type blockingTypedKVExecutor struct {
	entered chan struct{}
	release chan struct{}
}

func (*blockingTypedKVExecutor) Do(context.Context, ...any) (any, error) {
	return nil, errors.New("unexpected direct Do call")
}

func (e *blockingTypedKVExecutor) keyValueMGet(context.Context, []string) (any, error) {
	close(e.entered)
	<-e.release
	return []any{[]byte("value")}, nil
}

func (*blockingTypedKVExecutor) keyValueMSet(context.Context, []string, []any) (any, error) {
	return []byte("OK"), nil
}

func TestAutoBatchCloseWaitsForAcceptedTypedKVRequest(t *testing.T) {
	inner := &blockingTypedKVExecutor{entered: make(chan struct{}), release: make(chan struct{})}
	exec := NewAutoBatchExecutor(
		NewClientWithExecutor(inner),
		AutoBatchOptions{MaxSize: 1, FlushInterval: time.Hour, ShutdownTimeout: time.Second},
	)
	store := NewClientWithExecutor(exec).KV()
	requestDone := make(chan error, 1)
	go func() {
		_, err := store.MGet(context.Background(), "key")
		requestDone <- err
	}()
	select {
	case <-inner.entered:
	case <-time.After(time.Second):
		t.Fatal("typed KV request did not reach the inner executor")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- exec.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before the accepted typed KV request completed: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(inner.release)
	if err := <-requestDone; err != nil {
		t.Fatal(err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
}

func TestAutoBatchClosedTypedKVRejectsBeforeCodecWork(t *testing.T) {
	codec := &countingKVCodec{}
	queueSlots := make(chan struct{}, 1)
	queueSlots <- struct{}{}
	exec := &AutoBatchExecutor{
		client:     NewClientWithExecutor(&fakeExecutor{}),
		requests:   make(chan autoBatchRequest, 1),
		queueSlots: queueSlots,
		closed:     make(chan struct{}),
	}
	exec.isClosed.Store(true)

	_, err := exec.keyValueMSet(
		context.Background(),
		[]string{"key"},
		[]any{nativeDeferredCodecValue{codec: codec, value: "value"}},
	)
	if !errors.Is(err, errAutoBatchClosed) {
		t.Fatalf("closed typed MSET error = %v; want %v", err, errAutoBatchClosed)
	}
	if calls := codec.encodes.Load(); calls != 0 {
		t.Fatalf("closed typed MSET invoked codec %d times", calls)
	}
}

type blockingTypedKVKeyOwnerExecutor struct {
	entered chan struct{}
	release chan struct{}
	keys    chan []string
}

func (*blockingTypedKVKeyOwnerExecutor) Do(context.Context, ...any) (any, error) {
	return nil, errors.New("unexpected direct Do call")
}

func (e *blockingTypedKVKeyOwnerExecutor) keyValueDel(_ context.Context, keys []string) (any, error) {
	close(e.entered)
	<-e.release
	e.keys <- append([]string(nil), keys...)
	return int64(len(keys)), nil
}

func TestAutoBatchOwnsTypedKVKeysAfterAdmission(t *testing.T) {
	inner := &blockingTypedKVKeyOwnerExecutor{
		entered: make(chan struct{}), release: make(chan struct{}), keys: make(chan []string, 1),
	}
	exec := NewAutoBatchExecutor(
		NewClientWithExecutor(inner),
		AutoBatchOptions{MaxSize: 1, FlushInterval: time.Hour},
	)
	defer func() { _ = exec.Close() }()
	store := NewClientWithExecutor(exec).KV()
	ctx, cancel := context.WithCancel(context.Background())
	keys := []string{"original"}
	done := make(chan error, 1)
	go func() {
		_, err := store.Del(ctx, keys...)
		done <- err
	}()
	select {
	case <-inner.entered:
	case <-time.After(time.Second):
		t.Fatal("typed DEL did not reach the inner executor")
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("DEL error = %v; want context.Canceled", err)
	}
	keys[0] = "mutated"
	close(inner.release)
	if got := <-inner.keys; len(got) != 1 || got[0] != "original" {
		t.Fatalf("inner DEL keys = %#v; want the admitted snapshot", got)
	}
}

type blockingTypedKVValueOwnerExecutor struct {
	entered chan struct{}
	release chan struct{}
	value   chan string
}

func (*blockingTypedKVValueOwnerExecutor) Do(context.Context, ...any) (any, error) {
	return nil, errors.New("unexpected direct Do call")
}

func (*blockingTypedKVValueOwnerExecutor) supportsDeferredCodec(Codec) bool { return true }

func (e *blockingTypedKVValueOwnerExecutor) keyValueMGet(context.Context, []string) (any, error) {
	return nil, errors.New("unexpected MGET call")
}

func (e *blockingTypedKVValueOwnerExecutor) keyValueMSet(
	_ context.Context,
	_ []string,
	values []any,
) (any, error) {
	close(e.entered)
	<-e.release
	deferred, ok := values[0].(nativeDeferredCodecValue)
	if !ok {
		e.value <- "not deferred"
		return []byte("OK"), nil
	}
	value, ok := deferred.value.(map[string]any)
	if !ok {
		e.value <- "not a map"
		return []byte("OK"), nil
	}
	e.value <- asString(value["state"])
	return []byte("OK"), nil
}

func TestAutoBatchOwnsDeferredTypedKVValuesAfterAdmission(t *testing.T) {
	inner := &blockingTypedKVValueOwnerExecutor{
		entered: make(chan struct{}), release: make(chan struct{}), value: make(chan string, 1),
	}
	base := NewClientWithExecutor(inner, WithCodec(JSONCodec{}))
	exec := NewAutoBatchExecutor(base, AutoBatchOptions{MaxSize: 1, FlushInterval: time.Hour})
	defer func() { _ = exec.Close() }()
	store := NewClientWithExecutor(exec, WithCodec(JSONCodec{})).KV()
	ctx, cancel := context.WithCancel(context.Background())
	mutable := map[string]any{"state": "original"}
	done := make(chan error, 1)
	go func() { done <- store.MSet(ctx, map[string]any{"key": mutable}) }()
	select {
	case <-inner.entered:
	case <-time.After(time.Second):
		t.Fatal("typed MSET did not reach the inner executor")
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("MSET error = %v; want context.Canceled", err)
	}
	mutable["state"] = "mutated"
	close(inner.release)
	if got := <-inner.value; got != "original" {
		t.Fatalf("inner MSET value = %q; want admitted snapshot", got)
	}
}

func TestAutoBatchTypedMSetPreservesQueuedState(t *testing.T) {
	provider := &autoBatchSessionProvider{session: &autoBatchCommandSession{}}
	base := NewClientWithExecutor(provider)
	if err := base.Multi(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = base.Discard(context.Background()) }()

	exec := NewAutoBatchExecutor(base, AutoBatchOptions{MaxSize: 1, FlushInterval: time.Hour})
	defer func() { _ = exec.Close() }()
	if err := NewClientWithExecutor(exec).KV().MSet(
		context.Background(), map[string]any{"key": "value"},
	); err != nil {
		t.Fatalf("queued typed MSET returned an error: %v", err)
	}
}

func TestAutoBatchGenericTypedStatusPreservesQueuedState(t *testing.T) {
	provider := &autoBatchSessionProvider{session: &autoBatchCommandSession{}}
	base := NewClientWithExecutor(provider)
	if err := base.Multi(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = base.Discard(context.Background()) }()

	exec := NewAutoBatchExecutor(base, AutoBatchOptions{MaxSize: 1, FlushInterval: time.Hour})
	defer func() { _ = exec.Close() }()
	if err := NewClientWithExecutor(exec).KV().Set(context.Background(), "key", "value"); err != nil {
		t.Fatalf("queued typed SET returned an error: %v", err)
	}
}

func TestAutoBatchGenericTypedReplyIsNotQueued(t *testing.T) {
	provider := &autoBatchSessionProvider{session: &autoBatchCommandSession{}}
	base := NewClientWithExecutor(provider)
	if err := base.Multi(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = base.Discard(context.Background()) }()

	exec := NewAutoBatchExecutor(base, AutoBatchOptions{MaxSize: 1, FlushInterval: time.Hour})
	defer func() { _ = exec.Close() }()
	_, err := NewClientWithExecutor(exec).KV().Get(context.Background(), "key")
	if !errors.Is(err, ErrTypedReplyInTransaction) {
		t.Fatalf("typed GET error = %v; want ErrTypedReplyInTransaction", err)
	}
}
