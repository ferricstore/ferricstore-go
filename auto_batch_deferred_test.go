package ferricstore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type autoBatchSignaledJSONValue struct {
	called chan struct{}
	once   *sync.Once
}

type autoBatchDeferredPipelineExecutor struct {
	entered chan struct{}
	release chan struct{}
}

type autoBatchSignaledCodec struct {
	called chan struct{}
	once   sync.Once
}

func (c *autoBatchSignaledCodec) Encode(value any) (any, error) {
	c.once.Do(func() { close(c.called) })
	return []byte(asString(value)), nil
}

func (*autoBatchSignaledCodec) Decode(value any) (any, error) { return value, nil }

func (*autoBatchDeferredPipelineExecutor) Do(context.Context, ...any) (any, error) {
	return nil, errors.New("unexpected direct Do call")
}

func (*autoBatchDeferredPipelineExecutor) supportsDeferredCodec(Codec) bool { return true }

func (e *autoBatchDeferredPipelineExecutor) Pipeline(
	_ context.Context,
	commands [][]any,
) ([]any, error) {
	close(e.entered)
	<-e.release
	deferred, ok := commands[0][2].(nativeDeferredCodecValue)
	if !ok {
		return nil, errors.New("autobatch materialized a deferred codec before downstream execution")
	}
	if _, err := encodeNativeDeferredCodecValue(deferred); err != nil {
		return nil, err
	}
	return []any{[]byte("OK")}, nil
}

func (v autoBatchSignaledJSONValue) MarshalJSON() ([]byte, error) {
	v.once.Do(func() { close(v.called) })
	return []byte(`"value"`), nil
}

func TestAutoBatchDefersJSONEncodingUntilQueueAdmission(t *testing.T) {
	pipeline := &gatedPipelineExecutor{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	base := NewClientWithExecutor(pipeline)
	exec := NewAutoBatchExecutor(base, AutoBatchOptions{
		MaxSize:       1,
		QueueSize:     1,
		FlushInterval: time.Hour,
		FlushTimeout:  time.Hour,
	})
	client := NewClientWithExecutor(exec, WithCodec(JSONCodec{}))
	done := make(chan error, 2)
	var releaseOnce sync.Once
	cleanup := func() {
		releaseOnce.Do(func() { close(pipeline.release) })
		_ = exec.Close()
	}
	defer cleanup()

	go func() { done <- client.KV().Set(context.Background(), "first", "value") }()
	select {
	case <-pipeline.entered:
	case <-time.After(time.Second):
		t.Fatal("first AutoBatch pipeline did not start")
	}

	go func() { done <- client.KV().Set(context.Background(), "second", "value") }()
	deadline := time.Now().Add(time.Second)
	for len(exec.requests) != 1 || len(exec.queueSlots) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("second AutoBatch request did not fill the admitted queue")
		}
		time.Sleep(time.Millisecond)
	}

	called := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	err := client.KV().Set(ctx, "third", autoBatchSignaledJSONValue{
		called: called,
		once:   &sync.Once{},
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queue-saturated Set error = %v; want deadline exceeded", err)
	}
	select {
	case <-called:
		t.Fatal("JSON encoding ran before AutoBatch queue admission")
	default:
	}

	releaseOnce.Do(func() { close(pipeline.release) })
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if err := exec.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAutoBatchRetainsDeferredEncodingForCapableDownstreamExecutor(t *testing.T) {
	pipeline := &autoBatchDeferredPipelineExecutor{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	exec := NewAutoBatchExecutor(
		NewClientWithExecutor(pipeline),
		AutoBatchOptions{MaxSize: 1, FlushInterval: time.Hour},
	)
	called := make(chan struct{})
	client := NewClientWithExecutor(exec, WithCodec(&autoBatchSignaledCodec{called: called}))
	var releaseOnce sync.Once
	cleanup := func() {
		releaseOnce.Do(func() { close(pipeline.release) })
		_ = exec.Close()
	}
	defer cleanup()

	done := make(chan error, 1)
	go func() {
		done <- client.KV().Set(context.Background(), "key", "value")
	}()
	select {
	case <-pipeline.entered:
	case <-time.After(time.Second):
		t.Fatal("downstream pipeline did not start")
	}
	select {
	case <-called:
		t.Fatal("JSON encoding ran before the capable downstream executor admitted the pipeline")
	default:
	}

	releaseOnce.Do(func() { close(pipeline.release) })
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
