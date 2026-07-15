package ferricstore

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type canceledAutoBatchCodec struct{ calls atomic.Int64 }

func (c *canceledAutoBatchCodec) Encode(value any) (any, error) {
	c.calls.Add(1)
	return value, nil
}

func (*canceledAutoBatchCodec) Decode(value any) (any, error) { return value, nil }

func TestAutoBatchPreCanceledAdmissionSkipsCodecAndQueue(t *testing.T) {
	downstream := &fakePipelineExecutor{}
	exec := NewAutoBatchExecutor(
		NewClientWithExecutor(downstream),
		AutoBatchOptions{MaxSize: 1_000, QueueSize: 128, FlushInterval: time.Hour},
	)
	codec := &canceledAutoBatchCodec{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for range 100 {
		_, err := exec.Do(ctx, "SET", "key", nativeDeferredCodecValue{codec: codec, value: "value"})
		if !errors.Is(err, context.Canceled) {
			_ = exec.Close()
			t.Fatalf("pre-canceled autobatch call error = %v; want context.Canceled", err)
		}
	}
	if err := exec.Close(); err != nil {
		t.Fatal(err)
	}
	if calls := codec.calls.Load(); calls != 0 {
		t.Fatalf("pre-canceled autobatch calls invoked codec %d times; want 0", calls)
	}
	if batches := downstream.batchCount(); batches != 0 {
		t.Fatalf("pre-canceled autobatch calls dispatched %d batches; want 0", batches)
	}
}
