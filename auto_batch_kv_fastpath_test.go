package ferricstore

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type autoBatchKVFastPathProbe struct {
	generic atomic.Int64
	mget    atomic.Int64
	mset    atomic.Int64
	msetnx  atomic.Int64
	del     atomic.Int64
	exists  atomic.Int64
}

func (*autoBatchKVFastPathProbe) Do(context.Context, ...any) (any, error) {
	return nil, errors.New("unexpected direct generic call")
}

func (e *autoBatchKVFastPathProbe) Pipeline(_ context.Context, commands [][]any) ([]any, error) {
	e.generic.Add(int64(len(commands)))
	results := make([]any, len(commands))
	for index, command := range commands {
		switch commandName(command) {
		case "MGET":
			results[index] = []any{[]byte("value")}
		case "MSET":
			results[index] = []byte("OK")
		case "MSETNX", "DEL", "EXISTS":
			results[index] = int64(1)
		}
	}
	return results, nil
}

func (e *autoBatchKVFastPathProbe) keyValueMGet(context.Context, []string) (any, error) {
	e.mget.Add(1)
	return []any{[]byte("value")}, nil
}

func (e *autoBatchKVFastPathProbe) keyValueMSet(context.Context, []string, []any) (any, error) {
	e.mset.Add(1)
	return []byte("OK"), nil
}

func (e *autoBatchKVFastPathProbe) keyValueMSetNX(context.Context, []string, []any) (any, error) {
	e.msetnx.Add(1)
	return int64(1), nil
}

func (e *autoBatchKVFastPathProbe) keyValueDel(context.Context, []string) (any, error) {
	e.del.Add(1)
	return int64(1), nil
}

func (e *autoBatchKVFastPathProbe) keyValueExists(context.Context, []string) (any, error) {
	e.exists.Add(1)
	return int64(1), nil
}

func TestAutoBatchPreservesTypedKVFastPaths(t *testing.T) {
	probe := &autoBatchKVFastPathProbe{}
	exec := NewAutoBatchExecutor(
		NewClientWithExecutor(probe),
		AutoBatchOptions{MaxSize: 1, FlushInterval: time.Hour},
	)
	defer func() { _ = exec.Close() }()
	store := NewClientWithExecutor(exec).KV()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if _, err := store.MGet(ctx, "key"); err != nil {
		t.Fatal(err)
	}
	if err := store.MSet(ctx, map[string]any{"key": "value"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MSetNX(ctx, map[string]any{"key": "value"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Del(ctx, "key"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Exists(ctx, "key"); err != nil {
		t.Fatal(err)
	}

	if got := probe.generic.Load(); got != 0 {
		t.Fatalf("AutoBatch boxed %d typed KV command(s) into generic pipelines", got)
	}
	for name, calls := range map[string]int64{
		"MGET": probe.mget.Load(), "MSET": probe.mset.Load(), "MSETNX": probe.msetnx.Load(),
		"DEL": probe.del.Load(), "EXISTS": probe.exists.Load(),
	} {
		if calls != 1 {
			t.Fatalf("typed %s calls = %d; want 1", name, calls)
		}
	}
}
