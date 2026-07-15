package ferricstore

import (
	"context"
	"strconv"
	"testing"
)

type kvBenchmarkExecutor struct{ response any }

func (e kvBenchmarkExecutor) Do(context.Context, ...any) (any, error) {
	return e.response, nil
}

func (e kvBenchmarkExecutor) keyValueMGet(context.Context, []string) (any, error) {
	return e.response, nil
}

func (e kvBenchmarkExecutor) keyValueMSet(context.Context, []string, []any) (any, error) {
	return e.response, nil
}

func (e kvBenchmarkExecutor) keyValueMSetNX(context.Context, []string, []any) (any, error) {
	return e.response, nil
}

func (e kvBenchmarkExecutor) keyValueDel(context.Context, []string) (any, error) {
	return e.response, nil
}

func (e kvBenchmarkExecutor) keyValueExists(context.Context, []string) (any, error) {
	return e.response, nil
}

var kvBenchmarkResult any

func TestKeyValueBulkPathsHaveBoundedAllocations(t *testing.T) {
	keys := make([]string, 100)
	response := make([]any, len(keys))
	values := make(map[string]any, len(keys))
	for i := range keys {
		keys[i] = "key:" + strconv.Itoa(i)
		response[i] = []byte("value")
		values[keys[i]] = []byte("value")
	}
	ctx := context.Background()
	mget := NewClientWithExecutor(kvBenchmarkExecutor{response: response}).KV()
	mset := NewClientWithExecutor(kvBenchmarkExecutor{response: []byte("OK")}).KV()
	msetnx := NewClientWithExecutor(kvBenchmarkExecutor{response: int64(1)}).KV()
	del := NewClientWithExecutor(kvBenchmarkExecutor{response: int64(100)}).KV()
	exists := NewClientWithExecutor(kvBenchmarkExecutor{response: int64(100)}).KV()

	mgetAllocs := testing.AllocsPerRun(100, func() {
		var err error
		kvBenchmarkResult, err = mget.MGet(ctx, keys...)
		if err != nil {
			panic(err)
		}
	})
	if mgetAllocs > 10 {
		t.Fatalf("MGET(100) allocations = %.0f; want bounded allocation count <= 10", mgetAllocs)
	}

	msetAllocs := testing.AllocsPerRun(100, func() {
		if err := mset.MSet(ctx, values); err != nil {
			panic(err)
		}
	})
	if msetAllocs > 10 {
		t.Fatalf("MSET(100) allocations = %.0f; want bounded allocation count <= 10", msetAllocs)
	}

	msetnxAllocs := testing.AllocsPerRun(100, func() {
		if _, err := msetnx.MSetNX(ctx, values); err != nil {
			panic(err)
		}
	})
	if msetnxAllocs > 2 {
		t.Fatalf("MSETNX(100) allocations = %.0f; want bounded allocation count <= 2", msetnxAllocs)
	}

	delAllocs := testing.AllocsPerRun(100, func() {
		if _, err := del.Del(ctx, keys...); err != nil {
			panic(err)
		}
	})
	if delAllocs > 3 {
		t.Fatalf("DEL(100) allocations = %.0f; want bounded allocation count <= 3", delAllocs)
	}

	existsAllocs := testing.AllocsPerRun(100, func() {
		if _, err := exists.Exists(ctx, keys...); err != nil {
			panic(err)
		}
	})
	if existsAllocs > 3 {
		t.Fatalf("EXISTS(100) allocations = %.0f; want bounded allocation count <= 3", existsAllocs)
	}
}

func BenchmarkKeyValueMGet100(b *testing.B) {
	keys := make([]string, 100)
	response := make([]any, len(keys))
	for i := range keys {
		keys[i] = "key:" + strconv.Itoa(i)
		response[i] = []byte("value")
	}
	store := NewClientWithExecutor(kvBenchmarkExecutor{response: response}).KV()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var err error
		kvBenchmarkResult, err = store.MGet(ctx, keys...)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKeyValueMSet100(b *testing.B) {
	values := make(map[string]any, 100)
	for i := range 100 {
		values["key:"+strconv.Itoa(i)] = []byte("value")
	}
	store := NewClientWithExecutor(kvBenchmarkExecutor{response: []byte("OK")}).KV()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := store.MSet(ctx, values); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKeyValueMSetNX100(b *testing.B) {
	values := make(map[string]any, 100)
	for i := range 100 {
		values["key:"+strconv.Itoa(i)] = []byte("value")
	}
	store := NewClientWithExecutor(kvBenchmarkExecutor{response: int64(1)}).KV()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := store.MSetNX(ctx, values); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKeyValueDel100(b *testing.B) {
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = "key:" + strconv.Itoa(i)
	}
	store := NewClientWithExecutor(kvBenchmarkExecutor{response: int64(100)}).KV()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := store.Del(ctx, keys...); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKeyValueExists100(b *testing.B) {
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = "key:" + strconv.Itoa(i)
	}
	store := NewClientWithExecutor(kvBenchmarkExecutor{response: int64(100)}).KV()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := store.Exists(ctx, keys...); err != nil {
			b.Fatal(err)
		}
	}
}
