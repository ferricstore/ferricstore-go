package ferricstore

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type orderedBulkKVExecutor struct {
	msetOrders   [][]string
	msetNXOrders [][]string
}

type orderedMapCodec struct{}

func (*orderedMapCodec) Encode(value any) (any, error) { return value, nil }
func (*orderedMapCodec) Decode(value any) (any, error) { return value, nil }

func (*orderedBulkKVExecutor) Do(context.Context, ...any) (any, error) {
	return nil, errors.New("unexpected generic command")
}

func (*orderedBulkKVExecutor) keyValueMGet(context.Context, []string) (any, error) {
	return nil, errors.New("unexpected MGET")
}

func (e *orderedBulkKVExecutor) keyValueMSet(_ context.Context, keys []string, _ []any) (any, error) {
	e.msetOrders = append(e.msetOrders, append([]string(nil), keys...))
	return []byte("OK"), nil
}

func (e *orderedBulkKVExecutor) keyValueMSetNX(_ context.Context, keys []string, _ []any) (any, error) {
	e.msetNXOrders = append(e.msetNXOrders, append([]string(nil), keys...))
	return int64(1), nil
}

func TestKeyValueMapMutationsEncodeInDeterministicKeyOrder(t *testing.T) {
	values := map[string]any{
		"{order}:hotel": 8, "{order}:alpha": 1, "{order}:golf": 7, "{order}:bravo": 2,
		"{order}:foxtrot": 6, "{order}:charlie": 3, "{order}:echo": 5, "{order}:delta": 4,
	}
	want := []string{
		"{order}:alpha", "{order}:bravo", "{order}:charlie", "{order}:delta",
		"{order}:echo", "{order}:foxtrot", "{order}:golf", "{order}:hotel",
	}
	bulk := &orderedBulkKVExecutor{}
	store := NewClientWithExecutor(bulk, WithCodec(&orderedMapCodec{})).KV()

	for range 32 {
		if err := store.MSet(context.Background(), values); err != nil {
			t.Fatal(err)
		}
		if _, err := store.MSetNX(context.Background(), values); err != nil {
			t.Fatal(err)
		}
	}
	for _, order := range append(bulk.msetOrders, bulk.msetNXOrders...) {
		if !reflect.DeepEqual(order, want) {
			t.Fatalf("bulk key order = %v, want %v", order, want)
		}
	}

	fallback := &fakeExecutor{value: int64(1)}
	if _, err := NewClientWithExecutor(fallback, WithCodec(&orderedMapCodec{})).KV().MSetNX(
		context.Background(), values,
	); err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(values))
	for index := 1; index < len(fallback.calls[0]); index += 2 {
		got = append(got, fallback.calls[0][index].(string))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fallback MSETNX key order = %v, want %v", got, want)
	}
}
