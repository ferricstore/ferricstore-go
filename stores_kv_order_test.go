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
		"hotel": 8, "alpha": 1, "golf": 7, "bravo": 2,
		"foxtrot": 6, "charlie": 3, "echo": 5, "delta": 4,
	}
	want := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"}
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
