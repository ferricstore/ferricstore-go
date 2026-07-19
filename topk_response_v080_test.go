package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

var (
	_ func(*TopKStore, context.Context, string) ([]any, error)       = (*TopKStore).List
	_ func(*TopKStore, context.Context, string) ([]TopKEntry, error) = (*TopKStore).ListWithCount
)

func TestTopKListMethodsReturnStableTypedShapes(t *testing.T) {
	payload := []byte(`{"item":1}`)
	exec := &fakeExecutor{values: []any{
		[]any{payload},
		[]any{payload, []byte("3")},
	}}
	store := NewClientWithExecutor(exec, WithCodec(JSONCodec{})).TopK()

	items, err := store.List(context.Background(), "topk")
	if err != nil {
		t.Fatal(err)
	}
	entries, err := store.ListWithCount(context.Background(), "topk")
	if err != nil {
		t.Fatal(err)
	}

	decoded := map[string]any{"item": float64(1)}
	if want := []any{decoded}; !reflect.DeepEqual(items, want) {
		t.Fatalf("TOPK.LIST = %#v, want %#v", items, want)
	}
	if want := []TopKEntry{{Item: decoded, Count: 3}}; !reflect.DeepEqual(entries, want) {
		t.Fatalf("TOPK.LIST WITHCOUNT = %#v, want %#v", entries, want)
	}
}

func TestTopKListMethodsRejectMalformedItems(t *testing.T) {
	plain := NewClientWithExecutor(&fakeExecutor{value: []any{int64(1)}}).TopK()
	if _, err := plain.List(context.Background(), "topk"); err == nil {
		t.Fatal("TOPK.LIST accepted a non-binary item")
	}

	withCount := NewClientWithExecutor(&fakeExecutor{value: []any{nil, int64(1)}}).TopK()
	if _, err := withCount.ListWithCount(context.Background(), "topk"); err == nil {
		t.Fatal("TOPK.LIST WITHCOUNT accepted a nil item")
	}
}

var (
	topKListItemsSink   []any
	topKListEntriesSink []TopKEntry
)

func TestTopKRawListDecodeHasBoundedAllocations(t *testing.T) {
	var plain any = []any{[]byte("one"), []byte("two")}
	var withCount any = []any{[]byte("one"), int64(2), []byte("two"), int64(1)}
	var codec Codec = RawCodec{}

	plainAllocs := testing.AllocsPerRun(1000, func() {
		var err error
		topKListItemsSink, err = decodeTopKList(codec, plain, nil)
		if err != nil {
			panic(err)
		}
	})
	countedAllocs := testing.AllocsPerRun(1000, func() {
		var err error
		topKListEntriesSink, err = decodeTopKListWithCount(codec, withCount, nil)
		if err != nil {
			panic(err)
		}
	})

	if plainAllocs != 0 {
		t.Fatalf("raw TOPK.LIST decode allocations = %.0f; want 0", plainAllocs)
	}
	if countedAllocs > 1 {
		t.Fatalf("typed raw TOPK.LIST WITHCOUNT decode allocations = %.0f; want <= 1", countedAllocs)
	}
}
