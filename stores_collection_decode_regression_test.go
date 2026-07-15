package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestHashFieldCommandsRejectMalformedResultArrays(t *testing.T) {
	tests := []struct {
		name     string
		response any
		call     func(*HashStore) error
	}{
		{name: "HEXPIRE length", response: []any{int64(1)}, call: func(store *HashStore) error {
			_, err := store.Expire(context.Background(), "hash", 1, "one", "two")
			return err
		}},
		{name: "HEXPIRE sentinel", response: []any{int64(0)}, call: func(store *HashStore) error {
			_, err := store.Expire(context.Background(), "hash", 1, "field")
			return err
		}},
		{name: "HTTL sentinel", response: []any{int64(-3)}, call: func(store *HashStore) error {
			_, err := store.TTL(context.Background(), "hash", "field")
			return err
		}},
		{name: "HPERSIST sentinel", response: []any{int64(0)}, call: func(store *HashStore) error {
			_, err := store.Persist(context.Background(), "hash", "field")
			return err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewClientWithExecutor(&fakeExecutor{value: test.response}).Hash()
			if err := test.call(store); err == nil {
				t.Fatalf("accepted malformed response %#v", test.response)
			}
		})
	}
}

func TestBlockingListResponsesDecodeStoredValues(t *testing.T) {
	payload := []byte(`{"count":1}`)
	exec := &fakeExecutor{values: []any{
		[]any{[]byte("left"), payload},
		[]any{[]byte("right"), payload},
		[]any{[]byte("many"), []any{payload, payload}},
	}}
	store := NewClientWithExecutor(exec, WithCodec(JSONCodec{})).ListStore()

	left, err := store.BLPop(context.Background(), 0, "left")
	if err != nil {
		t.Fatal(err)
	}
	right, err := store.BRPop(context.Background(), 0, "right")
	if err != nil {
		t.Fatal(err)
	}
	many, err := store.BLMPop(context.Background(), 0, []string{"many"}, "LEFT", Int(2))
	if err != nil {
		t.Fatal(err)
	}

	decoded := map[string]any{"count": float64(1)}
	if want := []any{[]byte("left"), decoded}; !reflect.DeepEqual(left, want) {
		t.Fatalf("BLPOP = %#v, want %#v", left, want)
	}
	if want := []any{[]byte("right"), decoded}; !reflect.DeepEqual(right, want) {
		t.Fatalf("BRPOP = %#v, want %#v", right, want)
	}
	if want := []any{[]byte("many"), []any{decoded, decoded}}; !reflect.DeepEqual(many, want) {
		t.Fatalf("BLMPOP = %#v, want %#v", many, want)
	}
}

func TestSortedSetStructuredResponsesDecodeOnlyMembers(t *testing.T) {
	payload := []byte(`{"member":1}`)
	exec := &fakeExecutor{values: []any{
		[]any{payload, []byte("1.5")},
		[]any{payload, []byte("2.5")},
		[]any{payload, []byte("3.5")},
		[]any{payload, []byte("4.5")},
	}}
	store := NewClientWithExecutor(exec, WithCodec(JSONCodec{})).SortedSet()
	count := 1

	results := make([]any, 0, 4)
	value, err := store.PopMin(context.Background(), "zset", &count)
	results = append(results, value)
	if err != nil {
		t.Fatal(err)
	}
	value, err = store.RandMember(context.Background(), "zset", &count, true)
	results = append(results, value)
	if err != nil {
		t.Fatal(err)
	}
	value, err = store.RangeByScore(context.Background(), "zset", "-inf", "+inf", true, nil, nil)
	results = append(results, value)
	if err != nil {
		t.Fatal(err)
	}
	value, err = store.RevRangeByScore(context.Background(), "zset", "+inf", "-inf", true, nil, nil)
	results = append(results, value)
	if err != nil {
		t.Fatal(err)
	}

	decoded := map[string]any{"member": float64(1)}
	for index, result := range results {
		items, ok := result.([]any)
		if !ok || len(items) != 2 || !reflect.DeepEqual(items[0], decoded) {
			t.Fatalf("structured sorted-set result %d = %#v", index, result)
		}
		if !reflect.DeepEqual(items[1], exec.values[index].([]any)[1]) {
			t.Fatalf("structured sorted-set score %d changed: %#v", index, items[1])
		}
	}
}

func TestCollectionScanResponsesDecodeStoredValues(t *testing.T) {
	payload := []byte(`{"value":1}`)
	exec := &fakeExecutor{values: []any{
		[]any{"~aA", []any{"field", payload}},
		[]any{"~cw", []any{payload}},
		[]any{"~eg", []any{payload, []byte("1.25")}},
	}}
	client := NewClientWithExecutor(exec, WithCodec(JSONCodec{}))

	results := make([]any, 0, 3)
	value, err := client.Hash().Scan(context.Background(), "hash", 0, "", nil)
	results = append(results, value)
	if err != nil {
		t.Fatal(err)
	}
	value, err = client.SetStore().Scan(context.Background(), "set", 0, "", nil)
	results = append(results, value)
	if err != nil {
		t.Fatal(err)
	}
	value, err = client.SortedSet().Scan(context.Background(), "zset", 0, "", nil)
	results = append(results, value)
	if err != nil {
		t.Fatal(err)
	}

	decoded := map[string]any{"value": float64(1)}
	wants := []any{
		[]any{"~aA", []any{"field", decoded}},
		[]any{"~cw", []any{decoded}},
		[]any{"~eg", []any{decoded, []byte("1.25")}},
	}
	for index := range results {
		if !reflect.DeepEqual(results[index], wants[index]) {
			t.Fatalf("collection scan %d = %#v, want %#v", index, results[index], wants[index])
		}
	}
}

func TestHashRandFieldWithValuesDecodesOnlyValues(t *testing.T) {
	payload := []byte(`{"value":1}`)
	client := NewClientWithExecutor(&fakeExecutor{value: []any{"field", payload}}, WithCodec(JSONCodec{}))
	value, err := client.Hash().RandField(context.Background(), "hash", Int(1), true)
	if err != nil {
		t.Fatal(err)
	}
	want := []any{"field", map[string]any{"value": float64(1)}}
	if !reflect.DeepEqual(value, want) {
		t.Fatalf("HRANDFIELD WITHVALUES = %#v, want %#v", value, want)
	}
}

func TestCollectionScalarResponsesRejectImpossibleNegativeValues(t *testing.T) {
	tests := []struct {
		name     string
		response int64
		call     func(*Client) error
	}{
		{name: "LINSERT below sentinel", response: -2, call: func(client *Client) error {
			_, err := client.ListStore().Insert(context.Background(), "list", true, "pivot", "value")
			return err
		}},
		{name: "ZRANK negative", response: -1, call: func(client *Client) error {
			_, err := client.SortedSet().Rank(context.Background(), "zset", "member")
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(NewClientWithExecutor(&fakeExecutor{value: test.response})); err == nil {
				t.Fatalf("accepted impossible response %d", test.response)
			}
		})
	}
}

func TestSetInterCardRejectsCountAbovePositiveLimit(t *testing.T) {
	client := NewClientWithExecutor(&fakeExecutor{value: int64(3)})
	if _, err := client.SetStore().InterCard(context.Background(), []string{"one", "two"}, Int64(2)); err == nil {
		t.Fatal("SINTERCARD accepted a count above LIMIT")
	}
}
