package ferricstore

import (
	"context"
	"math"
	"reflect"
	"testing"
)

func TestCollectionFloatMutationsRejectNonFiniteValuesBeforeCodecOrTransport(t *testing.T) {
	for _, invalid := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		tests := []struct {
			name string
			call func(*Client) error
		}{
			{name: "HINCRBYFLOAT", call: func(client *Client) error {
				_, err := client.Hash().IncrByFloat(context.Background(), "hash", "field", invalid)
				return err
			}},
			{name: "ZINCRBY", call: func(client *Client) error {
				_, err := client.SortedSet().IncrBy(context.Background(), "zset", invalid, "member")
				return err
			}},
			{name: "ZADD", call: func(client *Client) error {
				_, err := client.SortedSet().Add(
					context.Background(),
					"zset",
					ZAddMember{Score: 1, Member: "first"},
					ZAddMember{Score: invalid, Member: "invalid"},
				)
				return err
			}},
			{name: "ZADD options", call: func(client *Client) error {
				_, err := client.SortedSet().AddWithOptions(
					context.Background(),
					"zset",
					ZAddOptions{CH: true},
					ZAddMember{Score: 1, Member: "first"},
					ZAddMember{Score: invalid, Member: "invalid"},
				)
				return err
			}},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				exec := &fakeExecutor{value: int64(1)}
				codec := &countingKVCodec{}
				if err := test.call(NewClientWithExecutor(exec, WithConcurrentCodec(codec))); err == nil {
					t.Fatal("non-finite mutation succeeded")
				}
				if codec.encodes.Load() != 0 {
					t.Fatalf("non-finite mutation invoked codec %d times", codec.encodes.Load())
				}
				if len(exec.calls) != 0 {
					t.Fatalf("non-finite mutation reached executor: %#v", exec.calls)
				}
			})
		}
	}
}

func TestCollectionCardinalityHelpersRejectNegativeResponses(t *testing.T) {
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "HLEN", call: func(client *Client) error { _, err := client.Hash().Len(context.Background(), "h"); return err }},
		{name: "HSTRLEN", call: func(client *Client) error { _, err := client.Hash().StrLen(context.Background(), "h", "f"); return err }},
		{name: "LPUSH", call: func(client *Client) error {
			_, err := client.ListStore().LPush(context.Background(), "l", "v")
			return err
		}},
		{name: "LLEN", call: func(client *Client) error { _, err := client.ListStore().Len(context.Background(), "l"); return err }},
		{name: "LREM", call: func(client *Client) error {
			_, err := client.ListStore().Rem(context.Background(), "l", 0, "v")
			return err
		}},
		{name: "SCARD", call: func(client *Client) error { _, err := client.SetStore().Card(context.Background(), "s"); return err }},
		{name: "SDIFFSTORE", call: func(client *Client) error {
			_, err := client.SetStore().DiffStore(context.Background(), "d", "s")
			return err
		}},
		{name: "SINTERCARD", call: func(client *Client) error {
			_, err := client.SetStore().InterCard(context.Background(), []string{"s"}, nil)
			return err
		}},
		{name: "ZCARD", call: func(client *Client) error { _, err := client.SortedSet().Card(context.Background(), "z"); return err }},
		{name: "ZCOUNT", call: func(client *Client) error {
			_, err := client.SortedSet().Count(context.Background(), "z", "-inf", "+inf")
			return err
		}},
		{name: "XLEN", call: func(client *Client) error { _, err := client.Stream().Len(context.Background(), "x"); return err }},
		{name: "XTRIM", call: func(client *Client) error {
			_, err := client.Stream().Trim(context.Background(), "x", false, "1", nil)
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(NewClientWithExecutor(&fakeExecutor{value: int64(-1)})); err == nil {
				t.Fatal("accepted negative cardinality response")
			}
		})
	}
}

func TestEmptyListPushesAreLocalNoOps(t *testing.T) {
	exec := &fakeExecutor{value: int64(1)}
	store := NewClientWithExecutor(exec).ListStore()
	for _, call := range []func() (int64, error){
		func() (int64, error) { return store.LPush(context.Background(), "list") },
		func() (int64, error) { return store.RPush(context.Background(), "list") },
		func() (int64, error) { return store.LPushX(context.Background(), "list") },
		func() (int64, error) { return store.RPushX(context.Background(), "list") },
	} {
		if value, err := call(); err != nil || value != 0 {
			t.Fatalf("empty push = %d, %v; want 0, nil", value, err)
		}
	}
	if len(exec.calls) != 0 {
		t.Fatalf("empty pushes reached executor: %#v", exec.calls)
	}
}

func TestStreamReadsDecodeOnlyFieldValues(t *testing.T) {
	entries := []any{
		[]any{[]byte("1-0"), []byte("field"), []byte(`{"count":1}`)},
	}
	streamResults := []any{[]any{[]byte("stream"), entries}}
	exec := &fakeExecutor{values: []any{entries, entries, streamResults, streamResults}}
	store := NewClientWithExecutor(exec, WithCodec(JSONCodec{})).Stream()

	results := make([]any, 0, 4)
	value, err := store.Range(context.Background(), "stream", "-", "+", nil)
	results = append(results, value)
	if err != nil {
		t.Fatal(err)
	}
	value, err = store.RevRange(context.Background(), "stream", "+", "-", nil)
	results = append(results, value)
	if err != nil {
		t.Fatal(err)
	}
	value, err = store.Read(context.Background(), StreamReadOptions{Streams: []StreamRef{{Key: "stream", ID: "0-0"}}})
	results = append(results, value)
	if err != nil {
		t.Fatal(err)
	}
	value, err = store.ReadGroup(context.Background(), StreamReadGroupOptions{
		Group: "group", Consumer: "consumer", Streams: []StreamRef{{Key: "stream", ID: ">"}},
	})
	results = append(results, value)
	if err != nil {
		t.Fatal(err)
	}

	wantEntry := []any{[]byte("1-0"), []byte("field"), map[string]any{"count": float64(1)}}
	for index, result := range results {
		var gotEntry []any
		if index < 2 {
			gotEntry = result.([]any)[0].([]any)
		} else {
			gotEntry = result.([]any)[0].([]any)[1].([]any)[0].([]any)
		}
		if !reflect.DeepEqual(gotEntry, wantEntry) {
			t.Fatalf("decoded stream result %d entry = %#v; want %#v", index, gotEntry, wantEntry)
		}
	}
}

func TestStreamReadRejectsMalformedEntryShape(t *testing.T) {
	store := NewClientWithExecutor(&fakeExecutor{value: []any{
		[]any{"1-0", "field-without-value"},
	}}, WithCodec(JSONCodec{})).Stream()
	if _, err := store.Range(context.Background(), "stream", "-", "+", nil); err == nil {
		t.Fatal("malformed stream entry succeeded")
	}
}

func TestStreamRawDecodeFastPathDoesNotAllocate(t *testing.T) {
	entries := []any{[]any{"1-0", "field", []byte("value")}}
	streamRead := []any{[]any{"stream", entries}}
	if allocations := testing.AllocsPerRun(1000, func() {
		if _, err := decodeStreamEntries(RawCodec{}, entries, nil); err != nil {
			panic(err)
		}
		if _, err := decodeStreamRead(RawCodec{}, streamRead, nil); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("raw stream decode allocations = %v; want 0", allocations)
	}
}

func TestStreamAddRejectsEmptyFieldsAndOrdersCustomCodecCalls(t *testing.T) {
	exec := &fakeExecutor{value: []byte("1-0")}
	codec := &countingKVCodec{}
	store := NewClientWithExecutor(exec, WithConcurrentCodec(codec)).Stream()
	if _, err := store.Add(context.Background(), "stream", "*", map[string]any{}); err == nil {
		t.Fatal("empty XADD fields succeeded")
	}
	if codec.encodes.Load() != 0 || len(exec.calls) != 0 {
		t.Fatalf("empty XADD invoked codec/transport: encodes=%d calls=%#v", codec.encodes.Load(), exec.calls)
	}

	fields := map[string]any{
		"hotel": 8, "alpha": 1, "golf": 7, "bravo": 2,
		"foxtrot": 6, "charlie": 3, "echo": 5, "delta": 4,
	}
	want := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"}
	for range 32 {
		if _, err := store.Add(context.Background(), "stream", "*", fields); err != nil {
			t.Fatal(err)
		}
	}
	for _, call := range exec.calls {
		got := make([]string, 0, len(fields))
		for index := 3; index < len(call); index += 2 {
			got = append(got, call[index].(string))
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("XADD field order = %v; want %v", got, want)
		}
	}
}
