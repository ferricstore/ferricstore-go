package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestStreamAddWithOptionsEmitsSupportedTrimContract(t *testing.T) {
	exec := &fakeExecutor{value: "1000-1"}
	id, err := NewClientWithExecutor(exec).Stream().AddWithOptions(
		context.Background(), "events", "*", map[string]any{"type": "created"},
		StreamAddOptions{NoMkStream: true, MaxLen: Int64(100), Approximate: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	if id != "1000-1" {
		t.Fatalf("id = %q", id)
	}
	want := []any{"XADD", "events", "NOMKSTREAM", "MAXLEN", "~", int64(100), "*", "type", "created"}
	if !reflect.DeepEqual(exec.calls[0], want) {
		t.Fatalf("command = %#v, want %#v", exec.calls[0], want)
	}
}

func TestStreamAddWithOptionsSupportsMinIDAndNoMkStreamNil(t *testing.T) {
	exec := &fakeExecutor{value: nil}
	id, err := NewClientWithExecutor(exec).Stream().AddWithOptions(
		context.Background(), "events", "*", map[string]any{"type": "created"},
		StreamAddOptions{NoMkStream: true, MinID: "1000-0"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if id != "" {
		t.Fatalf("NOMKSTREAM nil id = %q, want empty", id)
	}
	want := []any{"XADD", "events", "NOMKSTREAM", "MINID", "1000-0", "*", "type", "created"}
	if !reflect.DeepEqual(exec.calls[0], want) {
		t.Fatalf("command = %#v, want %#v", exec.calls[0], want)
	}
}

func TestStreamAddWithOptionsRejectsInvalidCombinationsBeforeEncoding(t *testing.T) {
	tests := []StreamAddOptions{
		{MaxLen: Int64(1), MinID: "1-0"},
		{Approximate: true},
		{MaxLen: Int64(-1)},
		{MinID: "invalid"},
	}
	for _, opt := range tests {
		codec := &countingKVCodec{}
		exec := &fakeExecutor{}
		client := NewClientWithExecutor(exec, WithCodec(codec))
		if _, err := client.Stream().AddWithOptions(context.Background(), "events", "*", map[string]any{"field": "value"}, opt); err == nil {
			t.Fatalf("invalid options succeeded: %#v", opt)
		}
		if codec.encodes.Load() != 0 || len(exec.calls) != 0 {
			t.Fatalf("invalid options encoded or reached transport: encodes=%d calls=%#v", codec.encodes.Load(), exec.calls)
		}
	}
}

func TestStreamTrimWithOptionsSupportsMaxLenAndMinID(t *testing.T) {
	for _, test := range []struct {
		name string
		opt  StreamTrimOptions
		want []any
	}{
		{
			name: "maxlen exact", opt: StreamTrimOptions{MaxLen: Int64(100)},
			want: []any{"XTRIM", "events", "MAXLEN", int64(100)},
		},
		{
			name: "minid approximate", opt: StreamTrimOptions{MinID: "123-4", Approximate: true},
			want: []any{"XTRIM", "events", "MINID", "~", "123-4"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: int64(2)}
			deleted, err := NewClientWithExecutor(exec).Stream().TrimWithOptions(
				context.Background(), "events", test.opt,
			)
			if err != nil || deleted != 2 {
				t.Fatalf("TrimWithOptions = %d, %v", deleted, err)
			}
			if !reflect.DeepEqual(exec.calls[0], test.want) {
				t.Fatalf("command = %#v, want %#v", exec.calls[0], test.want)
			}
		})
	}
}

func TestV080StreamTrimMinIDAcceptsPartialStreamIDs(t *testing.T) {
	tests := []struct {
		name     string
		call     func(*StreamStore) error
		want     []any
		response any
	}{
		{
			name: "XADD trim",
			call: func(store *StreamStore) error {
				_, err := store.AddWithOptions(
					context.Background(), "events", "*", map[string]any{"type": "created"},
					StreamAddOptions{MinID: "123"},
				)
				return err
			},
			want:     []any{"XADD", "events", "MINID", "123", "*", "type", "created"},
			response: "123-1",
		},
		{
			name: "XTRIM",
			call: func(store *StreamStore) error {
				_, err := store.TrimWithOptions(
					context.Background(), "events", StreamTrimOptions{MinID: "123"},
				)
				return err
			},
			want:     []any{"XTRIM", "events", "MINID", "123"},
			response: int64(0),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: test.response}
			if err := test.call(NewClientWithExecutor(exec).Stream()); err != nil {
				t.Fatal(err)
			}
			if len(exec.calls) != 1 || !reflect.DeepEqual(exec.calls[0], test.want) {
				t.Fatalf("command = %#v, want %#v", exec.calls, test.want)
			}
		})
	}
}

func TestV080StreamReadsAcceptPartialStreamIDs(t *testing.T) {
	tests := []struct {
		name string
		call func(*StreamStore) error
		want []any
	}{
		{
			name: "XREAD",
			call: func(store *StreamStore) error {
				_, err := store.Read(context.Background(), StreamReadOptions{
					Streams: []StreamRef{{Key: "events", ID: "123"}},
				})
				return err
			},
			want: []any{"XREAD", "STREAMS", "events", "123"},
		},
		{
			name: "XREADGROUP pending",
			call: func(store *StreamStore) error {
				_, err := store.ReadGroup(context.Background(), StreamReadGroupOptions{
					Group: "workers", Consumer: "one",
					Streams: []StreamRef{{Key: "events", ID: "123"}},
				})
				return err
			},
			want: []any{"XREADGROUP", "GROUP", "workers", "one", "STREAMS", "events", "123"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []any{}}
			if err := test.call(NewClientWithExecutor(exec).Stream()); err != nil {
				t.Fatal(err)
			}
			if len(exec.calls) != 1 || !reflect.DeepEqual(exec.calls[0], test.want) {
				t.Fatalf("command = %#v, want %#v", exec.calls, test.want)
			}
		})
	}
}

func TestStreamTrimWithOptionsRejectsInvalidStrategyBeforeIO(t *testing.T) {
	tests := []StreamTrimOptions{
		{},
		{MaxLen: Int64(1), MinID: "1-0"},
		{MaxLen: Int64(-1)},
		{MinID: "invalid"},
	}
	for _, opt := range tests {
		exec := &fakeExecutor{}
		if _, err := NewClientWithExecutor(exec).Stream().TrimWithOptions(context.Background(), "events", opt); err == nil {
			t.Fatalf("invalid trim options succeeded: %#v", opt)
		}
		if len(exec.calls) != 0 {
			t.Fatalf("invalid trim reached transport: %#v", exec.calls)
		}
	}
}
