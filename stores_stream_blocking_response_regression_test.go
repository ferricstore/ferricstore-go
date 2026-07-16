package ferricstore

import (
	"context"
	"testing"
)

func TestStreamCommandsRejectMalformedProtocolMetadata(t *testing.T) {
	one := 1
	tests := []struct {
		name     string
		response any
		call     func(*StreamStore) error
	}{
		{
			name:     "nil entry ID",
			response: []any{[]any{nil, "field", []byte("value")}},
			call: func(store *StreamStore) error {
				_, err := store.Range(context.Background(), "stream", "-", "+", nil)
				return err
			},
		},
		{
			name:     "malformed entry ID",
			response: []any{[]any{"not-an-id", "field", []byte("value")}},
			call: func(store *StreamStore) error {
				_, err := store.Range(context.Background(), "stream", "-", "+", nil)
				return err
			},
		},
		{
			name: "range IDs out of order",
			response: []any{
				[]any{"2-0", "field", []byte("two")},
				[]any{"1-0", "field", []byte("one")},
			},
			call: func(store *StreamStore) error {
				_, err := store.Range(context.Background(), "stream", "-", "+", nil)
				return err
			},
		},
		{
			name: "reverse range IDs out of order",
			response: []any{
				[]any{"1-0", "field", []byte("one")},
				[]any{"2-0", "field", []byte("two")},
			},
			call: func(store *StreamStore) error {
				_, err := store.RevRange(context.Background(), "stream", "+", "-", nil)
				return err
			},
		},
		{
			name:     "nil field name",
			response: []any{[]any{"1-0", nil, []byte("value")}},
			call: func(store *StreamStore) error {
				_, err := store.RevRange(context.Background(), "stream", "+", "-", nil)
				return err
			},
		},
		{
			name: "range above COUNT",
			response: []any{
				[]any{"1-0", "field", []byte("one")},
				[]any{"2-0", "field", []byte("two")},
			},
			call: func(store *StreamStore) error {
				_, err := store.Range(context.Background(), "stream", "-", "+", &one)
				return err
			},
		},
		{
			name:     "nil read stream key",
			response: []any{[]any{nil, []any{}}},
			call: func(store *StreamStore) error {
				_, err := store.Read(context.Background(), StreamReadOptions{
					Streams: []StreamRef{{Key: "stream", ID: "0-0"}},
				})
				return err
			},
		},
		{
			name:     "unexpected read stream key",
			response: []any{[]any{"other", []any{}}},
			call: func(store *StreamStore) error {
				_, err := store.Read(context.Background(), StreamReadOptions{
					Streams: []StreamRef{{Key: "stream", ID: "0-0"}},
				})
				return err
			},
		},
		{
			name: "read entries above COUNT",
			response: []any{[]any{"stream", []any{
				[]any{"1-0", "field", []byte("one")},
				[]any{"2-0", "field", []byte("two")},
			}}},
			call: func(store *StreamStore) error {
				_, err := store.Read(context.Background(), StreamReadOptions{
					Count: &one, Streams: []StreamRef{{Key: "stream", ID: "0-0"}},
				})
				return err
			},
		},
		{
			name: "read IDs out of order",
			response: []any{[]any{"stream", []any{
				[]any{"2-0", "field", []byte("two")},
				[]any{"1-0", "field", []byte("one")},
			}}},
			call: func(store *StreamStore) error {
				_, err := store.Read(context.Background(), StreamReadOptions{
					Streams: []StreamRef{{Key: "stream", ID: "0-0"}},
				})
				return err
			},
		},
		{
			name: "duplicate stream substituted for requested stream",
			response: []any{
				[]any{"one", []any{[]any{"1-0", "field", []byte("one")}}},
				[]any{"one", []any{[]any{"2-0", "field", []byte("two")}}},
			},
			call: func(store *StreamStore) error {
				_, err := store.Read(context.Background(), StreamReadOptions{Streams: []StreamRef{
					{Key: "one", ID: "0-0"},
					{Key: "two", ID: "0-0"},
				}})
				return err
			},
		},
		{
			name: "returned streams out of request order",
			response: []any{
				[]any{"two", []any{[]any{"1-0", "field", []byte("two")}}},
				[]any{"one", []any{[]any{"1-0", "field", []byte("one")}}},
			},
			call: func(store *StreamStore) error {
				_, err := store.Read(context.Background(), StreamReadOptions{Streams: []StreamRef{
					{Key: "one", ID: "0-0"},
					{Key: "two", ID: "0-0"},
				}})
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewClientWithExecutor(&fakeExecutor{value: test.response}).Stream()
			if err := test.call(store); err == nil {
				t.Fatalf("accepted malformed stream response %#v", test.response)
			}
		})
	}
}

func TestStreamReadAllowsRepeatedRequestedKeysWhenResponseMultiplicityMatches(t *testing.T) {
	response := []any{
		[]any{"stream", []any{[]any{"1-0", "field", []byte("one")}}},
		[]any{"stream", []any{[]any{"2-0", "field", []byte("two")}}},
	}
	store := NewClientWithExecutor(&fakeExecutor{value: response}).Stream()
	if _, err := store.Read(context.Background(), StreamReadOptions{Streams: []StreamRef{
		{Key: "stream", ID: "0-0"},
		{Key: "stream", ID: "1-0"},
	}}); err != nil {
		t.Fatalf("valid repeated stream response rejected: %v", err)
	}
}

func TestStreamInfoRejectsInconsistentMetadata(t *testing.T) {
	response := map[string]any{
		"length":            int64(1),
		"first-entry":       nil,
		"last-entry":        nil,
		"last-generated-id": "1-0",
		"groups":            int64(0),
	}
	store := NewClientWithExecutor(&fakeExecutor{value: response}).Stream()
	if _, err := store.Info(context.Background(), "stream"); err == nil {
		t.Fatal("accepted XINFO metadata with missing entries")
	}
}

func TestStreamInfoRejectsLastGeneratedIDBehindLastEntry(t *testing.T) {
	response := map[string]any{
		"length":            int64(2),
		"first-entry":       []any{"1-0", "field", []byte("one")},
		"last-entry":        []any{"2-0", "field", []byte("two")},
		"last-generated-id": "1-9",
		"groups":            int64(0),
	}
	store := NewClientWithExecutor(&fakeExecutor{value: response}).Stream()
	if _, err := store.Info(context.Background(), "stream"); err == nil {
		t.Fatal("accepted XINFO metadata with last-generated-id behind last-entry")
	}
}

func TestStreamAddRejectsMalformedReturnedID(t *testing.T) {
	store := NewClientWithExecutor(&fakeExecutor{value: "not-an-id"}).Stream()
	if _, err := store.Add(context.Background(), "stream", "*", map[string]any{"field": "value"}); err == nil {
		t.Fatal("accepted malformed XADD response ID")
	}
}

func TestStreamIDValidationDoesNotAllocate(t *testing.T) {
	if allocations := testing.AllocsPerRun(1000, func() {
		if _, _, ok := parseStreamIDResponse([]byte("18446744073709551615-42")); !ok {
			panic("valid stream ID rejected")
		}
	}); allocations != 0 {
		t.Fatalf("stream ID validation allocations = %v, want 0", allocations)
	}
}

func TestBlockingListCommandsRejectMalformedResponses(t *testing.T) {
	count := 1
	tests := []struct {
		name     string
		response any
		call     func(*ListStore) error
	}{
		{
			name:     "BLPOP unexpected key",
			response: []any{"other", []byte("value")},
			call: func(store *ListStore) error {
				_, err := store.BLPop(context.Background(), 0, "list")
				return err
			},
		},
		{
			name:     "BRPOP nil key",
			response: []any{nil, []byte("value")},
			call: func(store *ListStore) error {
				_, err := store.BRPop(context.Background(), 0, "list")
				return err
			},
		},
		{
			name:     "BLMPOP empty values",
			response: []any{"list", []any{}},
			call: func(store *ListStore) error {
				_, err := store.BLMPop(context.Background(), 0, []string{"list"}, "LEFT", &count)
				return err
			},
		},
		{
			name:     "BLMPOP above COUNT",
			response: []any{"list", []any{[]byte("one"), []byte("two")}},
			call: func(store *ListStore) error {
				_, err := store.BLMPop(context.Background(), 0, []string{"list"}, "LEFT", &count)
				return err
			},
		},
		{
			name:     "BLMPOP unexpected key",
			response: []any{"other", []any{[]byte("one")}},
			call: func(store *ListStore) error {
				_, err := store.BLMPop(context.Background(), 0, []string{"list"}, "LEFT", &count)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewClientWithExecutor(&fakeExecutor{value: test.response}).ListStore()
			if err := test.call(store); err == nil {
				t.Fatalf("accepted malformed blocking-list response %#v", test.response)
			}
		})
	}
}
