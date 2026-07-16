package ferricstore

import (
	"context"
	"testing"
)

func TestScanCommandsRejectMalformedResponses(t *testing.T) {
	tests := []struct {
		name     string
		response any
		call     func(*Client) error
	}{
		{
			name:     "SCAN negative cursor",
			response: []any{int64(-1), []any{}},
			call: func(client *Client) error {
				_, err := client.Scan(context.Background(), 0, "", nil)
				return err
			},
		},
		{
			name:     "SCAN values shape",
			response: []any{int64(0), "key"},
			call: func(client *Client) error {
				_, err := client.Scan(context.Background(), 0, "", nil)
				return err
			},
		},
		{
			name:     "SCAN nil key",
			response: []any{int64(0), []any{nil}},
			call: func(client *Client) error {
				_, err := client.Scan(context.Background(), 0, "", nil)
				return err
			},
		},
		{
			name:     "HSCAN invalid cursor token",
			response: []any{"field", []any{}},
			call: func(client *Client) error {
				_, err := client.Hash().Scan(context.Background(), "hash", 0, "", nil)
				return err
			},
		},
		{
			name:     "HSCAN nil field",
			response: []any{"0", []any{nil, []byte("value")}},
			call: func(client *Client) error {
				_, err := client.Hash().Scan(context.Background(), "hash", 0, "", nil)
				return err
			},
		},
		{
			name:     "ZSCAN invalid score",
			response: []any{"0", []any{[]byte("member"), []byte("NaN")}},
			call: func(client *Client) error {
				_, err := client.SortedSet().Scan(context.Background(), "zset", 0, "", nil)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := NewClientWithExecutor(&fakeExecutor{value: test.response})
			if err := test.call(client); err == nil {
				t.Fatalf("accepted malformed scan response %#v", test.response)
			}
		})
	}
}

func TestRandomCollectionCommandsRejectMalformedResponses(t *testing.T) {
	countTwo := 2
	tests := []struct {
		name     string
		response any
		call     func(*Client) error
	}{
		{
			name:     "HRANDFIELD scalar with count",
			response: []byte("field"),
			call: func(client *Client) error {
				_, err := client.Hash().RandField(context.Background(), "hash", &countTwo, false)
				return err
			},
		},
		{
			name:     "HRANDFIELD nil field",
			response: []any{nil},
			call: func(client *Client) error {
				_, err := client.Hash().RandField(context.Background(), "hash", &countTwo, false)
				return err
			},
		},
		{
			name:     "HRANDFIELD too many fields",
			response: []any{"one", "two", "three"},
			call: func(client *Client) error {
				_, err := client.Hash().RandField(context.Background(), "hash", &countTwo, false)
				return err
			},
		},
		{
			name:     "HRANDFIELD WITHVALUES nil field",
			response: []any{nil, []byte("value")},
			call: func(client *Client) error {
				_, err := client.Hash().RandField(context.Background(), "hash", &countTwo, true)
				return err
			},
		},
		{
			name:     "ZRANDMEMBER invalid score",
			response: []any{[]byte("member"), []byte("not-a-score")},
			call: func(client *Client) error {
				_, err := client.SortedSet().RandMember(context.Background(), "zset", &countTwo, true)
				return err
			},
		},
		{
			name:     "ZRANDMEMBER too many members",
			response: []any{[]byte("one"), []byte("two"), []byte("three")},
			call: func(client *Client) error {
				_, err := client.SortedSet().RandMember(context.Background(), "zset", &countTwo, false)
				return err
			},
		},
		{
			name:     "ZPOPMIN too many pairs",
			response: []any{[]byte("one"), []byte("1"), []byte("two"), []byte("2")},
			call: func(client *Client) error {
				_, err := client.SortedSet().PopMin(context.Background(), "zset", nil)
				return err
			},
		},
		{
			name:     "SRANDMEMBER too many members",
			response: []any{[]byte("one"), []byte("two"), []byte("three")},
			call: func(client *Client) error {
				_, err := client.SetStore().RandMember(context.Background(), "set", &countTwo)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := NewClientWithExecutor(&fakeExecutor{value: test.response})
			if err := test.call(client); err == nil {
				t.Fatalf("accepted malformed random response %#v", test.response)
			}
		})
	}
}

func TestSortedSetCommandsRejectMalformedScoreMetadata(t *testing.T) {
	for _, call := range []func(*SortedSetStore) error{
		func(store *SortedSetStore) error {
			_, err := store.PopMax(context.Background(), "zset", nil)
			return err
		},
		func(store *SortedSetStore) error {
			_, err := store.RangeByScore(context.Background(), "zset", "-inf", "+inf", true, nil, nil)
			return err
		},
		func(store *SortedSetStore) error {
			_, err := store.RevRangeByScore(context.Background(), "zset", "+inf", "-inf", true, nil, nil)
			return err
		},
	} {
		store := NewClientWithExecutor(&fakeExecutor{
			value: []any{[]byte("member"), []byte("+inf")},
		}).SortedSet()
		if err := call(store); err == nil {
			t.Fatal("accepted non-finite sorted-set score")
		}
	}
}

func TestSortedSetCommandsRejectScoresInWrongOrder(t *testing.T) {
	countTwo := 2
	tests := []struct {
		name     string
		response any
		call     func(*SortedSetStore) error
	}{
		{
			name: "ZPOPMIN descending",
			response: []any{
				[]byte("high"), []byte("2"), []byte("low"), []byte("1"),
			},
			call: func(store *SortedSetStore) error {
				_, err := store.PopMin(context.Background(), "zset", &countTwo)
				return err
			},
		},
		{
			name: "ZPOPMAX ascending",
			response: []any{
				[]byte("low"), []byte("1"), []byte("high"), []byte("2"),
			},
			call: func(store *SortedSetStore) error {
				_, err := store.PopMax(context.Background(), "zset", &countTwo)
				return err
			},
		},
		{
			name: "ZRANGEBYSCORE descending",
			response: []any{
				[]byte("high"), []byte("2"), []byte("low"), []byte("1"),
			},
			call: func(store *SortedSetStore) error {
				_, err := store.RangeByScore(context.Background(), "zset", "-inf", "+inf", true, nil, nil)
				return err
			},
		},
		{
			name: "ZREVRANGEBYSCORE ascending",
			response: []any{
				[]byte("low"), []byte("1"), []byte("high"), []byte("2"),
			},
			call: func(store *SortedSetStore) error {
				_, err := store.RevRangeByScore(context.Background(), "zset", "+inf", "-inf", true, nil, nil)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewClientWithExecutor(&fakeExecutor{value: test.response}).SortedSet()
			if err := test.call(store); err == nil {
				t.Fatalf("accepted scores in wrong order: %#v", test.response)
			}
		})
	}
}
