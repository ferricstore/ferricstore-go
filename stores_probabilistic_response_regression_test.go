package ferricstore

import (
	"context"
	"math"
	"testing"
)

func TestCMSIncrByRejectsMalformedSingleResult(t *testing.T) {
	for _, response := range []any{
		int64(1),
		[]any{},
		[]any{int64(-1)},
		[]any{int64(1), int64(2)},
	} {
		store := NewClientWithExecutor(&fakeExecutor{value: response}).CountMinSketch()
		if _, err := store.IncrBy(context.Background(), "cms", "item", 1); err == nil {
			t.Fatalf("accepted malformed CMS.INCRBY response %#v", response)
		}
	}
}

func TestTopKListRejectsMalformedCounts(t *testing.T) {
	for _, response := range []any{
		[]any{[]byte("item"), int64(-1)},
		[]any{[]byte("item"), []byte("not-a-count")},
	} {
		store := NewClientWithExecutor(&fakeExecutor{value: response}).TopK()
		if _, err := store.List(context.Background(), "topk", true); err == nil {
			t.Fatalf("accepted malformed TOPK.LIST response %#v", response)
		}
	}
}

func TestTDigestRankRejectsOutOfContractSentinels(t *testing.T) {
	for _, command := range []func(*TDigestStore) error{
		func(store *TDigestStore) error {
			_, err := store.Rank(context.Background(), "digest", 1)
			return err
		},
		func(store *TDigestStore) error {
			_, err := store.RevRank(context.Background(), "digest", 1)
			return err
		},
	} {
		store := NewClientWithExecutor(&fakeExecutor{value: []any{int64(-3)}}).TDigest()
		if err := command(store); err == nil {
			t.Fatal("accepted TDIGEST rank sentinel below -2")
		}
	}
}

func TestTDigestFloatQueriesRejectOutOfContractValues(t *testing.T) {
	tests := []struct {
		name     string
		response any
		call     func(*TDigestStore) error
	}{
		{
			name:     "quantile infinity",
			response: []any{[]byte("inf")},
			call: func(store *TDigestStore) error {
				_, err := store.Quantile(context.Background(), "digest", 0.5)
				return err
			},
		},
		{
			name:     "CDF below zero",
			response: []any{[]byte("-0.1")},
			call: func(store *TDigestStore) error {
				_, err := store.CDF(context.Background(), "digest", 1)
				return err
			},
		},
		{
			name:     "CDF above one",
			response: []any{[]byte("1.1")},
			call: func(store *TDigestStore) error {
				_, err := store.CDF(context.Background(), "digest", 1)
				return err
			},
		},
		{
			name:     "trimmed mean infinity",
			response: []byte("inf"),
			call: func(store *TDigestStore) error {
				_, err := store.TrimmedMean(context.Background(), "digest", 0, 1)
				return err
			},
		},
		{
			name:     "minimum infinity",
			response: []byte("-inf"),
			call: func(store *TDigestStore) error {
				_, err := store.Min(context.Background(), "digest")
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewClientWithExecutor(&fakeExecutor{value: test.response}).TDigest()
			if err := test.call(store); err == nil {
				t.Fatalf("accepted malformed t-digest response %#v", test.response)
			}
		})
	}

	store := NewClientWithExecutor(&fakeExecutor{values: []any{
		[]any{[]byte("nan")}, []any{[]byte("nan")}, []byte("nan"), []byte("nan"),
	}}).TDigest()
	quantiles, err := store.Quantile(context.Background(), "digest", 0.5)
	if err != nil || len(quantiles) != 1 || !math.IsNaN(quantiles[0]) {
		t.Fatalf("empty quantile = %#v, %v", quantiles, err)
	}
	cdf, err := store.CDF(context.Background(), "digest", 1)
	if err != nil || len(cdf) != 1 || !math.IsNaN(cdf[0]) {
		t.Fatalf("empty CDF = %#v, %v", cdf, err)
	}
	mean, err := store.TrimmedMean(context.Background(), "digest", 0, 1)
	if err != nil || !math.IsNaN(mean) {
		t.Fatalf("empty trimmed mean = %v, %v", mean, err)
	}
	minimum, err := store.Min(context.Background(), "digest")
	if err != nil || !math.IsNaN(minimum) {
		t.Fatalf("empty minimum = %v, %v", minimum, err)
	}
}
