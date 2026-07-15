package ferricstore

import (
	"context"
	"testing"
)

func TestBloomBooleanCommandsRejectOutOfDomainCounts(t *testing.T) {
	for _, call := range []struct {
		name string
		run  func(*BloomFilterStore) error
	}{
		{name: "BF.ADD", run: func(store *BloomFilterStore) error {
			_, err := store.Add(context.Background(), "filter", "item")
			return err
		}},
		{name: "BF.EXISTS", run: func(store *BloomFilterStore) error {
			_, err := store.Exists(context.Background(), "filter", "item")
			return err
		}},
	} {
		for _, response := range []int64{-1, 2} {
			t.Run(call.name, func(t *testing.T) {
				store := NewClientWithExecutor(&fakeExecutor{value: response}).Bloom()
				if err := call.run(store); err == nil {
					t.Fatalf("%s accepted out-of-domain boolean count %d", call.name, response)
				}
			})
		}
	}
}

func TestHashSetEXRejectsImpossibleCounts(t *testing.T) {
	for _, count := range []int64{-1, 2} {
		client := NewClientWithExecutor(&fakeExecutor{value: count})
		_, err := client.Hash().SetEX(
			context.Background(),
			"hash",
			map[string]any{"field": "value"},
			HashSetEXOptions{EXSeconds: Int64(60)},
		)
		if err == nil {
			t.Fatalf("HSETEX accepted impossible count %d for one field", count)
		}
	}
}
