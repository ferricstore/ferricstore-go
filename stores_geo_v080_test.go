package ferricstore

import (
	"context"
	"strings"
	"testing"
)

func TestV080GeoSearchStoreRejectsUnsupportedStoreDistLocally(t *testing.T) {
	exec := &fakeExecutor{value: int64(1)}
	codec := &countingKVCodec{}
	store := NewClientWithExecutor(exec, WithConcurrentCodec(codec)).Geo()

	_, err := store.SearchStore(context.Background(), "destination", "source", GeoSearchOptions{
		FromMember: "origin",
		ByRadius:   &GeoRadius{Radius: 1, Unit: "km"},
	}, true)
	if err == nil || !strings.Contains(err.Error(), "unsupported by FerricStore 0.8") {
		t.Fatalf("SearchStore STOREDIST error = %v", err)
	}
	if codec.encodes.Load() != 0 {
		t.Fatalf("unsupported STOREDIST invoked codec %d times", codec.encodes.Load())
	}
	if len(exec.calls) != 0 {
		t.Fatalf("unsupported STOREDIST reached transport: %#v", exec.calls)
	}
}

func TestV080HashPExpireTimeRejectsUnsupportedCommandLocally(t *testing.T) {
	exec := &fakeExecutor{value: []any{int64(1)}}
	store := NewClientWithExecutor(exec).Hash()

	_, err := store.PExpireTime(context.Background(), "hash", "field")
	if err == nil || !strings.Contains(err.Error(), "unsupported by FerricStore 0.8") {
		t.Fatalf("Hash.PExpireTime error = %v", err)
	}
	if len(exec.calls) != 0 {
		t.Fatalf("unsupported HPEXPIRETIME reached transport: %#v", exec.calls)
	}
}
