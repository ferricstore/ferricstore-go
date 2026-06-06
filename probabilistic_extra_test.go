package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestBloomMAddBuildsCommand(t *testing.T) {
	exec := &fakeExecutor{value: []any{int64(1), int64(0)}}
	client := NewClientWithExecutor(exec)

	added, err := client.Bloom().MAdd(context.Background(), "bf", "a", "b")

	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(added, []bool{true, false}) {
		t.Fatalf("unexpected result: %#v", added)
	}
	assertCall(t, exec, []any{"BF.MADD", "bf", "a", "b"})
}

func TestCountMinSketchMergeBuildsCommand(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)

	ok, err := client.CountMinSketch().Merge(context.Background(), "dst", CMSMergeOptions{
		Sources: []string{"s1", "s2"},
		Weights: []int64{2, 3},
	})

	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected merge ok")
	}
	assertCall(t, exec, []any{"CMS.MERGE", "dst", 2, "s1", "s2", "WEIGHTS", int64(2), int64(3)})
}

func TestTopKReserveWithOptionsBuildsCommand(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)
	decay := 0.9

	ok, err := client.TopK().ReserveWithOptions(context.Background(), "tk", 3, TopKReserveOptions{
		Width: Int64(20),
		Depth: Int64(7),
		Decay: &decay,
	})

	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected reserve ok")
	}
	assertCall(t, exec, []any{"TOPK.RESERVE", "tk", int64(3), int64(20), int64(7), "0.9"})
}

func TestTDigestQuantileParsesFloats(t *testing.T) {
	exec := &fakeExecutor{value: []any{[]byte("2.5"), "4"}}
	client := NewClientWithExecutor(exec)

	values, err := client.TDigest().Quantile(context.Background(), "td", 0.5, 0.9)

	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(values, []float64{2.5, 4}) {
		t.Fatalf("unexpected values: %#v", values)
	}
	assertCall(t, exec, []any{"TDIGEST.QUANTILE", "td", "0.5", "0.9"})
}

func TestCuckooDelBuildsCommand(t *testing.T) {
	exec := &fakeExecutor{value: int64(1)}
	client := NewClientWithExecutor(exec)

	ok, err := client.Cuckoo().Del(context.Background(), "cf", "item")

	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected delete true")
	}
	assertCall(t, exec, []any{"CF.DEL", "cf", "item"})
}
