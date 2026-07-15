package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestKeyValueMSetBuildsDeterministicCommand(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)

	err := client.KV().MSet(context.Background(), map[string]any{
		"b": []byte("2"),
		"a": []byte("1"),
	})

	if err != nil {
		t.Fatal(err)
	}
	assertCall(t, exec, []any{"MSET", "a", []byte("1"), "b", []byte("2")})
}

func TestHashGetEXBuildsCommand(t *testing.T) {
	exec := &fakeExecutor{value: []any{[]byte("one"), []byte("two")}}
	client := NewClientWithExecutor(exec)

	values, err := client.Hash().GetEX(context.Background(), "hash:1", []string{"a", "b"}, HashGetEXOptions{
		PXMilliseconds: Int64(5000),
	})

	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(values, []any{[]byte("one"), []byte("two")}) {
		t.Fatalf("unexpected values: %#v", values)
	}
	assertCall(t, exec, []any{"HGETEX", "hash:1", "PX", int64(5000), "FIELDS", 2, "a", "b"})
}

func TestHashSetEXBuildsFerricStoreCommand(t *testing.T) {
	exec := &fakeExecutor{value: int64(2)}
	client := NewClientWithExecutor(exec)

	ok, err := client.Hash().SetEX(context.Background(), "hash:1", map[string]any{
		"b": []byte("two"),
		"a": []byte("one"),
	}, HashSetEXOptions{
		EXSeconds: Int64(60),
	})

	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected HSETEX success")
	}
	assertCall(t, exec, []any{"HSETEX", "hash:1", int64(60), "a", []byte("one"), "b", []byte("two")})
}

func TestHashScanOmitsDefaultCount(t *testing.T) {
	exec := &fakeExecutor{value: []any{"0", []any{}}}
	client := NewClientWithExecutor(exec)
	count := 10

	_, err := client.Hash().Scan(context.Background(), "hash:1", 0, "", &count)

	if err != nil {
		t.Fatal(err)
	}
	assertCall(t, exec, []any{"HSCAN", "hash:1", int64(0)})
}

func TestStreamReadPreservesStreamOrder(t *testing.T) {
	exec := &fakeExecutor{value: []any{}}
	client := NewClientWithExecutor(exec)

	_, err := client.Stream().Read(context.Background(), StreamReadOptions{
		Count:   Int(10),
		BlockMS: Int64(1000),
		Streams: []StreamRef{
			{Key: "s1", ID: "0-0"},
			{Key: "s2", ID: "$"},
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	assertCall(t, exec, []any{"XREAD", "COUNT", 10, "BLOCK", int64(1000), "STREAMS", "s1", "s2", "0-0", "$"})
}

func TestListBLMPopBuildsCommand(t *testing.T) {
	exec := &fakeExecutor{value: nil}
	client := NewClientWithExecutor(exec)
	count := 2

	_, err := client.ListStore().BLMPop(context.Background(), 1.5, []string{"l1", "l2"}, "LEFT", &count)

	if err != nil {
		t.Fatal(err)
	}
	assertCall(t, exec, []any{"BLMPOP", "1.5", 2, "l1", "l2", "LEFT", "COUNT", 2})
}

func TestGeoSearchStoreBuildsCommand(t *testing.T) {
	exec := &fakeExecutor{value: int64(3)}
	client := NewClientWithExecutor(exec)
	count := 5

	n, err := client.Geo().SearchStore(context.Background(), "geo:dst", "geo:src", GeoSearchOptions{
		FromLonLat: &GeoCoordinate{Longitude: 10.1, Latitude: 20.2},
		ByRadius:   &GeoRadius{Radius: 15, Unit: "km"},
		Desc:       true,
		Count:      &count,
		Any:        true,
	}, true)

	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("expected 3, got %d", n)
	}
	assertCall(t, exec, []any{
		"GEOSEARCHSTORE", "geo:dst", "geo:src",
		"FROMLONLAT", 10.1, 20.2,
		"BYRADIUS", float64(15), "km",
		"DESC", "COUNT", 5, "ANY", "STOREDIST",
	})
}

func TestSortedSetMScoreDecodesFloats(t *testing.T) {
	exec := &fakeExecutor{value: []any{[]byte("1.5"), "2"}}
	client := NewClientWithExecutor(exec)

	scores, err := client.SortedSet().MScore(context.Background(), "z", "a", "b")

	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(scores, []float64{1.5, 2}) {
		t.Fatalf("unexpected scores: %#v", scores)
	}
	assertCall(t, exec, []any{"ZMSCORE", "z", "a", "b"})
}
