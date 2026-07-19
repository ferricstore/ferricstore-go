package ferricstore

import (
	"context"
	"math"
	"reflect"
	"testing"
)

func TestGeoCommandsRejectInvalidArgumentsBeforeCodecOrTransport(t *testing.T) {
	tests := []struct {
		name string
		call func(*GeoStore) error
	}{
		{name: "GEOADD non-finite longitude", call: func(store *GeoStore) error {
			_, err := store.Add(context.Background(), "geo", math.NaN(), 0, "member")
			return err
		}},
		{name: "GEOADD longitude out of range", call: func(store *GeoStore) error {
			_, err := store.Add(context.Background(), "geo", 181, 0, "member")
			return err
		}},
		{name: "GEOADD latitude out of range", call: func(store *GeoStore) error {
			_, err := store.Add(context.Background(), "geo", 0, 85.05112879, "member")
			return err
		}},
		{name: "GEODIST invalid unit", call: func(store *GeoStore) error {
			_, err := store.Distance(context.Background(), "geo", "one", "two", "yards")
			return err
		}},
		{name: "GEOSEARCH invalid origin coordinate", call: func(store *GeoStore) error {
			_, err := store.Search(context.Background(), "geo", GeoSearchOptions{
				FromLonLat: &GeoCoordinate{Longitude: 200, Latitude: 0},
				ByRadius:   &GeoRadius{Radius: 1, Unit: "km"},
			})
			return err
		}},
		{name: "GEOSEARCH negative radius", call: func(store *GeoStore) error {
			_, err := store.Search(context.Background(), "geo", GeoSearchOptions{
				FromMember: "member", ByRadius: &GeoRadius{Radius: -1, Unit: "km"},
			})
			return err
		}},
		{name: "GEOSEARCH invalid box height", call: func(store *GeoStore) error {
			_, err := store.Search(context.Background(), "geo", GeoSearchOptions{
				FromMember: "member", ByBox: &GeoBox{Width: 1, Height: 0, Unit: "km"},
			})
			return err
		}},
		{name: "GEOSEARCH invalid unit", call: func(store *GeoStore) error {
			_, err := store.Search(context.Background(), "geo", GeoSearchOptions{
				FromMember: "member", ByRadius: &GeoRadius{Radius: 1, Unit: "yards"},
			})
			return err
		}},
		{name: "GEOSEARCH zero count", call: func(store *GeoStore) error {
			_, err := store.Search(context.Background(), "geo", GeoSearchOptions{
				FromMember: "member", ByRadius: &GeoRadius{Radius: 1, Unit: "km"}, Count: Int(0),
			})
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []any{}}
			codec := &countingKVCodec{}
			if err := test.call(NewClientWithExecutor(exec, WithConcurrentCodec(codec)).Geo()); err == nil {
				t.Fatal("invalid geo command succeeded")
			}
			if codec.encodes.Load() != 0 {
				t.Fatalf("invalid geo command invoked codec %d times", codec.encodes.Load())
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid geo command reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestGeoSearchDecodesMembersAndPreservesMetadata(t *testing.T) {
	payload := []byte(`{"member":1}`)
	exec := &fakeExecutor{values: []any{
		[]any{payload},
		[]any{[]any{payload, []byte("1.25"), int64(42), []any{[]byte("1"), []byte("2")}}},
	}}
	store := NewClientWithExecutor(exec, WithCodec(JSONCodec{})).Geo()

	plain, err := store.Search(context.Background(), "geo", GeoSearchOptions{
		FromMember: "origin", ByRadius: &GeoRadius{Radius: 1, Unit: "km"},
	})
	if err != nil {
		t.Fatal(err)
	}
	structured, err := store.Search(context.Background(), "geo", GeoSearchOptions{
		FromMember: "origin", ByRadius: &GeoRadius{Radius: 1, Unit: "km"},
		WithDist: true, WithHash: true, WithCoord: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	decoded := map[string]any{"member": float64(1)}
	if want := []any{decoded}; !reflect.DeepEqual(plain, want) {
		t.Fatalf("plain GEOSEARCH = %#v, want %#v", plain, want)
	}
	want := []any{[]any{decoded, []byte("1.25"), int64(42), []any{[]byte("1"), []byte("2")}}}
	if !reflect.DeepEqual(structured, want) {
		t.Fatalf("structured GEOSEARCH = %#v, want %#v", structured, want)
	}
}

func TestGeoCardinalityResponsesRejectImpossibleCounts(t *testing.T) {
	if _, err := NewClientWithExecutor(&fakeExecutor{value: int64(2)}).Geo().Add(context.Background(), "geo", 0, 0, "member"); err == nil {
		t.Fatal("GEOADD accepted count above one")
	}
	opt := GeoSearchOptions{FromMember: "member", ByRadius: &GeoRadius{Radius: 1, Unit: "m"}}
	if _, err := NewClientWithExecutor(&fakeExecutor{value: int64(-1)}).Geo().SearchStore(context.Background(), "dest", "source", opt, false); err == nil {
		t.Fatal("GEOSEARCHSTORE accepted negative count")
	}
}

func TestProbabilisticCommandsRejectInvalidArgumentsBeforeCodecOrTransport(t *testing.T) {
	zero := int64(0)
	nan := math.NaN()
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "BF.RESERVE error rate", call: func(client *Client) error {
			_, err := client.Bloom().Reserve(context.Background(), "key", 1, 10)
			return err
		}},
		{name: "BF.RESERVE capacity", call: func(client *Client) error {
			_, err := client.Bloom().Reserve(context.Background(), "key", 0.01, 0)
			return err
		}},
		{name: "CF.RESERVE capacity", call: func(client *Client) error {
			_, err := client.Cuckoo().Reserve(context.Background(), "key", 0)
			return err
		}},
		{name: "CMS.INITBYDIM width", call: func(client *Client) error {
			_, err := client.CountMinSketch().InitByDim(context.Background(), "key", 0, 1)
			return err
		}},
		{name: "CMS.INITBYPROB probability", call: func(client *Client) error {
			_, err := client.CountMinSketch().InitByProb(context.Background(), "key", 0.1, 1)
			return err
		}},
		{name: "CMS.INCRBY count", call: func(client *Client) error {
			_, err := client.CountMinSketch().IncrBy(context.Background(), "key", "item", 0)
			return err
		}},
		{name: "CMS.INCRBY many count", call: func(client *Client) error {
			_, err := client.CountMinSketch().IncrByMany(context.Background(), "key", CMSIncrement{Item: "item", Count: 0})
			return err
		}},
		{name: "CMS.MERGE empty sources", call: func(client *Client) error {
			_, err := client.CountMinSketch().Merge(context.Background(), "dest", CMSMergeOptions{})
			return err
		}},
		{name: "CMS.MERGE weight count", call: func(client *Client) error {
			_, err := client.CountMinSketch().Merge(context.Background(), "dest", CMSMergeOptions{Sources: []string{"one", "two"}, Weights: []int64{1}})
			return err
		}},
		{name: "TOPK.RESERVE k", call: func(client *Client) error {
			_, err := client.TopK().Reserve(context.Background(), "key", 0)
			return err
		}},
		{name: "TOPK.RESERVE width", call: func(client *Client) error {
			_, err := client.TopK().ReserveWithOptions(context.Background(), "key", 1, TopKReserveOptions{Width: &zero, Depth: Int64(1)})
			return err
		}},
		{name: "TOPK.INCRBY count", call: func(client *Client) error {
			_, err := client.TopK().IncrBy(context.Background(), "key", TopKIncrement{Item: "item", Count: 0})
			return err
		}},
		{name: "TDIGEST.CREATE compression", call: func(client *Client) error {
			_, err := client.TDigest().Create(context.Background(), "key", &zero)
			return err
		}},
		{name: "TDIGEST.ADD empty", call: func(client *Client) error { _, err := client.TDigest().Add(context.Background(), "key"); return err }},
		{name: "TDIGEST.ADD non-finite", call: func(client *Client) error {
			_, err := client.TDigest().Add(context.Background(), "key", nan)
			return err
		}},
		{name: "TDIGEST.QUANTILE range", call: func(client *Client) error {
			_, err := client.TDigest().Quantile(context.Background(), "key", 1.1)
			return err
		}},
		{name: "TDIGEST.CDF non-finite", call: func(client *Client) error {
			_, err := client.TDigest().CDF(context.Background(), "key", nan)
			return err
		}},
		{name: "TDIGEST.TRIMMED_MEAN order", call: func(client *Client) error {
			_, err := client.TDigest().TrimmedMean(context.Background(), "key", 0.8, 0.2)
			return err
		}},
		{name: "TDIGEST.MERGE empty sources", call: func(client *Client) error {
			_, err := client.TDigest().Merge(context.Background(), "dest", TDigestMergeOptions{})
			return err
		}},
		{name: "TDIGEST.MERGE compression", call: func(client *Client) error {
			_, err := client.TDigest().Merge(context.Background(), "dest", TDigestMergeOptions{Sources: []string{"source"}, Compression: &zero})
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			codec := &countingKVCodec{}
			if err := test.call(NewClientWithExecutor(exec, WithConcurrentCodec(codec))); err == nil {
				t.Fatal("invalid probabilistic command succeeded")
			}
			if codec.encodes.Load() != 0 {
				t.Fatalf("invalid probabilistic command invoked codec %d times", codec.encodes.Load())
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid probabilistic command reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestProbabilisticCountsRejectNegativeResponses(t *testing.T) {
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "BF.CARD", call: func(client *Client) error { _, err := client.Bloom().Card(context.Background(), "key"); return err }},
		{name: "CF.COUNT", call: func(client *Client) error {
			_, err := client.Cuckoo().Count(context.Background(), "key", "item")
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(NewClientWithExecutor(&fakeExecutor{value: int64(-1)})); err == nil {
				t.Fatal("accepted negative probabilistic count")
			}
		})
	}
}

func TestTopKListDecodesItemsWithoutDecodingCounts(t *testing.T) {
	payload := []byte(`{"item":1}`)
	exec := &fakeExecutor{values: []any{
		[]any{payload},
		[]any{payload, int64(3)},
	}}
	store := NewClientWithExecutor(exec, WithCodec(JSONCodec{})).TopK()
	plain, err := store.List(context.Background(), "key")
	if err != nil {
		t.Fatal(err)
	}
	withCount, err := store.ListWithCount(context.Background(), "key")
	if err != nil {
		t.Fatal(err)
	}
	decoded := map[string]any{"item": float64(1)}
	if !reflect.DeepEqual(plain, []any{decoded}) {
		t.Fatalf("TOPK.LIST = %#v", plain)
	}
	if !reflect.DeepEqual(withCount, []TopKEntry{{Item: decoded, Count: 3}}) {
		t.Fatalf("TOPK.LIST WITHCOUNT = %#v", withCount)
	}
}

func TestSpecializedRawDecodePathsDoNotAllocate(t *testing.T) {
	var plain any = []any{[]byte("one"), []byte("two")}
	var structured any = []any{[]any{[]byte("member"), []byte("1.5")}}
	plainGeo := testing.AllocsPerRun(1000, func() {
		if _, err := decodeGeoSearch(RawCodec{}, plain, nil, geoSearchMetadata{}); err != nil {
			panic(err)
		}
	})
	structuredGeo := testing.AllocsPerRun(1000, func() {
		if _, err := decodeGeoSearch(RawCodec{}, structured, nil, geoSearchMetadata{withDistance: true}); err != nil {
			panic(err)
		}
	})
	array := testing.AllocsPerRun(1000, func() {
		if _, err := decodeArray(RawCodec{}, plain, nil); err != nil {
			panic(err)
		}
	})
	if plainGeo != 0 || structuredGeo != 0 || array != 0 {
		t.Fatalf("raw decode allocations: plain geo=%v structured geo=%v array=%v; want all zero", plainGeo, structuredGeo, array)
	}
}
