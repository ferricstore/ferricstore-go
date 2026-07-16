package ferricstore

import (
	"context"
	"testing"
)

func TestGeoSearchRejectsMalformedMetadata(t *testing.T) {
	base := GeoSearchOptions{
		FromMember: "origin",
		ByRadius:   &GeoRadius{Radius: 1, Unit: "km"},
	}
	tests := []struct {
		name      string
		response  any
		configure func(*GeoSearchOptions)
	}{
		{
			name:      "negative distance",
			response:  []any{[]any{[]byte("member"), []byte("-0.1")}},
			configure: func(opt *GeoSearchOptions) { opt.WithDist = true },
		},
		{
			name:      "non-integer hash",
			response:  []any{[]any{[]byte("member"), []byte("1.5")}},
			configure: func(opt *GeoSearchOptions) { opt.WithHash = true },
		},
		{
			name:      "negative hash",
			response:  []any{[]any{[]byte("member"), int64(-1)}},
			configure: func(opt *GeoSearchOptions) { opt.WithHash = true },
		},
		{
			name:      "hash outside 52-bit range",
			response:  []any{[]any{[]byte("member"), int64(1 << 52)}},
			configure: func(opt *GeoSearchOptions) { opt.WithHash = true },
		},
		{
			name:      "coordinate shape",
			response:  []any{[]any{[]byte("member"), []any{[]byte("1")}}},
			configure: func(opt *GeoSearchOptions) { opt.WithCoord = true },
		},
		{
			name:      "longitude range",
			response:  []any{[]any{[]byte("member"), []any{[]byte("181"), []byte("0")}}},
			configure: func(opt *GeoSearchOptions) { opt.WithCoord = true },
		},
		{
			name:     "metadata order",
			response: []any{[]any{[]byte("member"), int64(42), []byte("1.25")}},
			configure: func(opt *GeoSearchOptions) {
				opt.WithDist = true
				opt.WithHash = true
			},
		},
		{
			name: "ascending distance order",
			response: []any{
				[]any{[]byte("far"), []byte("2.0")},
				[]any{[]byte("near"), []byte("1.0")},
			},
			configure: func(opt *GeoSearchOptions) {
				opt.WithDist = true
				opt.Asc = true
			},
		},
		{
			name: "descending distance order",
			response: []any{
				[]any{[]byte("near"), []byte("1.0")},
				[]any{[]byte("far"), []byte("2.0")},
			},
			configure: func(opt *GeoSearchOptions) {
				opt.WithDist = true
				opt.Desc = true
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opt := base
			test.configure(&opt)
			store := NewClientWithExecutor(&fakeExecutor{value: test.response}).Geo()
			if _, err := store.Search(context.Background(), "geo", opt); err == nil {
				t.Fatalf("accepted malformed GEOSEARCH response %#v", test.response)
			}
		})
	}
}

func TestListPosRejectsMalformedResponses(t *testing.T) {
	countTwo := int64(2)
	countZero := int64(0)
	tests := []struct {
		name     string
		response any
		count    *int64
	}{
		{name: "negative scalar", response: int64(-1)},
		{name: "array without COUNT", response: []any{int64(0)}},
		{name: "scalar with COUNT", response: int64(0), count: &countTwo},
		{name: "negative array position", response: []any{int64(0), int64(-1)}, count: &countTwo},
		{name: "too many positions", response: []any{int64(0), int64(1), int64(2)}, count: &countTwo},
		{name: "non-integer position", response: []any{int64(0), []byte("one")}, count: &countZero},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewClientWithExecutor(&fakeExecutor{value: test.response}).ListStore()
			if _, err := store.Pos(context.Background(), "list", "value", nil, test.count, nil); err == nil {
				t.Fatalf("accepted malformed LPOS response %#v", test.response)
			}
		})
	}
}

func TestListPosAcceptsServerMissingAndCountShapes(t *testing.T) {
	count := int64(2)
	exec := &fakeExecutor{values: []any{nil, int64(3), []any{}, []any{int64(1), int64(4)}}}
	store := NewClientWithExecutor(exec).ListStore()

	for _, requestCount := range []*int64{nil, nil, &count, &count} {
		if _, err := store.Pos(context.Background(), "list", "value", nil, requestCount, nil); err != nil {
			t.Fatal(err)
		}
	}
}

func TestListPosRejectsPositionsInWrongRankOrder(t *testing.T) {
	count := int64(2)
	positiveRank := int64(1)
	negativeRank := int64(-1)
	for _, test := range []struct {
		response any
		rank     *int64
	}{
		{response: []any{int64(4), int64(1)}, rank: &positiveRank},
		{response: []any{int64(1), int64(4)}, rank: &negativeRank},
		{response: []any{int64(1), int64(1)}, rank: &positiveRank},
	} {
		store := NewClientWithExecutor(&fakeExecutor{value: test.response}).ListStore()
		if _, err := store.Pos(context.Background(), "list", "value", test.rank, &count, nil); err == nil {
			t.Fatalf("accepted LPOS positions in invalid order: %#v", test.response)
		}
	}
}

func TestGeoSearchCountRejectsExcessResults(t *testing.T) {
	count := 1
	response := []any{[]byte("one"), []byte("two")}
	store := NewClientWithExecutor(&fakeExecutor{value: response}).Geo()
	opt := GeoSearchOptions{
		FromMember: "origin",
		ByRadius:   &GeoRadius{Radius: 1, Unit: "km"},
		Count:      &count,
	}
	if _, err := store.Search(context.Background(), "geo", opt); err == nil {
		t.Fatal("GEOSEARCH accepted more results than COUNT")
	}
	if _, err := NewClientWithExecutor(&fakeExecutor{value: int64(2)}).Geo().SearchStore(
		context.Background(), "destination", "source", opt, false,
	); err == nil {
		t.Fatal("GEOSEARCHSTORE accepted count above COUNT")
	}
}
