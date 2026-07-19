package ferricstore

import (
	"bytes"
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestKVByteRangeOperationsBypassValueCodec(t *testing.T) {
	exec := &fakeExecutor{values: []any{int64(3), []byte{0xff, 0x00, 'x'}, int64(4)}}
	client := NewClientWithExecutor(exec, WithCodec(JSONCodec{}))

	suffix := []byte{0xff, 'z'}
	if _, err := client.KV().Append(context.Background(), "key", suffix); err != nil {
		t.Fatal(err)
	}
	if got, ok := exec.calls[0][2].([]byte); !ok || !bytes.Equal(got, suffix) {
		t.Fatalf("APPEND value = %#v; want raw bytes %#v", exec.calls[0][2], suffix)
	}

	rangeValue, err := client.KV().GetRange(context.Background(), "key", 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	wantRange := []byte{0xff, 0x00, 'x'}
	if got, ok := rangeValue.([]byte); !ok || !bytes.Equal(got, wantRange) {
		t.Fatalf("GETRANGE = %#v; want raw bytes %#v", rangeValue, wantRange)
	}

	if _, err := client.KV().SetRange(context.Background(), "key", 1, "raw"); err != nil {
		t.Fatal(err)
	}
	if got := exec.calls[2][3]; got != "raw" {
		t.Fatalf("SETRANGE value = %#v; want raw string", got)
	}
}

func TestPositionalMultiGetHelpersRejectWrongCardinality(t *testing.T) {
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "MGET truncated", call: func(c *Client) error {
			_, err := c.KV().MGet(context.Background(), "a", "b")
			return err
		}},
		{name: "HMGET expanded", call: func(c *Client) error {
			_, err := c.Hash().MGet(context.Background(), "hash", "field")
			return err
		}},
		{name: "FLOW.VALUE.MGET truncated", call: func(c *Client) error {
			_, err := c.ValueMGet(context.Background(), []string{"a", "b"}, nil)
			return err
		}},
		{name: "SMISMEMBER expanded", call: func(c *Client) error {
			_, err := c.SetStore().MIsMember(context.Background(), "set", "member")
			return err
		}},
		{name: "ZMSCORE truncated", call: func(c *Client) error {
			_, err := c.SortedSet().MScore(context.Background(), "scores", "a", "b")
			return err
		}},
		{name: "BF.MEXISTS truncated", call: func(c *Client) error {
			_, err := c.Bloom().MExists(context.Background(), "filter", "a", "b")
			return err
		}},
		{name: "TDIGEST.QUANTILE expanded", call: func(c *Client) error {
			_, err := c.TDigest().Quantile(context.Background(), "digest", 0.5)
			return err
		}},
	}
	responses := []any{
		[]any{[]byte("a")},
		[]any{[]byte("a"), []byte("extra")},
		[]any{[]byte("a")},
		[]any{true, false},
		[]any{[]byte("1")},
		[]any{true},
		[]any{[]byte("1"), []byte("2")},
	}
	for index, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := NewClientWithExecutor(&fakeExecutor{value: responses[index]})
			if err := tc.call(client); err == nil {
				t.Fatal("expected response cardinality mismatch")
			}
		})
	}
}

func TestNullableScalarResponsesReturnErrNil(t *testing.T) {
	client := NewClientWithExecutor(&fakeExecutor{values: []any{nil, nil, nil}})
	if _, err := client.KV().Expire(context.Background(), "missing", 1); !errors.Is(err, ErrNil) {
		t.Fatalf("nil EXPIRE error = %v; want ErrNil", err)
	}
	if _, err := client.RandomKey(context.Background()); !errors.Is(err, ErrNil) {
		t.Fatalf("nil RANDOMKEY error = %v; want ErrNil", err)
	}
	matched, err := client.CAS(context.Background(), "key", "old", "new", nil)
	if err != nil || matched {
		t.Fatalf("nil CAS = %t, %v; want false, nil", matched, err)
	}
}

func TestTypedHelpersRejectMalformedScalarResponses(t *testing.T) {
	t.Run("integer", func(t *testing.T) {
		client := NewClientWithExecutor(&fakeExecutor{value: []byte("not-an-integer")})
		if _, err := client.KV().Incr(context.Background(), "key"); err == nil {
			t.Fatal("expected malformed integer response to fail")
		}
	})
	t.Run("boolean", func(t *testing.T) {
		client := NewClientWithExecutor(&fakeExecutor{value: map[string]any{"unexpected": true}})
		if _, err := client.KV().Expire(context.Background(), "key", 1); err == nil {
			t.Fatal("expected malformed boolean response to fail")
		}
	})
	t.Run("float", func(t *testing.T) {
		if _, err := responseFloat64([]byte("NaN-not"), nil); err == nil {
			t.Fatal("expected malformed float response to fail")
		}
	})
}

func TestKVResponseRejectsMalformedShapes(t *testing.T) {
	for _, value := range []any{
		int64(7),
		true,
		[]any{"dangling"},
		map[any]any{int64(1): "value"},
		"not-a-key-value-response",
	} {
		if parsed, err := kvResponse(value); err == nil {
			t.Fatalf("kvResponse(%#v) = %#v; want shape error", value, parsed)
		}
	}
}

func TestBooleanResponsesAcceptAllGoIntegerWidthsStrictly(t *testing.T) {
	for _, test := range []struct {
		value any
		want  bool
	}{
		{value: int8(0)},
		{value: int16(1), want: true},
		{value: int32(0)},
		{value: uint(1), want: true},
		{value: uint8(0)},
		{value: uint16(1), want: true},
		{value: uint32(0)},
		{value: uint64(1), want: true},
	} {
		got, err := responseBool(test.value, nil)
		if err != nil || got != test.want {
			t.Errorf("responseBool(%T(%v)) = %t, %v; want %t", test.value, test.value, got, err, test.want)
		}
	}
	for _, value := range []any{int8(-1), uint8(2), uint64(math.MaxUint64)} {
		if _, err := responseBool(value, nil); err == nil {
			t.Errorf("responseBool(%T(%v)) accepted a non-boolean integer", value, value)
		}
	}
}

func TestByteScalarResponseParsingDoesNotAllocate(t *testing.T) {
	integer := []byte("12345")
	boolean := []byte("true")
	if allocations := testing.AllocsPerRun(1000, func() {
		if _, err := responseInt64(integer, nil); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("integer response allocations = %v, want 0", allocations)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if _, err := responseBool(boolean, nil); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("boolean response allocations = %v, want 0", allocations)
	}
}

func TestTypedArrayHelpersRejectWrongShapesAndItems(t *testing.T) {
	client := NewClientWithExecutor(&fakeExecutor{value: []byte("scalar")})
	if _, err := client.KV().MGet(context.Background(), "key"); err == nil {
		t.Fatal("expected scalar MGET response to fail")
	}
	if _, err := decodeArray(RawCodec{}, []byte("scalar"), nil); err == nil {
		t.Fatal("expected scalar decoded array response to fail")
	}
	if _, err := intArray([]any{int64(1), []byte("bad")}, nil); err == nil {
		t.Fatal("expected malformed integer array item to fail")
	}
	if _, err := boolArray([]any{true, map[string]any{}}, nil); err == nil {
		t.Fatal("expected malformed boolean array item to fail")
	}
}

func TestNativeCommandArgumentPropagatesJSONErrors(t *testing.T) {
	command, err := buildNativeCommand([]any{"CUSTOM", func() {}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encodeNativeValue(command.payload); err == nil {
		t.Fatal("expected unsupported command argument to fail encoding")
	}
}

func TestPipelinePreservesBusyAndErrorKinds(t *testing.T) {
	for _, kind := range []string{"busy", "error"} {
		_, err := pipelineItemValue([]any{kind, "try again"})
		var nativeErr NativeError
		if !errors.As(err, &nativeErr) {
			t.Fatalf("%s item returned %T, want NativeError", kind, err)
		}
		if nativeErr.Kind != kind {
			t.Fatalf("pipeline kind = %q, want %q", nativeErr.Kind, kind)
		}
	}
}

func TestSortedSetNullableResultsRemainDistinguishable(t *testing.T) {
	client := NewClientWithExecutor(&fakeExecutor{values: []any{
		nil,
		nil,
		[]any{nil, []byte("1.5")},
	}})
	if _, err := client.SortedSet().Score(context.Background(), "scores", "missing"); !errors.Is(err, ErrNil) {
		t.Fatalf("missing ZSCORE error = %v, want ErrNil", err)
	}
	if _, err := client.SortedSet().Rank(context.Background(), "scores", "missing"); !errors.Is(err, ErrNil) {
		t.Fatalf("missing ZRANK error = %v, want ErrNil", err)
	}
	scores, err := client.SortedSet().MScore(context.Background(), "scores", "missing", "present")
	if err != nil {
		t.Fatal(err)
	}
	if len(scores) != 2 || !math.IsNaN(scores[0]) || scores[1] != 1.5 {
		t.Fatalf("nullable ZMSCORE result = %#v, want [NaN 1.5]", scores)
	}
}

func TestTDigestAcceptsDocumentedNonFiniteResults(t *testing.T) {
	client := NewClientWithExecutor(&fakeExecutor{values: []any{
		[]byte("nan"),
		[]any{[]byte("-inf"), []byte("+inf"), []byte("nan")},
	}})
	minimum, err := client.TDigest().Min(context.Background(), "empty")
	if err != nil {
		t.Fatal(err)
	}
	if !math.IsNaN(minimum) {
		t.Fatalf("TDIGEST.MIN = %v, want NaN", minimum)
	}
	values, err := client.TDigest().ByRank(context.Background(), "digest", -1, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 3 || !math.IsInf(values[0], -1) || !math.IsInf(values[1], 1) || !math.IsNaN(values[2]) {
		t.Fatalf("TDIGEST.BYRANK = %#v, want [-Inf +Inf NaN]", values)
	}
}

func TestFlowResponsesRejectMalformedCriticalFields(t *testing.T) {
	t.Run("claimed fencing token", func(t *testing.T) {
		_, err := claimedItemFromNative(map[string]any{
			"id": "job-1", "lease_token": "lease-1", "fencing_token": "not-an-integer",
		}, RawCodec{})
		if err == nil {
			t.Fatal("expected malformed fencing token to fail")
		}
	})

	t.Run("record version", func(t *testing.T) {
		_, err := recordFromMap(map[string]any{
			"id": "job-1", "version": "overflowed", "attributes": map[string]any{},
		}, RawCodec{})
		if err == nil {
			t.Fatal("expected malformed record version to fail")
		}
	})

	t.Run("record map", func(t *testing.T) {
		_, err := recordFromMap(map[string]any{
			"id": "job-1", "version": int64(1), "fencing_token": int64(2),
			"attributes": "not-a-map",
		}, RawCodec{})
		if err == nil {
			t.Fatal("expected malformed attributes map to fail")
		}
	})

	t.Run("duplicate map keys", func(t *testing.T) {
		_, err := recordFromMap(map[string]any{
			"id": "job-1", "attributes": []any{"owner", "first", "owner", "second"},
		}, RawCodec{})
		if err == nil {
			t.Fatal("expected duplicate attributes keys to fail")
		}
	})
}

func TestFlowResponsesRejectImpossibleCriticalValues(t *testing.T) {
	tests := []struct {
		name string
		call func() error
	}{
		{name: "empty record id", call: func() error {
			_, err := recordFromMap(map[string]any{"id": ""}, RawCodec{})
			return err
		}},
		{name: "negative record version", call: func() error {
			_, err := recordFromMap(map[string]any{"id": "flow", "version": int64(-1)}, RawCodec{})
			return err
		}},
		{name: "negative record fencing token", call: func() error {
			_, err := recordFromMap(map[string]any{"id": "flow", "fencing_token": int64(-1)}, RawCodec{})
			return err
		}},
		{name: "empty claimed id", call: func() error {
			_, err := claimedItemFromNative([]any{"", "partition", "lease", int64(1)}, RawCodec{})
			return err
		}},
		{name: "empty claimed lease", call: func() error {
			_, err := claimedItemFromNative([]any{"flow", "partition", "", int64(1)}, RawCodec{})
			return err
		}},
		{name: "negative claimed fencing token", call: func() error {
			_, err := claimedItemFromNative(map[string]any{
				"id": "flow", "lease_token": "lease", "fencing_token": int64(-1),
			}, RawCodec{})
			return err
		}},
		{name: "claimed fencing token exceeds exact range", call: func() error {
			_, err := claimedItemFromNative(map[string]any{
				"id": "flow", "lease_token": "lease", "fencing_token": maxFlowExactIntegerV080 + 1,
			}, RawCodec{})
			return err
		}},
		{name: "claimed tuple trailing field", call: func() error {
			_, err := claimedItemFromNative([]any{
				"flow", "partition", "lease", int64(1), "queued", map[string]any{}, "trailing",
			}, RawCodec{})
			return err
		}},
		{name: "typed claimed item", call: func() error {
			_, err := claimedItemsFromNative([]ClaimedItem{{
				ID: "flow", LeaseToken: "lease", FencingToken: -1,
			}}, RawCodec{})
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); err == nil {
				t.Fatal("impossible Flow response was accepted")
			}
		})
	}
}

func TestFlowResponsesRejectMalformedStringFields(t *testing.T) {
	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "claimed map id",
			call: func() error {
				_, err := claimedItemFromNative(map[string]any{
					"id": map[string]any{}, "lease_token": "lease", "fencing_token": int64(1),
				}, RawCodec{})
				return err
			},
		},
		{
			name: "claimed list lease",
			call: func() error {
				_, err := claimedItemFromNative([]any{"flow", "partition", map[string]any{}, int64(1)}, RawCodec{})
				return err
			},
		},
		{
			name: "record state",
			call: func() error {
				_, err := recordFromMap(map[string]any{
					"id": "flow", "type": "type", "state": []any{"queued"},
				}, RawCodec{})
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); err == nil {
				t.Fatal("malformed string field was silently coerced")
			}
		})
	}
}

func TestNativeMapRejectsNonStringKeysRecursively(t *testing.T) {
	for _, value := range []any{
		map[interface{}]interface{}{int64(1): "value"},
		[]any{int64(1), "value"},
		[]any{[]any{true, "value"}},
		map[string]any{"nested": map[interface{}]interface{}{int64(1): "value"}},
	} {
		if _, err := nativeMap(value); err == nil {
			t.Errorf("nativeMap accepted non-string map key in %#v", value)
		}
	}
}

func TestNativeMapRejectsExcessiveNesting(t *testing.T) {
	var value any = "leaf"
	for range nativeMaxDecodeDepth + 2 {
		value = map[string]any{"next": value}
	}
	if _, err := nativeMap(value); err == nil {
		t.Fatal("nativeMap accepted a response beyond the protocol nesting limit")
	}
}

func TestNativeMapRejectsAggregateContainerOverflow(t *testing.T) {
	leaf := make([]any, nativeMaxContainerItems/2+1)
	value := map[string]any{"items": []any{leaf, leaf}}
	if _, err := nativeMap(value); err == nil {
		t.Fatal("nativeMap accepted a response beyond the aggregate item limit")
	}
}

func TestEmptySetSingleMemberOperationsReturnNil(t *testing.T) {
	client := NewClientWithExecutor(&fakeExecutor{values: []any{nil, nil}})
	for name, call := range map[string]func() ([]any, error){
		"SRANDMEMBER": func() ([]any, error) {
			return client.SetStore().RandMember(context.Background(), "empty", nil)
		},
		"SPOP": func() ([]any, error) {
			return client.SetStore().Pop(context.Background(), "empty", nil)
		},
	} {
		got, err := call()
		if err != nil {
			t.Fatalf("%s empty result error = %v", name, err)
		}
		if got != nil {
			t.Fatalf("%s empty result = %#v, want nil", name, got)
		}
	}
}

func TestNullableDecodedArraysPreserveProtocolNulls(t *testing.T) {
	client := NewClientWithExecutor(&fakeExecutor{values: []any{
		[]any{nil, []byte("value")},
		[]any{nil, []byte("field-value")},
	}}, WithCodec(StringCodec{}))

	values, err := client.KV().MGet(context.Background(), "missing", "present")
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 || values[0] != nil || values[1] != "value" {
		t.Fatalf("MGET nullable values = %#v", values)
	}
	fields, err := client.Hash().MGet(context.Background(), "hash", "missing", "present")
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 2 || fields[0] != nil || fields[1] != "field-value" {
		t.Fatalf("HMGET nullable values = %#v", fields)
	}
}

func TestAdminResponsesPreserveDocumentedShape(t *testing.T) {
	normalized, err := normalizeAdminResponse(map[string]any{
		"name":  []byte("cluster-a"),
		"nodes": []any{map[interface{}]interface{}{"id": []byte("node-1")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"name":  "cluster-a",
		"nodes": []any{map[string]any{"id": "node-1"}},
	}
	if !reflect.DeepEqual(normalized, want) {
		t.Fatalf("normalized admin response = %#v, want %#v", normalized, want)
	}
	if _, err := adminArrayResponse(map[string]any{"unexpected": true}); err == nil {
		t.Fatal("admin list silently accepted a record response")
	}
}
