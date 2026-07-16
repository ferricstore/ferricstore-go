package ferricstore

import (
	"bytes"
	"math"
	"reflect"
	"testing"
)

func FuzzDecodeNativeCompactResponses(f *testing.F) {
	for _, seed := range [][]byte{
		{nativeCompactOKList, 0, 0, 0, 0},
		{nativeCompactKVGet, 0},
		{nativeCompactKVMGet, 0, 0, 0, 0},
		{nativeCompactKVMGetFixed, 0, 0, 0, 0, 0, 0, 0, 0},
		{nativeCompactFlowClaimJobs, 0, 0, 0, 0},
		{nativeCompactPipelineResponse, 0, 0, 0, 0},
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		_, _ = decodeNativeCompactOKList(raw)
		_, _ = decodeNativeCompactKVGet(raw)
		_, _ = decodeNativeCompactKVMGet(raw)
		_, _ = decodeNativeCompactKVMGetFixed(raw)
		_, _ = decodeNativeCompactClaimJobs(raw)
		_, _ = decodeNativeCompactPipelineResponse(raw)
		for _, opcode := range []uint16{nativeOpGet, nativeOpMGet, nativeOpSet, nativeOpPipeline, nativeOpFlowClaimDue} {
			_, _, _ = decodeNativeCompactValue(opcode, raw)
		}
	})
}

func FuzzNativeValueRoundTrip(f *testing.F) {
	for _, seed := range []any{
		nil,
		int64(-1),
		[]byte("value"),
		[]any{int64(1), []byte("two")},
		map[string]any{"key": []byte("value"), "nested": []any{true, nil}},
	} {
		encoded, err := encodeNativeValue(seed)
		if err != nil {
			f.Fatalf("encode seed: %v", err)
		}
		f.Add(encoded)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		value, rest, err := decodeNativeValueWithLimits(raw, 16, 256)
		if err != nil || len(rest) != 0 {
			return
		}
		encoded, err := encodeNativeValueWithLimit(value, 1<<20)
		if err != nil {
			t.Fatalf("decoded value could not be re-encoded: %v", err)
		}
		decoded, rest, err := decodeNativeValueWithLimits(encoded, 16, 256)
		if err != nil || len(rest) != 0 {
			t.Fatalf("re-encoded value did not decode: rest=%d err=%v", len(rest), err)
		}
		if !nativeFuzzValuesEqual(value, decoded) {
			t.Fatalf("native round trip changed value\nfirst:  %#v\nsecond: %#v", value, decoded)
		}
	})
}

func nativeFuzzValuesEqual(left, right any) bool {
	switch value := left.(type) {
	case float64:
		other, ok := right.(float64)
		return ok && math.Float64bits(value) == math.Float64bits(other)
	case []byte:
		other, ok := right.([]byte)
		return ok && bytes.Equal(value, other)
	case []any:
		other, ok := right.([]any)
		if !ok || len(value) != len(other) {
			return false
		}
		for index := range value {
			if !nativeFuzzValuesEqual(value[index], other[index]) {
				return false
			}
		}
		return true
	case map[string]any:
		other, ok := right.(map[string]any)
		if !ok || len(value) != len(other) {
			return false
		}
		for key, item := range value {
			otherItem, exists := other[key]
			if !exists || !nativeFuzzValuesEqual(item, otherItem) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(left, right)
	}
}

func FuzzDecodedProtocolSurfaces(f *testing.F) {
	for _, seed := range []any{
		map[string]any{},
		[]any{},
		map[string]any{"id": []byte("flow"), "lease_token": []byte("lease"), "fencing_token": int64(1)},
		[]any{[]byte("subscribe"), []byte("channel"), int64(1)},
		[]any{[]any{[]byte("1-0"), []byte("field"), []byte("value")}},
	} {
		encoded, err := encodeNativeValue(seed)
		if err != nil {
			f.Fatalf("encode seed: %v", err)
		}
		f.Add(encoded)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		value, rest, err := decodeNativeValueWithLimits(raw, 16, 256)
		if err != nil || len(rest) != 0 {
			return
		}

		_, _ = recordFromNative(value, RawCodec{})
		_, _ = recordsFromNative(value, RawCodec{})
		_, _ = claimedItemsFromNative(value, RawCodec{})
		_, _ = scheduleResult(value, nil)
		_, _ = effectResult(value, nil)
		_, _ = approvalResult(value, nil)
		_, _ = circuitResult(value, nil)
		_, _ = budgetResult(value, nil)
		_, _ = limitResult(value, nil)
		_, _ = kvResponse(value)
		_, _ = nativeEventFromValue(value)
		_ = pubSubMessageFromNative(value)
		_, _ = parsePubSubMessage(value)
		for _, kind := range []string{"subscribe", "unsubscribe", "psubscribe", "punsubscribe"} {
			if items, ok := value.([]any); ok {
				_, _ = parsePubSubAcknowledgement(items, kind)
			}
		}

		metadataFields := 0
		if len(raw) > 0 {
			metadataFields = int(raw[0] % 4)
		}
		metadata := geoSearchMetadata{
			withDistance:    metadataFields&1 != 0,
			withHash:        metadataFields&2 != 0,
			withCoordinates: len(raw) > 1 && raw[1]&1 != 0,
		}
		_, _ = decodeGeoSearch(RawCodec{}, value, nil, metadata)
		_, _ = validateGeoPositionResponse(value, nil, metadataFields)
		_, _ = decodeStreamEntries(RawCodec{}, value, nil)
		_, _ = decodeStreamRead(RawCodec{}, value, nil)
		kind := setCollectionScan
		if len(raw) > 0 {
			kind = collectionScanKind(raw[0] % 3)
		}
		_, _ = decodeCollectionScan(RawCodec{}, value, nil, kind, "SCAN")
		_, _ = nullableFloatArray(value, nil)
		_, _ = nonFiniteFloatArray(value, nil)
		_, _ = intArray(value, nil)
		_, _ = boolArray(value, nil)

		if mapping, ok := value.(map[string]any); ok {
			_, _ = governanceOverviewFromMap(mapping)
			_, _ = buildRoutingTopology(mapping)
		}
	})
}

func FuzzParseFerricURLRoundTrip(f *testing.F) {
	for _, seed := range []string{
		"localhost",
		"localhost:6380",
		"ferric://user:pass@localhost:6380?timeout=1s",
		"ferrics://[::1]:6381",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		parsed, err := parseFerricURL(raw)
		if err != nil {
			return
		}
		roundTrip, err := parseFerricURL(parsed.RawURL)
		if err != nil {
			t.Fatalf("canonical URL %q did not parse: %v", parsed.RawURL, err)
		}
		parsed.query = nil
		roundTrip.query = nil
		// Canonical URLs always spell out their port, so whether the original
		// input omitted it is intentionally not round-trippable.
		parsed.ExplicitPort = false
		roundTrip.ExplicitPort = false
		if !reflect.DeepEqual(parsed, roundTrip) {
			t.Fatalf("URL round trip changed value\nfirst:  %#v\nsecond: %#v", parsed, roundTrip)
		}
	})
}
