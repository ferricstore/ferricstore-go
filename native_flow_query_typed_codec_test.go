package ferricstore

import (
	"encoding/binary"
	"reflect"
	"testing"
)

func TestNativeCompactFlowQueryTypedDecoderMatchesGenericPublicResult(t *testing.T) {
	for _, payload := range [][]byte{
		nativeCompactQueryPageFixture(t),
		nativeCompactQueryCursorFixture(),
		nativeCompactQueryAllTextFieldsFixture(t),
		nativeCompactQueryNilTextFieldsFixture(t),
		nativeCompactQueryCountFixture(42),
	} {
		genericMap, err := decodeNativeCompactFlowQueryResult(payload)
		if err != nil {
			t.Fatal(err)
		}
		want, err := decodeFlowQueryResult(ownedNativeFlowQueryResponse(genericMap))
		if err != nil {
			t.Fatal(err)
		}

		direct, err := decodeNativeCompactFlowQueryResultTyped(payload)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeFlowQueryResult(ownedNativeFlowQueryResult{result: direct})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("direct result differs from generic result:\n got: %#v\nwant: %#v", got, want)
		}
	}
}

func TestNativeCompactFlowQueryTypedDecoderNormalizesEveryRecordTextField(t *testing.T) {
	result, err := decodeNativeCompactFlowQueryResultTyped(nativeCompactQueryAllTextFieldsFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 1 {
		t.Fatalf("records = %d", len(result.Records))
	}
	for _, field := range flowQueryRecordTextFields {
		if value, ok := result.Records[0][field].(string); !ok || value != field+"-value" {
			t.Fatalf("record %s = %#v", field, result.Records[0][field])
		}
	}
}

func TestNativeCompactFlowQueryTypedDecoderRetainsPublicValidation(t *testing.T) {
	valid := nativeCompactQueryPageFixture(t)
	reserved := append([]byte(nil), valid...)
	binary.BigEndian.PutUint32(reserved[103:107], binary.BigEndian.Uint32(reserved[103:107])|(1<<20))
	invalidText := append([]byte(nil), valid...)
	invalidText[112] = 0xff
	wrongRecords := append([]byte(nil), valid...)
	binary.BigEndian.PutUint64(wrongRecords[62:70], 2)
	wrongBytes := append([]byte(nil), valid...)
	binary.BigEndian.PutUint64(wrongBytes[70:78], uint64(len(wrongBytes)+1))
	invalidPage := append([]byte(nil), valid...)
	invalidPage[94] = 1
	countUsageMismatch := nativeCompactQueryCountFixture(42)
	binary.BigEndian.PutUint64(countUsageMismatch[62:70], 0)

	for name, payload := range map[string][]byte{
		"reserved record field":  reserved,
		"truncated":              valid[:len(valid)-1],
		"trailing":               append(append([]byte(nil), valid...), 0),
		"invalid record text":    invalidText,
		"record usage mismatch":  wrongRecords,
		"response size mismatch": wrongBytes,
		"invalid page":           invalidPage,
		"count usage mismatch":   countUsageMismatch,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeNativeCompactFlowQueryResultTyped(payload); err == nil {
				t.Fatal("expected malformed compact query result to fail")
			}
		})
	}
}

func TestNativeFlowQueryResponseModePreservesRawExecutorBytes(t *testing.T) {
	payload := nativeCompactQueryPageFixture(t)
	body := append([]byte{0, 0}, payload...)
	frame := nativeFrame{flags: nativeFlagCustomPayload, laneID: 1, opcode: nativeOpFlowQuery, requestID: 7}
	codecs := nativeResponseCodecs{
		negotiated: true,
		byOpcode:   map[uint16]nativeCompactCodec{nativeOpFlowQuery: nativeCodecFlowQueryResult},
	}

	raw, err := decodeNativeResponseFrameWithCodecsAndMode(frame, body, frame.flags, codecs, nativeResponseDecodeGeneric)
	if err != nil {
		t.Fatal(err)
	}
	rawRecord := raw.value.(map[string]any)["records"].([]any)[0].(map[string]any)
	if _, ok := rawRecord["id"].([]byte); !ok {
		t.Fatalf("raw FLOW.QUERY id = %T, want []byte", rawRecord["id"])
	}

	typed, err := decodeNativeResponseFrameWithCodecsAndMode(frame, body, frame.flags, codecs, nativeResponseDecodeFlowQuery)
	if err != nil {
		t.Fatal(err)
	}
	result, err := decodeFlowQueryResult(typed.value)
	if err != nil {
		t.Fatal(err)
	}
	if result.Records[0]["id"] != "run-1" {
		t.Fatalf("typed FLOW.QUERY id = %#v", result.Records[0]["id"])
	}

	errorBody := append([]byte(nil), body...)
	binary.BigEndian.PutUint16(errorBody[:2], 1)
	errorResponse, err := decodeNativeResponseFrameWithCodecsAndMode(
		frame, errorBody, frame.flags, codecs, nativeResponseDecodeFlowQuery,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := errorResponse.value.(map[string]any); !ok {
		t.Fatalf("error response = %T, want generic map", errorResponse.value)
	}
}

func TestNativeFlowQueryResponseModeIsSelectedOnlyForTypedResultQueries(t *testing.T) {
	prepared, err := prepareFlowQuery("FROM runs WHERE run_id = 'run-1' RETURN RECORD", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := nativeResponseModeForPayload(newNativeFlowQueryCommand(prepared).payload); got != nativeResponseDecodeFlowQuery {
		t.Fatalf("typed query response mode = %d", got)
	}
	if got := nativeResponseModeForPayload(map[string]any{"query": prepared.query}); got != nativeResponseDecodeGeneric {
		t.Fatalf("raw query response mode = %d", got)
	}
	explain := prepared
	explain.query = "EXPLAIN " + explain.query
	if got := nativeResponseModeForPayload(newNativeFlowQueryCommand(explain).payload); got != nativeResponseDecodeGeneric {
		t.Fatalf("explain response mode = %d", got)
	}
}

func TestNativeResponseAssemblerRetainsTypedQueryModeAcrossChunks(t *testing.T) {
	payload := nativeCompactQueryPageFixture(t)
	body := append([]byte{0, 0}, payload...)
	split := len(body) / 2
	assembler := newNativeResponseAssembler(len(body), 2, nativeResponseCodecs{
		negotiated: true,
		byOpcode:   map[uint16]nativeCompactCodec{nativeOpFlowQuery: nativeCodecFlowQueryResult},
	})
	first := nativeFrame{
		flags: nativeFlagCustomPayload | nativeFlagMoreChunks, laneID: 1,
		opcode: nativeOpFlowQuery, requestID: 9, body: body[:split],
	}
	if response, err := assembler.addWithDecodeMode(first, nativeResponseDecodeFlowQuery); err != nil || response != nil {
		t.Fatalf("first chunk response=%#v err=%v", response, err)
	}
	last := first
	last.flags = nativeFlagCustomPayload
	last.body = body[split:]
	response, err := assembler.addWithDecodeMode(last, nativeResponseDecodeFlowQuery)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := response.value.(ownedNativeFlowQueryResult); !ok {
		t.Fatalf("assembled response = %T, want owned typed query result", response.value)
	}

	assembler = newNativeResponseAssembler(len(body), 2)
	if response, err := assembler.addWithDecodeMode(first, nativeResponseDecodeFlowQuery); err != nil || response != nil {
		t.Fatalf("mode-change first chunk response=%#v err=%v", response, err)
	}
	if response, err := assembler.addWithDecodeMode(last, nativeResponseDecodeGeneric); err == nil || response != nil {
		t.Fatalf("changed decode mode response=%#v err=%v", response, err)
	}
}

func TestNativeCompactFlowQueryTypedDecoderCutsAllocations(t *testing.T) {
	payload := flowQueryCompactBenchmarkPayload(t)
	genericAllocs := testing.AllocsPerRun(20, func() {
		mapping, err := decodeNativeCompactFlowQueryResult(payload)
		if err != nil {
			panic(err)
		}
		flowQueryBenchmarkResult, err = decodeFlowQueryResult(ownedNativeFlowQueryResponse(mapping))
		if err != nil {
			panic(err)
		}
	})
	typedAllocs := testing.AllocsPerRun(20, func() {
		result, err := decodeNativeCompactFlowQueryResultTyped(payload)
		if err != nil {
			panic(err)
		}
		flowQueryBenchmarkResult = result
	})
	if typedAllocs*5 >= genericAllocs*4 {
		t.Fatalf("typed allocations = %.0f, generic = %.0f; want at least 20%% reduction", typedAllocs, genericAllocs)
	}
}

func FuzzNativeCompactFlowQueryTypedDecoderMatchesGeneric(f *testing.F) {
	f.Add(nativeCompactQueryPageFixture(f))
	f.Add(nativeCompactQueryCursorFixture())
	f.Add(nativeCompactQueryCountFixture(42))
	f.Add([]byte{nativeCompactFlowQueryResult})

	f.Fuzz(func(t *testing.T, payload []byte) {
		genericMap, genericErr := decodeNativeCompactFlowQueryResult(payload)
		var want *FlowQueryResult
		if genericErr == nil {
			want, genericErr = decodeFlowQueryResult(ownedNativeFlowQueryResponse(genericMap))
		}
		got, typedErr := decodeNativeCompactFlowQueryResultTyped(payload)
		if (genericErr == nil) != (typedErr == nil) {
			t.Fatalf("generic err=%v, typed err=%v", genericErr, typedErr)
		}
		if genericErr == nil && !reflect.DeepEqual(got, want) {
			t.Fatalf("typed result differs from generic result:\n got: %#v\nwant: %#v", got, want)
		}
	})
}

func nativeCompactQueryCursorFixture() []byte {
	cursor := []byte("fqc1_cursor-token")
	payload := []byte{nativeCompactFlowQueryResult, 0, 0, 0, 0, 2}
	payload = append(payload, nativeCompactQueryUsage(0)...)
	payload = append(payload, 1)
	payload = binary.BigEndian.AppendUint32(payload, uint32(len(cursor)))
	payload = append(payload, cursor...)
	payload = binary.BigEndian.AppendUint32(payload, 0)
	binary.BigEndian.PutUint64(payload[70:78], uint64(len(payload)))
	return payload
}

func nativeCompactQueryAllTextFieldsFixture(t *testing.T) []byte {
	t.Helper()
	textIndexes := map[int]string{
		0: "id", 1: "type", 2: "state", 5: "partition_key", 11: "run_state",
		13: "parent_flow_id", 14: "root_flow_id", 15: "correlation_id",
	}
	bitmap := uint32(0)
	values := make([]byte, 0, len(textIndexes)*24)
	for index, field := range nativeCompactQueryRecordFields {
		name, present := textIndexes[index]
		if !present {
			continue
		}
		if name != field {
			t.Fatalf("compact field %d = %q, want %q", index, field, name)
		}
		bitmap |= 1 << index
		encoded, err := encodeNativeValue([]byte(field + "-value"))
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, encoded...)
	}
	payload := []byte{nativeCompactFlowQueryResult, 0, 0, 0, 0, 2}
	payload = append(payload, nativeCompactQueryUsage(1)...)
	payload = append(payload, 0, 0xff, 0xff, 0xff, 0xff)
	payload = binary.BigEndian.AppendUint32(payload, 1)
	payload = binary.BigEndian.AppendUint32(payload, bitmap)
	payload = append(payload, values...)
	binary.BigEndian.PutUint64(payload[70:78], uint64(len(payload)))
	return payload
}

func nativeCompactQueryNilTextFieldsFixture(t *testing.T) []byte {
	t.Helper()
	values := make([]byte, 0, 16)
	for _, value := range []any{[]byte("run-1"), nil, nil} {
		encoded, err := encodeNativeValue(value)
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, encoded...)
	}
	payload := []byte{nativeCompactFlowQueryResult, 0, 0, 0, 0, 2}
	payload = append(payload, nativeCompactQueryUsage(1)...)
	payload = append(payload, 0, 0xff, 0xff, 0xff, 0xff)
	payload = binary.BigEndian.AppendUint32(payload, 1)
	payload = binary.BigEndian.AppendUint32(payload, (1<<0)|(1<<11)|(1<<13))
	payload = append(payload, values...)
	binary.BigEndian.PutUint64(payload[70:78], uint64(len(payload)))
	return payload
}
