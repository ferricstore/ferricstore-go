package ferricstore

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestV080CompactFlowRecordToleratesUnknownExtensions(t *testing.T) {
	record := []byte{nativeCompactFlowRecord, 0, 0, 0, 4}
	record = appendCompactFlowField(record, 1, "", []byte("flow-1"))
	record = appendCompactFlowField(record, 3, "", []byte("failed"))
	record = appendCompactFlowField(record, 250, "", map[string]any{"future": true})
	record = appendCompactFlowField(record, 0, "max_active_ms", int64(500))

	codecs, err := parseNativeResponseCodecs(map[string]any{
		"typed_value": true,
		"compact_response_opcodes": map[string]any{
			"flow_record_v1": []any{int64(0x0202)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	body := append([]byte{0, 0}, record...)
	response, err := decodeNativeResponseFrameWithCodecs(nativeFrame{
		laneID: 1, opcode: 0x0202, requestID: 7,
	}, body, nativeFlagCustomPayload, codecs)
	if err != nil {
		t.Fatal(err)
	}
	mapping, err := nativeMap(response.value)
	if err != nil {
		t.Fatal(err)
	}
	if asString(mapping["id"]) != "flow-1" || asString(mapping["state"]) != "failed" {
		t.Fatalf("compact record = %#v", mapping)
	}
	if got := asInt64(mapping["max_active_ms"]); got != 500 {
		t.Fatalf("max_active_ms = %d, want 500", got)
	}
	if _, leaked := mapping["250"]; leaked {
		t.Fatalf("unknown numeric extension leaked into record: %#v", mapping)
	}
}

func TestV080CompactFlowRecordListEnforcesAggregateItemBudget(t *testing.T) {
	const fieldsPerRecord = nativeMaxContainerItems/2 + 1
	payload := []byte{nativeCompactFlowRecordList, 0, 0, 0, 2}
	for range 2 {
		payload = append(payload, nativeCompactFlowRecord)
		var count [4]byte
		binary.BigEndian.PutUint32(count[:], fieldsPerRecord)
		payload = append(payload, count[:]...)
		// Unknown numeric extensions still carry a typed value and must consume
		// the aggregate decoder budget even though their values are discarded.
		payload = append(payload, bytes.Repeat([]byte{250, 0}, fieldsPerRecord)...)
	}
	if _, err := decodeNativeCompactFlowRecordList(payload); err == nil {
		t.Fatal("compact Flow record list exceeded the aggregate item budget")
	}
}

func TestV080CompactFlowRecordSharesBudgetWithTypedFieldValues(t *testing.T) {
	const valuesPerField = nativeMaxContainerItems / 2
	record := []byte{nativeCompactFlowRecord, 0, 0, 0, 2}
	for fieldID := byte(250); fieldID < 252; fieldID++ {
		record = append(record, fieldID, 5)
		var count [4]byte
		binary.BigEndian.PutUint32(count[:], valuesPerField)
		record = append(record, count[:]...)
		record = append(record, bytes.Repeat([]byte{0}, valuesPerField)...)
	}
	if _, err := decodeNativeCompactFlowRecord(record); err == nil {
		t.Fatal("compact Flow typed fields exceeded the aggregate item budget")
	}
}

func TestV080FlowRecordDecodesMaxActiveFailure(t *testing.T) {
	record, err := recordFromNative(map[string]any{
		"id":            "flow-1",
		"state":         "failed",
		"max_active_ms": int64(500),
		"error": map[string]any{
			"reason":        "max_active_ms",
			"max_active_ms": int64(500),
		},
	}, RawCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if record.MaxActiveMS != 500 || record.FailureReason != "max_active_ms" {
		t.Fatalf("decoded record = %#v", record)
	}
	errorMap, ok := record.Error.(map[string]any)
	if !ok || asInt64(errorMap["max_active_ms"]) != 500 {
		t.Fatalf("decoded error = %#v", record.Error)
	}
}

func TestV080FlowRecordAcceptsInfiniteMaxActiveAsNil(t *testing.T) {
	record, err := recordFromNative(map[string]any{
		"id": "flow-infinite", "type": "order", "state": "queued", "max_active_ms": nil,
	}, RawCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if record.MaxActiveMS != 0 {
		t.Fatalf("infinite max_active_ms = %d; want zero/unbounded", record.MaxActiveMS)
	}
}

func TestV080FlowRecordRejectsOutOfContractIntegers(t *testing.T) {
	tests := []struct {
		name  string
		field string
		value int64
	}{
		{name: "fencing token", field: "fencing_token", value: maxFlowExactIntegerV080 + 1},
		{name: "version", field: "version", value: maxFlowExactIntegerV080 + 1},
		{name: "max active", field: "max_active_ms", value: maxFlowActiveMS + 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := map[string]any{"id": "flow-1", test.field: test.value}
			if _, err := recordFromNative(response, RawCodec{}); err == nil {
				t.Fatalf("accepted %s=%d outside FerricStore 0.8 contract", test.field, test.value)
			}
		})
	}
}

func TestV080FlowRecordRejectsMalformedFailureReason(t *testing.T) {
	_, err := recordFromNative(map[string]any{
		"id": "flow-1", "state": "failed", "error": map[string]any{"reason": int64(7)},
	}, RawCodec{})
	if err == nil {
		t.Fatal("accepted non-text Flow failure reason")
	}
}

func appendCompactFlowField(dst []byte, id byte, name string, value any) []byte {
	dst = append(dst, id)
	if id == 0 {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(name)))
		dst = append(dst, length[:]...)
		dst = append(dst, name...)
	}
	encoded, err := encodeNativeValue(value)
	if err != nil {
		panic(err)
	}
	return append(dst, encoded...)
}
