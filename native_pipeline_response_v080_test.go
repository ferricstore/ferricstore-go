package ferricstore

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestV080CompactPipelineDecodesAllNegotiatedValueShapes(t *testing.T) {
	record := []byte{nativeCompactFlowRecord, 0, 0, 0, 4}
	record = appendCompactFlowField(record, 1, "", []byte("flow-1"))
	record = appendCompactFlowField(record, 2, "", []byte("email"))
	record = appendCompactFlowField(record, 3, "", []byte("queued"))
	record = appendCompactFlowField(record, 4, "", int64(7))

	var body bytes.Buffer
	body.WriteByte(nativeCompactPipelineResponse)
	_ = binary.Write(&body, binary.BigEndian, uint32(5))
	body.Write([]byte{0, 2})
	body.Write(record)
	body.Write([]byte{0, 3, nativeCompactFlowRecordList})
	_ = binary.Write(&body, binary.BigEndian, uint32(1))
	body.Write(record)
	body.Write([]byte{0, 5})
	writeCompactBinary(&body, []byte("flow/value/ref-1"))
	writeCompactOptionalBinary(&body, []byte("tenant-a"))
	writeCompactOptionalBinary(&body, []byte("owner-1"))
	body.Write([]byte{0, 6})
	_ = binary.Write(&body, binary.BigEndian, uint32(2))
	writeCompactBinary(&body, []byte("a"))
	writeCompactBinary(&body, []byte("bb"))
	body.Write([]byte{0, 7})
	_ = binary.Write(&body, binary.BigEndian, uint32(2))
	writeCompactBinary(&body, []byte("alpha"))
	writeCompactBinary(&body, []byte("one"))
	writeCompactBinary(&body, []byte("beta"))
	writeCompactBinary(&body, []byte("two"))

	items, err := decodeNativeCompactPipelineResponse(body.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 5 {
		t.Fatalf("pipeline item count = %d, want 5", len(items))
	}
	recordValue := pipelineOKValue(t, items[0]).(map[string]any)
	if asString(recordValue["id"]) != "flow-1" || asInt64(recordValue["version"]) != 7 {
		t.Fatalf("Flow record = %#v", recordValue)
	}
	records := pipelineOKValue(t, items[1]).([]any)
	if len(records) != 1 || asString(records[0].(map[string]any)["id"]) != "flow-1" {
		t.Fatalf("Flow record list = %#v", records)
	}
	ref := pipelineOKValue(t, items[2]).(map[string]any)
	if asString(ref["ref"]) != "flow/value/ref-1" || asString(ref["partition_key"]) != "tenant-a" || asString(ref["owner_flow_id"]) != "owner-1" {
		t.Fatalf("Flow value ref = %#v", ref)
	}
	values := pipelineOKValue(t, items[3]).([]any)
	if len(values) != 2 || asString(values[0]) != "a" || asString(values[1]) != "bb" {
		t.Fatalf("binary list = %#v", values)
	}
	mapping := pipelineOKValue(t, items[4]).(map[string]any)
	if asString(mapping["alpha"]) != "one" || asString(mapping["beta"]) != "two" {
		t.Fatalf("binary map = %#v", mapping)
	}
}

func TestV080CompactPipelineDecodesAdjacentClaimedJobLayouts(t *testing.T) {
	var body bytes.Buffer
	body.WriteByte(nativeCompactPipelineResponse)
	_ = binary.Write(&body, binary.BigEndian, uint32(2))
	body.Write([]byte{0, 4})
	writePipelineClaimForTest(&body, "flow-1", "tenant-a", "lease-1", 7)
	body.Write([]byte{0, 4})
	writePipelineClaimForTest(&body, "flow-2", "", "lease-2", 8)
	writeCompactOptionalBinary(&body, []byte("retrying"))
	attributes, err := encodeNativeValue(map[string]any{"attempt": int64(2)})
	if err != nil {
		t.Fatal(err)
	}
	body.Write(attributes)

	items, err := decodeNativeCompactPipelineResponse(body.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	first, err := claimedItemFromNative(pipelineOKValue(t, items[0]), RawCodec{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := claimedItemFromNative(pipelineOKValue(t, items[1]), RawCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != "flow-1" || first.PartitionKey != "tenant-a" || first.LeaseToken != "lease-1" || first.FencingToken != 7 {
		t.Fatalf("base claimed job = %#v", first)
	}
	if second.ID != "flow-2" || second.RunState != "retrying" || asInt64(second.Attributes["attempt"]) != 2 {
		t.Fatalf("extended claimed job = %#v", second)
	}
}

func TestV080CompactPipelineEnforcesAggregateNestedItemBudget(t *testing.T) {
	const valuesPerItem = nativeMaxContainerItems / 2
	var body bytes.Buffer
	body.WriteByte(nativeCompactPipelineResponse)
	_ = binary.Write(&body, binary.BigEndian, uint32(2))
	for range 2 {
		body.Write([]byte{0, 6})
		_ = binary.Write(&body, binary.BigEndian, uint32(valuesPerItem))
		for range valuesPerItem {
			_ = binary.Write(&body, binary.BigEndian, uint32(0))
		}
	}

	if _, err := decodeNativeCompactPipelineResponse(body.Bytes()); err == nil {
		t.Fatal("compact pipeline nested values exceeded the aggregate item budget")
	}
}

func TestV080NegotiatedPipelineDecodesValuesOnlyCollectionMarkers(t *testing.T) {
	codecs, err := parseNativeResponseCodecs(map[string]any{
		"typed_value": true,
		"compact_response_opcodes": map[string]any{
			"pipeline_v1": []any{int64(nativeOpPipeline)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("integers", func(t *testing.T) {
		payload := []byte{0x88, 0, 0, 0, 2}
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], uint64(int64(7)))
		payload = append(payload, encoded[:]...)
		binary.BigEndian.PutUint64(encoded[:], uint64(int64(9)))
		payload = append(payload, encoded[:]...)
		results := decodePipelineValuesForTest(t, codecs, payload, 2)
		if results[0].err != nil || results[0].value != int64(7) || results[1].value != int64(9) {
			t.Fatalf("integer results = %#v", results)
		}
	})

	t.Run("binary lists preserve two element values", func(t *testing.T) {
		var payload bytes.Buffer
		payload.WriteByte(0x86)
		_ = binary.Write(&payload, binary.BigEndian, uint32(2))
		_ = binary.Write(&payload, binary.BigEndian, uint32(2))
		writeCompactBinary(&payload, []byte("a"))
		writeCompactBinary(&payload, []byte("b"))
		_ = binary.Write(&payload, binary.BigEndian, uint32(0))
		results := decodePipelineValuesForTest(t, codecs, payload.Bytes(), 2)
		first, ok := results[0].value.([]any)
		if results[0].err != nil || !ok || len(first) != 2 || asString(first[0]) != "a" || asString(first[1]) != "b" {
			t.Fatalf("binary-list results = %#v", results)
		}
		if second, ok := results[1].value.([]any); results[1].err != nil || !ok || len(second) != 0 {
			t.Fatalf("empty binary-list result = %#v", results[1])
		}
	})

	t.Run("binary maps preserve status fields as data", func(t *testing.T) {
		var payload bytes.Buffer
		payload.WriteByte(0x87)
		_ = binary.Write(&payload, binary.BigEndian, uint32(1))
		_ = binary.Write(&payload, binary.BigEndian, uint32(1))
		writeCompactBinary(&payload, []byte("status"))
		writeCompactBinary(&payload, []byte("domain-value"))
		results := decodePipelineValuesForTest(t, codecs, payload.Bytes(), 1)
		mapping, ok := results[0].value.(map[string]any)
		if results[0].err != nil || !ok || asString(mapping["status"]) != "domain-value" {
			t.Fatalf("binary-map result = %#v", results[0])
		}
	})

	t.Run("Flow record lists preserve status extensions as data", func(t *testing.T) {
		record := []byte{nativeCompactFlowRecord, 0, 0, 0, 2}
		record = appendCompactFlowField(record, 1, "", []byte("flow-1"))
		record = appendCompactFlowField(record, 0, "status", []byte("domain-value"))
		payload := []byte{nativeCompactFlowRecordList, 0, 0, 0, 1}
		payload = append(payload, record...)
		results := decodePipelineValuesForTest(t, codecs, payload, 1)
		mapping, ok := results[0].value.(map[string]any)
		if results[0].err != nil || !ok || asString(mapping["id"]) != "flow-1" || asString(mapping["status"]) != "domain-value" {
			t.Fatalf("Flow-record result = %#v", results[0])
		}
	})

	t.Run("claimed jobs", func(t *testing.T) {
		var payload bytes.Buffer
		payload.WriteByte(nativeCompactFlowClaimJobs)
		_ = binary.Write(&payload, binary.BigEndian, uint32(1))
		writePipelineClaimForTest(&payload, "flow-1", "tenant-a", "lease-1", 7)
		results := decodePipelineValuesForTest(t, codecs, payload.Bytes(), 1)
		claim, ok := results[0].value.(ClaimedItem)
		if results[0].err != nil || !ok || claim.ID != "flow-1" || claim.FencingToken != 7 {
			t.Fatalf("claimed-job result = %#v", results[0])
		}
	})
}

func decodePipelineValuesForTest(t *testing.T, codecs nativeResponseCodecs, payload []byte, expected int) []pipelineItemResult {
	t.Helper()
	value, compact, err := decodeNativeCompactValueWithCodecs(nativeOpPipeline, payload, codecs)
	if err != nil {
		t.Fatal(err)
	}
	if !compact {
		t.Fatalf("pipeline marker 0x%x was not accepted from HELLO negotiation", payload[0])
	}
	results, err := pipelineItemResults(value, expected)
	if err != nil {
		t.Fatal(err)
	}
	return results
}

func pipelineOKValue(t *testing.T, item any) any {
	t.Helper()
	pair, ok := item.([]any)
	if !ok || len(pair) != 2 || asString(pair[0]) != "ok" {
		t.Fatalf("pipeline item = %#v, want [ok, value]", item)
	}
	return pair[1]
}

func writePipelineClaimForTest(buf *bytes.Buffer, id, partition, lease string, fencing int64) {
	writeCompactBinary(buf, []byte(id))
	if partition == "" {
		writeCompactOptionalBinary(buf, nil)
	} else {
		writeCompactOptionalBinary(buf, []byte(partition))
	}
	writeCompactBinary(buf, []byte(lease))
	_ = binary.Write(buf, binary.BigEndian, uint64(fencing))
}
