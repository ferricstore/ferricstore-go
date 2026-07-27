package ferricstore

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"reflect"
	"strconv"
	"testing"
)

func TestNativeCompactFlowQueryResultMatchesSharedServerGoldenCorpus(t *testing.T) {
	type vector struct {
		Name         string `json:"name"`
		PayloadHex   string `json:"payload_hex"`
		Kind         string `json:"kind"`
		CountDecimal string `json:"count_decimal"`
	}
	type corpus struct {
		Tag           byte     `json:"tag"`
		RecordFields  []string `json:"record_fields"`
		QualityFields []string `json:"quality_fields"`
		UsageFields   []string `json:"usage_fields"`
		Vectors       []vector `json:"vectors"`
	}

	data, err := os.ReadFile("testdata/flow_query_result_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var golden corpus
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatal(err)
	}
	if golden.Tag != nativeCompactFlowQueryResult ||
		!reflect.DeepEqual(golden.RecordFields, nativeCompactQueryRecordFields[:]) ||
		!reflect.DeepEqual(golden.QualityFields, nativeCompactQueryQualityFields[:]) ||
		!reflect.DeepEqual(golden.UsageFields, nativeCompactQueryUsageFields[:]) {
		t.Fatalf("compact query result schema drifted from shared corpus: %#v", golden)
	}

	for _, vector := range golden.Vectors {
		t.Run(vector.Name, func(t *testing.T) {
			payload, err := hex.DecodeString(vector.PayloadHex)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := decodeNativeCompactFlowQueryResult(payload)
			if err != nil {
				t.Fatal(err)
			}
			switch vector.Kind {
			case "records":
				records := decoded["records"].([]any)
				want := map[string]any{
					"id": []byte("run-1"), "state": []byte("failed"),
					"fields": map[string]any{"invoice_total": int64(42)},
				}
				if len(records) != 1 || !reflect.DeepEqual(records[0], want) {
					t.Fatalf("records = %#v", records)
				}
			case "count":
				if vector.CountDecimal != strconv.FormatInt(math.MaxInt64, 10) {
					t.Fatalf("count fixture = %q", vector.CountDecimal)
				}
				result := decoded["result"].(map[string]any)
				if result["value"] != int64(math.MaxInt64) {
					t.Fatalf("count = %#v", result["value"])
				}
			default:
				t.Fatalf("unknown golden vector kind %q", vector.Kind)
			}
		})
	}
}

func TestNativeCompactFlowQueryResultDecodesProjectedPage(t *testing.T) {
	payload := nativeCompactQueryPageFixture(t)
	codecs := nativeResponseCodecs{
		negotiated: true,
		byOpcode:   map[uint16]nativeCompactCodec{nativeOpFlowQuery: nativeCodecFlowQueryResult},
	}
	value, found, err := decodeNativeCompactValueWithCodecs(nativeOpFlowQuery, payload, codecs)
	if err != nil || !found {
		t.Fatalf("decode compact query page: found=%v err=%v", found, err)
	}
	mapping := value.(map[string]any)
	if string(mapping["version"].([]byte)) != flowQueryResultContract {
		t.Fatalf("version = %#v", mapping["version"])
	}
	wantRecord := map[string]any{
		"id":     []byte("run-1"),
		"state":  []byte("failed"),
		"fields": map[string]any{"invoice_total": int64(42)},
	}
	if records := mapping["records"].([]any); len(records) != 1 || !reflect.DeepEqual(records[0], wantRecord) {
		t.Fatalf("records = %#v", records)
	}
	usage := mapping["usage"].(map[string]any)
	if usage["result_records"] != int64(1) || usage["response_bytes"] != int64(len(payload)) {
		t.Fatalf("usage = %#v", usage)
	}
}

func TestNativeCompactFlowQueryResultDecodesCountAtSignedLimit(t *testing.T) {
	payload := nativeCompactQueryCountFixture(math.MaxInt64)
	codecs := nativeResponseCodecs{
		negotiated: true,
		byOpcode:   map[uint16]nativeCompactCodec{nativeOpCommandExec: nativeCodecFlowQueryResult},
	}
	value, found, err := decodeNativeCompactValueWithCodecs(nativeOpCommandExec, payload, codecs)
	if err != nil || !found {
		t.Fatalf("decode compact query count: found=%v err=%v", found, err)
	}
	result := value.(map[string]any)["result"].(map[string]any)
	if result["value"] != int64(math.MaxInt64) {
		t.Fatalf("count = %#v", result["value"])
	}
}

func TestNativeCompactFlowQueryResultRejectsReservedTruncatedAndTrailingData(t *testing.T) {
	valid := nativeCompactQueryPageFixture(t)
	reserved := append([]byte(nil), valid...)
	binary.BigEndian.PutUint32(reserved[103:107], binary.BigEndian.Uint32(reserved[103:107])|(1<<20))
	hydratedBeyondScan := append([]byte(nil), valid...)
	binary.BigEndian.PutUint64(hydratedBeyondScan[38:46], 2)
	wrongRecords := append([]byte(nil), valid...)
	binary.BigEndian.PutUint64(wrongRecords[62:70], 2)
	countUsageMismatch := nativeCompactQueryCountFixture(42)
	binary.BigEndian.PutUint64(countUsageMismatch[62:70], 0)
	for name, payload := range map[string][]byte{
		"reserved":              reserved,
		"truncated":             valid[:len(valid)-1],
		"trailing":              append(append([]byte(nil), valid...), 0),
		"inconsistent usage":    hydratedBeyondScan,
		"record usage mismatch": wrongRecords,
		"count usage mismatch":  countUsageMismatch,
		"invalid cursor prefix": nativeCompactQueryCursorFixtureWith("other_cursor_token"),
		"invalid cursor text": nativeCompactQueryCursorFixtureWith(
			"fqc1_" + string([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}),
		),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeNativeCompactFlowQueryResult(payload); err == nil {
				t.Fatal("expected malformed compact query result to fail")
			}
		})
	}
}

func TestNativeHelloRequestsQueryCodecWithoutBroadCompactFlowResponses(t *testing.T) {
	payload := nativeHelloPayload("test-client")
	if payload["compact_flow_responses"] != false {
		t.Fatalf("compact_flow_responses = %#v", payload["compact_flow_responses"])
	}
	want := []any{"flow_query_result_v1"}
	if !reflect.DeepEqual(payload["compact_response_codecs"], want) {
		t.Fatalf("compact_response_codecs = %#v, want %#v", payload["compact_response_codecs"], want)
	}
}

func nativeCompactQueryPageFixture(tb testing.TB) []byte {
	tb.Helper()
	values := make([]byte, 0)
	for _, value := range []any{[]byte("run-1"), []byte("failed"), map[string]any{"invoice_total": int64(42)}} {
		encoded, err := encodeNativeValue(value)
		if err != nil {
			tb.Fatal(err)
		}
		values = append(values, encoded...)
	}
	payload := []byte{0xA0, 0, 0, 0, 0, 2}
	payload = append(payload, nativeCompactQueryUsage(1)...)
	payload = append(payload, 0, 0xff, 0xff, 0xff, 0xff)
	payload = binary.BigEndian.AppendUint32(payload, 1)
	payload = binary.BigEndian.AppendUint32(payload, (1<<0)|(1<<2)|(1<<19))
	payload = append(payload, values...)
	binary.BigEndian.PutUint64(payload[70:78], uint64(len(payload)))
	return payload
}

func nativeCompactQueryCountFixture(count int64) []byte {
	payload := []byte{0xA0, 1, 2, 1, 0, 0}
	payload = append(payload, nativeCompactQueryUsage(1)...)
	payload = binary.BigEndian.AppendUint64(payload, uint64(count))
	binary.BigEndian.PutUint64(payload[70:78], uint64(len(payload)))
	return payload
}

func nativeCompactQueryUsage(resultRecords int64) []byte {
	values := make([]int64, 11)
	values[2] = resultRecords
	values[4] = resultRecords
	values[7] = resultRecords
	payload := make([]byte, 0, len(values)*8)
	for _, value := range values {
		payload = binary.BigEndian.AppendUint64(payload, uint64(value))
	}
	return payload
}
