package ferricstore

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"
)

var (
	flowQueryBenchmarkBytes  []byte
	flowQueryBenchmarkParams map[string]any
	flowQueryBenchmarkResult *FlowQueryResult
	flowQueryBenchmarkText   string
)

func BenchmarkFlowQueryNativeRequest32Parameters(b *testing.B) {
	params := make(map[string]any, 32)
	for index := 0; index < 32; index++ {
		params[fmt.Sprintf("parameter_%02d", index)] = int64(index)
	}
	query := "FROM runs WHERE partition_key = @parameter_00 ORDER BY updated_at_ms ASC LIMIT 10 RETURN RECORDS"
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		prepared, err := prepareFlowQuery(query, params)
		if err != nil {
			b.Fatal(err)
		}
		command := newNativeFlowQueryCommand(prepared)
		body, err := encodeNativeValue(command.payload)
		if err != nil {
			b.Fatal(err)
		}
		flowQueryBenchmarkBytes = body
	}
}

func BenchmarkFlowQuerySearchBuilder(b *testing.B) {
	options := SearchOptions{
		Type: "invoice", State: "queued", PartitionKey: "tenant-a", Count: Int(100),
		FromMS: Int64(100), ToMS: Int64(1_000), Rev: Bool(true),
		Attributes: map[string]any{
			"customer.region": "eu", "plan": "pro", "segment": int64(3),
		},
		StateMeta: map[string]map[string]any{
			"queued": {"attempt": int64(2), "risk": "high"},
		},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		query, params, err := buildFlowSearchQuery(options)
		if err != nil {
			b.Fatal(err)
		}
		flowQueryBenchmarkText = query
		flowQueryBenchmarkParams = params
	}
}

func BenchmarkFlowQueryListBuilder(b *testing.B) {
	options := ReadOptions{PartitionKey: "tenant-a"}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		query, params, err := buildFlowListQuery("invoice", options)
		if err != nil {
			b.Fatal(err)
		}
		flowQueryBenchmarkText = query
		flowQueryBenchmarkParams = params
	}
}

func BenchmarkFlowQueryDecode100Records(b *testing.B) {
	benchmarkFlowQueryDecode100Records(b, true)
}

func BenchmarkFlowQueryDecode100RecordsGenericExecutor(b *testing.B) {
	benchmarkFlowQueryDecode100Records(b, false)
}

func benchmarkFlowQueryDecode100Records(b *testing.B, owned bool) {
	records := make([]any, flowQueryMaxResponseRecords)
	for index := range records {
		records[index] = map[string]any{
			"id": []byte(fmt.Sprintf("run-%03d", index)), "type": []byte("invoice"), "state": []byte("queued"),
			"partition_key": []byte("tenant-a"), "updated_at_ms": int64(index),
		}
	}
	var response any = flowQueryPageResponse(records, false, nil)
	if owned {
		response = ownedNativeFlowQueryResponse(response.(map[string]any))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := decodeFlowQueryResult(response)
		if err != nil {
			b.Fatal(err)
		}
		flowQueryBenchmarkResult = result
	}
}

func BenchmarkFlowQueryCompactRoundTrip100Records(b *testing.B) {
	payload := flowQueryCompactBenchmarkPayload(b)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		mapping, err := decodeNativeCompactFlowQueryResult(payload)
		if err != nil {
			b.Fatal(err)
		}
		result, err := decodeFlowQueryResult(ownedNativeFlowQueryResponse(mapping))
		if err != nil {
			b.Fatal(err)
		}
		flowQueryBenchmarkResult = result
	}
}

func BenchmarkFlowQueryCompactTyped100Records(b *testing.B) {
	payload := flowQueryCompactBenchmarkPayload(b)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := decodeNativeCompactFlowQueryResultTyped(payload)
		if err != nil {
			b.Fatal(err)
		}
		flowQueryBenchmarkResult = result
	}
}

func BenchmarkFlowQueryCompactPaired100Records(b *testing.B) {
	payload := flowQueryCompactBenchmarkPayload(b)
	var genericDuration time.Duration
	var typedDuration time.Duration
	runGeneric := func() {
		started := time.Now()
		mapping, err := decodeNativeCompactFlowQueryResult(payload)
		if err == nil {
			flowQueryBenchmarkResult, err = decodeFlowQueryResult(ownedNativeFlowQueryResponse(mapping))
		}
		genericDuration += time.Since(started)
		if err != nil {
			b.Fatal(err)
		}
	}
	runTyped := func() {
		started := time.Now()
		result, err := decodeNativeCompactFlowQueryResultTyped(payload)
		typedDuration += time.Since(started)
		if err != nil {
			b.Fatal(err)
		}
		flowQueryBenchmarkResult = result
	}
	b.ResetTimer()
	iterations := 0
	for b.Loop() {
		if iterations&1 == 0 {
			runGeneric()
			runTyped()
		} else {
			runTyped()
			runGeneric()
		}
		iterations++
	}
	b.StopTimer()
	b.ReportMetric(float64(genericDuration.Nanoseconds())/float64(iterations), "generic-ns/op")
	b.ReportMetric(float64(typedDuration.Nanoseconds())/float64(iterations), "typed-ns/op")
}

func flowQueryCompactBenchmarkPayload(tb testing.TB) []byte {
	tb.Helper()
	payload := []byte{nativeCompactFlowQueryResult, 0, 1, 1, 0, 0}
	usage := make([]byte, 8*len(nativeCompactQueryUsageFields))
	binary.BigEndian.PutUint64(usage[2*8:3*8], flowQueryMaxResponseRecords)
	binary.BigEndian.PutUint64(usage[4*8:5*8], flowQueryMaxResponseRecords)
	binary.BigEndian.PutUint64(usage[7*8:8*8], flowQueryMaxResponseRecords)
	payload = append(payload, usage...)
	payload = append(payload, 0, 0xff, 0xff, 0xff, 0xff)
	payload = binary.BigEndian.AppendUint32(payload, flowQueryMaxResponseRecords)
	bitmap := uint32((1 << 0) | (1 << 1) | (1 << 2) | (1 << 5) | (1 << 7))
	for index := range flowQueryMaxResponseRecords {
		payload = binary.BigEndian.AppendUint32(payload, bitmap)
		for _, value := range []any{
			[]byte(fmt.Sprintf("run-%03d", index)), []byte("invoice"), []byte("queued"),
			[]byte("tenant-a"), int64(index),
		} {
			encoded, err := encodeNativeValue(value)
			if err != nil {
				tb.Fatal(err)
			}
			payload = append(payload, encoded...)
		}
	}
	binary.BigEndian.PutUint64(payload[70:78], uint64(len(payload)))
	return payload
}
