package ferricstore

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func BenchmarkDecodeNativeCompactClaimJobsAttributes(b *testing.B) {
	payload := compactClaimAttributesBenchmarkPayload(b)

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for range b.N {
		if _, err := decodeNativeCompactClaimJobs(payload); err != nil {
			b.Fatal(err)
		}
	}
}

func TestDecodeNativeCompactClaimJobsAttributesAllocationBound(t *testing.T) {
	payload := compactClaimAttributesBenchmarkPayload(t)
	allocations := testing.AllocsPerRun(1_000, func() {
		if _, err := decodeNativeCompactClaimJobs(payload); err != nil {
			panic(err)
		}
	})
	if allocations > 18 {
		t.Fatalf("compact claim attribute decode allocations = %.0f; want at most 18", allocations)
	}
}

type compactClaimTestHelper interface {
	Helper()
	Fatal(...any)
}

func compactClaimAttributesBenchmarkPayload(tb compactClaimTestHelper) []byte {
	tb.Helper()
	var claim bytes.Buffer
	claim.WriteByte(nativeCompactFlowClaimJobs)
	_ = binary.Write(&claim, binary.BigEndian, uint32(1))
	writeCompactBinary(&claim, []byte("flow-1"))
	writeCompactOptionalBinary(&claim, []byte("partition-1"))
	writeCompactBinary(&claim, []byte("lease-1"))
	_ = binary.Write(&claim, binary.BigEndian, uint64(7))
	attributes, err := encodeNativeValue(map[string]any{
		"tenant": []byte("acme"),
		"nested": map[string]any{"attempt": int64(1)},
	})
	if err != nil {
		tb.Fatal(err)
	}
	claim.Write(attributes)
	return claim.Bytes()
}
