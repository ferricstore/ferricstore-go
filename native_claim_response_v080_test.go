package ferricstore

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestV080CompactClaimJobsEnforcesAggregateAttributeBudget(t *testing.T) {
	const valuesPerClaim = nativeMaxContainerItems / 2
	var body bytes.Buffer
	body.WriteByte(nativeCompactFlowClaimJobs)
	_ = binary.Write(&body, binary.BigEndian, uint32(2))
	for index := range 2 {
		writePipelineClaimForTest(&body, string(rune('a'+index)), "", "lease", int64(index+1))
		body.WriteByte(6)
		_ = binary.Write(&body, binary.BigEndian, uint32(1))
		writeCompactBinary(&body, []byte("nested"))
		body.WriteByte(5)
		_ = binary.Write(&body, binary.BigEndian, uint32(valuesPerClaim))
		body.Write(bytes.Repeat([]byte{0}, valuesPerClaim))
	}

	if _, err := decodeNativeCompactClaimJobs(body.Bytes()); err == nil {
		t.Fatal("compact claimed-job attributes exceeded the aggregate item budget")
	}
}
