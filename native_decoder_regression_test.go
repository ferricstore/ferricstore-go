package ferricstore

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

func TestDecodeNativeMapRejectsDuplicateKeys(t *testing.T) {
	raw := []byte{6}
	raw = appendUint32(raw, 2)
	raw = appendNativeMapEntryForTest(raw, "key", 1)
	raw = appendNativeMapEntryForTest(raw, "key", 2)

	_, _, err := decodeNativeValue(raw)
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate native map key error = %v; want explicit rejection", err)
	}
}

func TestNativeResponseCompactPayloadRequiresWireFlag(t *testing.T) {
	compact := []byte{nativeCompactKVGet, 1, 0, 0, 0, 5, 'v', 'a', 'l', 'u', 'e'}
	body := make([]byte, 2, 2+len(compact))
	body = append(body, compact...)
	frame := nativeFrame{laneID: 1, opcode: nativeOpGet, requestID: 1}

	if _, err := decodeNativeResponseFrame(frame, body, 0); err == nil {
		t.Fatal("compact response without custom-payload flag was accepted")
	}
	response, err := decodeNativeResponseFrame(frame, body, nativeFlagCustomPayload)
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := response.value.([]byte); !ok || !bytes.Equal(value, []byte("value")) {
		t.Fatalf("flagged compact response = %#v; want value bytes", response.value)
	}
	wrongOpcode := frame
	wrongOpcode.opcode = nativeOpMSet
	if _, err := decodeNativeResponseFrame(wrongOpcode, body, nativeFlagCustomPayload); err == nil {
		t.Fatal("compact GET payload was accepted for MSET opcode")
	}

	generic, err := encodeNativeValue([]byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	body = body[:2]
	body = append(body, generic...)
	if _, err := decodeNativeResponseFrame(frame, body, nativeFlagCustomPayload); err == nil {
		t.Fatal("generic response with custom-payload flag was accepted")
	}
}

func TestNativeResponseAssemblerPreservesFlagsFromEveryChunk(t *testing.T) {
	encoded, err := encodeNativeValue([]byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 2, 2+len(encoded))
	body = append(body, encoded...)
	assembler := newNativeResponseAssembler(len(body)+16, 4)
	third := len(body) / 3

	frames := []nativeFrame{
		{flags: nativeFlagMoreChunks, body: body[:third]},
		{flags: nativeFlagMoreChunks | nativeFlagCompressed, body: body[third : 2*third]},
		{body: body[2*third:]},
	}
	for index := range frames {
		frames[index].laneID = 1
		frames[index].opcode = nativeOpGet
		frames[index].requestID = 1
	}

	for _, frame := range frames[:2] {
		response, err := assembler.add(frame)
		if err != nil || response != nil {
			t.Fatalf("partial chunk = %#v, %v; want incomplete response", response, err)
		}
	}
	if response, err := assembler.add(frames[2]); err == nil || response != nil {
		t.Fatalf("completed response = %#v, %v; want retained compression flag rejection", response, err)
	}
}

func appendNativeMapEntryForTest(raw []byte, key string, value int64) []byte {
	raw = appendUint32(raw, uint32(len(key)))
	raw = append(raw, key...)
	raw = append(raw, 3)
	var integer [8]byte
	binary.BigEndian.PutUint64(integer[:], uint64(value))
	return append(raw, integer[:]...)
}

func FuzzDecodeNativeValueBounded(f *testing.F) {
	f.Add([]byte{0})
	f.Add([]byte{4, 0, 0, 0, 1, 'x'})
	f.Add([]byte{6, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, raw []byte) {
		_, _, _ = decodeNativeValueWithLimits(raw, 16, 256)
	})
}
