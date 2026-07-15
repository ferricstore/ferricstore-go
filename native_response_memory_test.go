package ferricstore

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestNativeResponseAssemblerCompactsLargeChunksBeforeFinalFrame(t *testing.T) {
	want := bytes.Repeat([]byte("a"), 128)
	encoded, err := encodeNativeValue(want)
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 2, 2+len(encoded))
	binary.BigEndian.PutUint16(body, nativeStatusOK)
	body = append(body, encoded...)
	split := len(body) * 2 / 3
	firstBody := append([]byte(nil), body[:split]...)
	finalBody := append([]byte(nil), body[split:]...)
	assembler := newNativeResponseAssembler(len(body)+16, 4)

	response, err := assembler.add(nativeFrame{
		flags: nativeFlagMoreChunks, laneID: 1, opcode: nativeOpGet, requestID: 1, body: firstBody,
	})
	if err != nil || response != nil {
		t.Fatalf("first chunk = %#v, %v; want incomplete response", response, err)
	}
	firstBody[16] = 'z'
	response, err = assembler.add(nativeFrame{
		laneID: 1, opcode: nativeOpGet, requestID: 1, body: finalBody,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, ok := response.value.([]byte)
	if !ok {
		t.Fatalf("chunked response value = %T; want []byte", response.value)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("large chunk storage retained every original frame until final assembly")
	}
}
