package ferricstore

import (
	"strings"
	"testing"
)

func TestV080ChunkAssemblerRejectsChangingCustomPayloadFlag(t *testing.T) {
	codecs := nativeResponseCodecs{
		byOpcode:   map[uint16]nativeCompactCodec{nativeOpMGet: nativeCodecKVMGet},
		negotiated: true,
	}
	body := []byte{0, 0, nativeCompactKVMGet, 0, 0, 0, 0}
	assembler := newNativeResponseAssembler(64, 4, codecs)

	response, err := assembler.add(nativeFrame{
		flags: nativeFlagMoreChunks, laneID: 1, opcode: nativeOpMGet, requestID: 7,
		body: body[:2],
	})
	if err != nil || response != nil {
		t.Fatalf("first chunk = %#v, %v", response, err)
	}
	response, err = assembler.add(nativeFrame{
		flags: nativeFlagCustomPayload, laneID: 1, opcode: nativeOpMGet, requestID: 7,
		body: body[2:],
	})
	if err == nil || response != nil || !strings.Contains(err.Error(), "flags changed") {
		t.Fatalf("changed custom-payload flag = %#v, %v", response, err)
	}
}

func TestV080ResponseRejectsReservedFlags(t *testing.T) {
	encoded, err := encodeNativeValue(nil)
	if err != nil {
		t.Fatal(err)
	}
	body := append([]byte{0, 0}, encoded...)
	for _, flag := range []byte{0x10, 0x40, 0x80} {
		if _, err := decodeNativeResponseFrame(nativeFrame{}, body, flag); err == nil {
			t.Fatalf("reserved response flag %#x was accepted", flag)
		}
	}
}
