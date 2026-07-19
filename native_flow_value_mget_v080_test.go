package ferricstore

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestV080FlowValueMGetUsesDedicatedCompactWireSchema(t *testing.T) {
	command, err := buildNativeCommand([]any{
		"FLOW.VALUE.MGET", "ref-1", []byte("ref-2"), "MAX_BYTES", int64(64),
	})
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != 0x020C || command.laneID != 1 ||
		command.flags != nativeFlagCustomPayload {
		t.Fatalf("FLOW.VALUE.MGET native command = %#v", command)
	}
	body := encodeNativeCustomPayloadForTest(t, command.payload)
	var want bytes.Buffer
	want.WriteByte(0x9D)
	_ = binary.Write(&want, binary.BigEndian, int64(64))
	_ = binary.Write(&want, binary.BigEndian, uint32(2))
	writeCompactBinary(&want, []byte("ref-1"))
	writeCompactBinary(&want, []byte("ref-2"))
	if !bytes.Equal(body, want.Bytes()) {
		t.Fatalf("FLOW.VALUE.MGET compact body = %x, want %x", body, want.Bytes())
	}
}

func TestV080FlowValueMGetPipelineKeepsDedicatedOpcode(t *testing.T) {
	payload, flags, err := nativePipelinePayload([][]any{
		{"FLOW.VALUE.MGET", "ref-1", "ref-2", "MAX_BYTES", int64(64)},
	}, 7, nativeDefaultRequestFrameBytes)
	if err != nil {
		t.Fatal(err)
	}
	if flags != 0 {
		t.Fatalf("PIPELINE flags = %#x, want typed payload", flags)
	}
	mapping, err := nativeMap(payload)
	if err != nil {
		t.Fatal(err)
	}
	commands := mapping["commands"].([]any)
	item := commands[0].(map[string]any)
	if asInt64(item["opcode"]) != int64(nativeOpFlowValueMGet) {
		t.Fatalf("PIPELINE FLOW.VALUE.MGET opcode = %#v", item["opcode"])
	}
	body := item["body"].(map[string]any)
	refs := body["refs"].([]any)
	if len(refs) != 2 || asString(refs[0]) != "ref-1" || asString(refs[1]) != "ref-2" ||
		asInt64(body["max_bytes"]) != 64 {
		t.Fatalf("PIPELINE FLOW.VALUE.MGET body = %#v", body)
	}
}
