package ferricstore

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"strings"
	"testing"
)

func nativeHelloForTest() map[string]any {
	return nativeHelloForTestWithLimits(nil)
}

func nativeHelloForTestWithLimits(overrides map[string]any) map[string]any {
	limits := map[string]any{
		"max_frame_bytes":       int64(nativeDefaultRequestFrameBytes),
		"max_response_bytes":    int64(nativeDefaultResponseBytes),
		"max_pipeline_commands": int64(nativeDefaultPipelineCommands),
		"max_lane_queue":        int64(nativeDefaultLaneQueue),
	}
	for key, value := range overrides {
		limits[key] = value
	}
	return map[string]any{
		"protocol":      "ferricstore-native",
		"version":       int64(NativeProtocolVersion),
		"auth_required": false,
		"capabilities": map[string]any{
			"protocol_versions": []any{int64(NativeProtocolVersion)},
			"limits":            limits,
			"multiplexing": map[string]any{
				"max_lanes_per_connection": int64(1),
			},
			"flow_control": map[string]any{
				"max_inflight_per_connection": int64(nativeDefaultConnectionCredits),
				"max_inflight_per_lane":       int64(nativeDefaultLaneCredits),
			},
			"response_codecs": map[string]any{
				"typed_value": true,
				"compact_response_opcodes": map[string]any{
					"flow_claim_jobs_v1": []any{int64(nativeOpFlowClaimDue)},
					"kv_get_v1":          []any{int64(nativeOpGet)},
					"kv_mget_v1":         []any{int64(nativeOpMGet), int64(nativeOpFlowValueMGet)},
					"ok_list_v1":         []any{int64(nativeOpSet), int64(nativeOpMSet), int64(nativeOpFlowCreateMany), int64(nativeOpFlowCompleteMany)},
					"pipeline_v1":        []any{int64(nativeOpPipeline)},
				},
			},
		},
	}
}

func nativeHelloForTestWithEvents(events ...string) map[string]any {
	hello := nativeHelloForTest()
	items := make([]any, len(events))
	for index, event := range events {
		items[index] = event
	}
	hello["capabilities"].(map[string]any)["events"] = items
	return hello
}

func TestV080HelloNegotiatesResponseLimitAndCompactOpcodes(t *testing.T) {
	const advertisedOpcode = uint16(0x3333)
	hello := map[string]any{
		"protocol":      "ferricstore-native",
		"version":       int64(1),
		"auth_required": false,
		"capabilities": map[string]any{
			"protocol_versions": []any{int64(1)},
			"limits": map[string]any{
				"max_frame_bytes":       int64(4096),
				"max_response_bytes":    int64(64),
				"max_pipeline_commands": int64(8),
			},
			"response_codecs": map[string]any{
				"typed_value": true,
				"compact_response_opcodes": map[string]any{
					"kv_mget_v1": []any{int64(advertisedOpcode)},
				},
			},
		},
	}

	contract, err := parseNativeHelloContract(hello, 48)
	if err != nil {
		t.Fatal(err)
	}
	if contract.maxResponseBytes != 48 {
		t.Fatalf("max response bytes = %d, want client-capped 48", contract.maxResponseBytes)
	}

	compact := []byte{nativeCompactKVMGet, 0, 0, 0, 1, 1, 0, 0, 0, 1, 'v'}
	body := append([]byte{0, 0}, compact...)
	assembler := newNativeResponseAssembler(48, 8, contract.responseCodecs)
	response, err := assembler.add(nativeFrame{
		flags: nativeFlagCustomPayload, laneID: 7, opcode: advertisedOpcode, requestID: 9, body: body,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(response.value, []any{[]byte("v")}) {
		t.Fatalf("negotiated compact response = %#v", response.value)
	}

	assembler = newNativeResponseAssembler(48, 8, contract.responseCodecs)
	if _, err := assembler.add(nativeFrame{
		flags: nativeFlagCustomPayload, laneID: 7, opcode: nativeOpMGet, requestID: 10, body: body,
	}); err == nil {
		t.Fatal("accepted a compact MGET opcode not advertised by HELLO")
	}
}

func TestV080HelloRequiresNegotiatedCapabilities(t *testing.T) {
	_, err := parseNativeHelloContract(map[string]any{
		"protocol": "ferricstore-native", "version": int64(1), "ready": true,
	}, 64)
	if err == nil || !strings.Contains(err.Error(), "capabilities") {
		t.Fatalf("capability-less HELLO error = %v", err)
	}
}

func TestV080HelloToleratesUnrelatedFutureResponseCodecs(t *testing.T) {
	hello := nativeHelloForTest()
	table := hello["capabilities"].(map[string]any)["response_codecs"].(map[string]any)["compact_response_opcodes"].(map[string]any)
	table["future_collection_v2"] = []any{int64(0x7FFE)}

	contract, err := parseNativeHelloContract(hello, nativeDefaultResponseBytes)
	if err != nil {
		t.Fatal(err)
	}
	if _, known := contract.responseCodecs.byOpcode[0x7FFE]; known {
		t.Fatal("unknown future codec was treated as a supported decoder")
	}
	if codec := contract.responseCodecs.byOpcode[nativeOpPipeline]; codec != nativeCodecPipeline {
		t.Fatalf("known pipeline codec = %d, want %d", codec, nativeCodecPipeline)
	}
}

func TestV080ChunkAssemblyUsesFullIdentityAndAggregateLimit(t *testing.T) {
	codecs, err := parseNativeResponseCodecs(map[string]any{
		"typed_value": true,
		"compact_response_opcodes": map[string]any{
			"kv_mget_v1": []any{int64(nativeOpMGet), int64(nativeOpFlowValueMGet)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mget := compactMGetResponseBody("mget")
	flow := compactMGetResponseBody("flow")
	assembler := newNativeResponseAssembler(len(mget)+len(flow), 8, codecs)

	frames := []nativeFrame{
		{flags: nativeFlagMoreChunks | nativeFlagCustomPayload, laneID: 1, opcode: nativeOpMGet, requestID: 7, body: mget[:4]},
		{flags: nativeFlagMoreChunks | nativeFlagCustomPayload, laneID: 2, opcode: nativeOpFlowValueMGet, requestID: 7, body: flow[:5]},
		{flags: nativeFlagCustomPayload, laneID: 1, opcode: nativeOpMGet, requestID: 7, body: mget[4:]},
		{flags: nativeFlagCustomPayload, laneID: 2, opcode: nativeOpFlowValueMGet, requestID: 7, body: flow[5:]},
	}
	var values []any
	for _, frame := range frames {
		response, err := assembler.add(frame)
		if err != nil {
			t.Fatal(err)
		}
		if response != nil {
			values = append(values, response.value)
		}
	}
	if !reflect.DeepEqual(values, []any{[]any{[]byte("mget")}, []any{[]byte("flow")}}) {
		t.Fatalf("assembled values = %#v", values)
	}

	assembler = newNativeResponseAssembler(len(mget)-1, 8, codecs)
	if response, err := assembler.add(frames[0]); err != nil || response != nil {
		t.Fatalf("first oversized stream chunk = %#v, %v", response, err)
	}
	if _, err := assembler.add(frames[2]); err == nil {
		t.Fatal("chunked response exceeded negotiated aggregate limit")
	}
}

func TestV080UnauthenticatedSessionKeeps64KiBRequestLimit(t *testing.T) {
	contract := constrainNativeContractForAuthentication(
		nativeHelloContract{maxRequestFrameBytes: 4 * 1024 * 1024}, false,
	)
	if contract.maxRequestFrameBytes != nativeUnauthenticatedFrameBytes {
		t.Fatalf("unauthenticated request limit = %d, want %d", contract.maxRequestFrameBytes, nativeUnauthenticatedFrameBytes)
	}

	authenticated := constrainNativeContractForAuthentication(
		nativeHelloContract{maxRequestFrameBytes: 4 * 1024 * 1024}, true,
	)
	if authenticated.maxRequestFrameBytes != 4*1024*1024 {
		t.Fatalf("authenticated request limit = %d", authenticated.maxRequestFrameBytes)
	}
}

func TestV080NegotiatedResponseLimitRejectsFrameBeforeBodyAllocation(t *testing.T) {
	header := make([]byte, nativeHeaderLen)
	copy(header, nativeMagic)
	header[4] = nativeResponseVersion
	binary.BigEndian.PutUint32(header[20:24], 65)
	reader := bytes.NewReader(append(header, make([]byte, 65)...))
	if _, err := readNativeFrameWithLimit(reader, 64); err == nil {
		t.Fatal("accepted frame larger than negotiated response limit")
	}
	if consumed := int(reader.Size()) - reader.Len(); consumed != nativeHeaderLen {
		t.Fatalf("oversized frame consumed %d bytes, want header only", consumed)
	}
}

func compactMGetResponseBody(value string) []byte {
	body := make([]byte, 2, 12+len(value))
	body = append(body, nativeCompactKVMGet, 0, 0, 0, 1, 1, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(body[8:12], uint32(len(value)))
	return append(body, value...)
}
