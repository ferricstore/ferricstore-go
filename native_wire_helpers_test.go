package ferricstore

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"
)

func readNativeRequestFrame(reader *bufio.Reader) (nativeFrame, error) {
	header := make([]byte, nativeHeaderLen)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nativeFrame{}, err
	}
	if string(header[0:4]) != nativeMagic || header[4] != nativeRequestVersion {
		return nativeFrame{}, errUnexpectedValue("request header", append([]byte(nil), header[:5]...))
	}
	bodyLen := binary.BigEndian.Uint32(header[20:24])
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(reader, body); err != nil {
		return nativeFrame{}, err
	}
	return nativeFrame{
		flags:     header[5],
		laneID:    binary.BigEndian.Uint32(header[6:10]),
		opcode:    binary.BigEndian.Uint16(header[10:12]),
		requestID: binary.BigEndian.Uint64(header[12:20]),
		body:      body,
	}, nil
}

func writeNativeTestResponse(writer *bufio.Writer, request nativeFrame, status uint16, value any) error {
	if request.opcode == nativeOpHello && status == nativeStatusOK {
		value = normalizedNativeHelloForTest(value)
	}
	valueBody, err := encodeNativeValue(value)
	if err != nil {
		return err
	}
	return writeNativeRawTestResponse(writer, request, status, valueBody)
}

// normalizedNativeHelloForTest upgrades terse transport-test fixtures to the
// mandatory FerricStore 0.8 HELLO envelope while preserving their limit
// overrides. Contract-rejection tests call parseNativeHelloContract directly.
func normalizedNativeHelloForTest(value any) any {
	mapping, err := nativeMap(value)
	if err != nil {
		return value
	}
	if _, exists := mapping["capabilities"]; exists {
		return value
	}
	hello := nativeHelloForTest()
	limits := hello["capabilities"].(map[string]any)["limits"].(map[string]any)
	for _, key := range []string{"max_frame_bytes", "max_response_bytes", "max_pipeline_commands", "max_lane_queue"} {
		if override, exists := mapping[key]; exists {
			limits[key] = override
		}
	}
	if nested, nestedErr := nativeMap(mapping["limits"]); nestedErr == nil {
		for key, override := range nested {
			limits[key] = override
		}
	}
	capabilities := hello["capabilities"].(map[string]any)
	for _, key := range []string{"multiplexing", "flow_control"} {
		if nested, nestedErr := nativeMap(mapping[key]); nestedErr == nil {
			target := capabilities[key].(map[string]any)
			for field, override := range nested {
				target[field] = override
			}
		}
	}
	for _, key := range []string{"auth_required", "protocol", "version"} {
		if override, exists := mapping[key]; exists {
			hello[key] = override
		}
	}
	return hello
}

func writeNativeRawTestResponse(writer *bufio.Writer, request nativeFrame, status uint16, valueBody []byte) error {
	return writeNativeRawTestResponseWithFlags(writer, request, status, valueBody, 0)
}

func writeNativeCompactTestResponse(writer *bufio.Writer, request nativeFrame, status uint16, valueBody []byte) error {
	return writeNativeRawTestResponseWithFlags(writer, request, status, valueBody, nativeFlagCustomPayload)
}

func writeNativeRawTestResponseWithFlags(
	writer *bufio.Writer,
	request nativeFrame,
	status uint16,
	valueBody []byte,
	flags byte,
) error {
	body := bytes.NewBuffer(make([]byte, 0, 2+len(valueBody)))
	var statusBytes [2]byte
	binary.BigEndian.PutUint16(statusBytes[:], status)
	body.Write(statusBytes[:])
	body.Write(valueBody)
	return writeNativeFrameBody(writer, request, flags, body.Bytes())
}

func writeNativeFrameBody(writer *bufio.Writer, request nativeFrame, flags byte, body []byte) error {
	header := make([]byte, nativeHeaderLen)
	copy(header[0:4], nativeMagic)
	header[4] = nativeResponseVersion
	header[5] = flags
	binary.BigEndian.PutUint32(header[6:10], request.laneID)
	binary.BigEndian.PutUint16(header[10:12], request.opcode)
	binary.BigEndian.PutUint64(header[12:20], request.requestID)
	binary.BigEndian.PutUint32(header[20:24], uint32(len(body)))

	if _, err := writer.Write(header); err != nil {
		return err
	}
	if _, err := writer.Write(body); err != nil {
		return err
	}
	return writer.Flush()
}

func errUnexpectedFrame(frame nativeFrame) error {
	return errUnexpectedValue("frame", map[string]any{
		"lane_id": frame.laneID,
		"opcode":  frame.opcode,
	})
}

func errUnexpectedValue(name string, value any) error {
	return NativeError{Status: 1, Value: map[string]any{"message": name + " unexpected: " + asString(value)}}
}

var benchmarkNativeResponseSink *nativeResponse

func BenchmarkNativeFlowControllerUncontended(b *testing.B) {
	controller := newNativeFlowController(4096, 1024, 1024)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := controller.acquire(ctx, 1); err != nil {
			b.Fatal(err)
		}
		controller.release(1)
	}
}

func BenchmarkNativeResponseAssemblerSingleFrame(b *testing.B) {
	valueBody, err := encodeNativeValue([]byte("value"))
	if err != nil {
		b.Fatal(err)
	}
	body := make([]byte, 2, 2+len(valueBody))
	binary.BigEndian.PutUint16(body, nativeStatusOK)
	body = append(body, valueBody...)
	frame := nativeFrame{laneID: 1, opcode: nativeOpGet, requestID: 1, body: body}
	assembler := newNativeResponseAssembler(nativeMaxFrameBytes, nativeMaxResponseChunkFrames)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		benchmarkNativeResponseSink, err = assembler.add(frame)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNativeResponseAssemblerChunkedBinary(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 1024*1024)
	valueBody, err := encodeNativeValue(payload)
	if err != nil {
		b.Fatal(err)
	}
	body := make([]byte, 2, 2+len(valueBody))
	binary.BigEndian.PutUint16(body, nativeStatusOK)
	body = append(body, valueBody...)
	const chunkSize = 64 * 1024
	frames := make([]nativeFrame, 0, (len(body)+chunkSize-1)/chunkSize)
	for offset := 0; offset < len(body); offset += chunkSize {
		end := min(offset+chunkSize, len(body))
		flags := byte(nativeFlagMoreChunks)
		if end == len(body) {
			flags = 0
		}
		frames = append(frames, nativeFrame{flags: flags, laneID: 1, opcode: nativeOpGet, requestID: 1, body: body[offset:end]})
	}
	assembler := newNativeResponseAssembler(nativeMaxFrameBytes, nativeMaxResponseChunkFrames)
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for _, frame := range frames {
			benchmarkNativeResponseSink, err = assembler.add(frame)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}
