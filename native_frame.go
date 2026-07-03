package ferricstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type nativeFrame struct {
	flags     byte
	laneID    uint32
	opcode    uint16
	requestID uint64
	body      []byte
}

type nativeResponse struct {
	laneID    uint32
	opcode    uint16
	requestID uint64
	status    uint16
	value     any
	err       error
}

func appendNativeResponseChunk(body, next []byte, max int) ([]byte, error) {
	if len(next) > max-len(body) {
		return nil, errors.New("ferricstore native response body is too large")
	}
	return append(body, next...), nil
}

func nativeBoundedItemCount(kind string, raw uint32, remainingBytes, minBytesPerItem int) (int, error) {
	if raw > nativeMaxContainerItems {
		return 0, fmt.Errorf("ferricstore native %s item count %d exceeds limit %d", kind, raw, nativeMaxContainerItems)
	}
	if minBytesPerItem > 0 && uint64(raw)*uint64(minBytesPerItem) > uint64(remainingBytes) {
		return 0, fmt.Errorf("ferricstore native %s item count exceeds remaining payload", kind)
	}
	return int(raw), nil
}

func readNativeFrame(reader io.Reader) (nativeFrame, error) {
	header := make([]byte, nativeHeaderLen)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nativeFrame{}, err
	}
	if string(header[0:4]) != nativeMagic {
		return nativeFrame{}, errors.New("ferricstore native response has invalid magic")
	}
	if header[4] != nativeResponseVersion {
		return nativeFrame{}, fmt.Errorf("ferricstore native response has unsupported version 0x%x", header[4])
	}
	bodyLen := binary.BigEndian.Uint32(header[20:24])
	if bodyLen > nativeMaxFrameBytes {
		return nativeFrame{}, errors.New("ferricstore native response frame is too large")
	}
	body := make([]byte, int(bodyLen))
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
