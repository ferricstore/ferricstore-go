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
	flags     byte
	laneID    uint32
	opcode    uint16
	requestID uint64
	status    uint16
	value     any
	err       error
	wireBytes int
}

const nativeMaxResponseChunkFrames = 65_536

type nativeResponseChunkKey struct {
	requestID uint64
	opcode    uint16
	laneID    uint32
}

type nativeResponseChunkState struct {
	first  nativeFrame
	parts  [][]byte
	body   []byte
	bytes  int
	frames int
}

type nativeResponseAssembler struct {
	maxBytes       int
	maxFrames      int
	chunks         map[nativeResponseChunkKey]*nativeResponseChunkState
	bufferedBytes  int
	bufferedFrames int
}

func newNativeResponseAssembler(maxBytes, maxFrames int) *nativeResponseAssembler {
	return &nativeResponseAssembler{
		maxBytes:  maxBytes,
		maxFrames: maxFrames,
		chunks:    make(map[nativeResponseChunkKey]*nativeResponseChunkState),
	}
}

func (a *nativeResponseAssembler) add(frame nativeFrame) (*nativeResponse, error) {
	if a.maxBytes <= 0 || len(frame.body) > a.maxBytes {
		return nil, errors.New("ferricstore native response body is too large")
	}
	if a.maxFrames <= 0 {
		return nil, errors.New("ferricstore native response chunk frame limit is invalid")
	}
	key := nativeResponseChunkKey{requestID: frame.requestID, opcode: frame.opcode, laneID: frame.laneID}
	state := a.chunks[key]
	more := frame.flags&nativeFlagMoreChunks != 0
	if state == nil && !more {
		if len(frame.body) > a.maxBytes-a.bufferedBytes {
			return nil, errors.New("ferricstore native buffered chunk responses are too large")
		}
		if a.bufferedFrames >= a.maxFrames {
			return nil, fmt.Errorf("ferricstore native buffered chunk responses exceed %d frames", a.maxFrames)
		}
		response, err := decodeNativeResponseFrame(frame, frame.body, frame.flags)
		if err != nil {
			return nil, err
		}
		return &response, nil
	}
	if state == nil {
		first := frame
		first.body = nil
		state = &nativeResponseChunkState{first: first}
		a.chunks[key] = state
	}
	state.first.flags |= frame.flags
	if len(frame.body) > a.maxBytes-state.bytes {
		return nil, errors.New("ferricstore native response body is too large")
	}
	if state.frames >= a.maxFrames {
		return nil, fmt.Errorf("ferricstore native chunked response exceeds %d frames", a.maxFrames)
	}
	if len(frame.body) > a.maxBytes-a.bufferedBytes {
		return nil, errors.New("ferricstore native buffered chunk responses are too large")
	}
	if a.bufferedFrames >= a.maxFrames {
		return nil, fmt.Errorf("ferricstore native buffered chunk responses exceed %d frames", a.maxFrames)
	}
	if state.body != nil {
		var err error
		state.body, err = appendNativeResponseChunk(state.body, frame.body, a.maxBytes)
		if err != nil {
			return nil, err
		}
	} else {
		state.parts = append(state.parts, frame.body)
	}
	state.bytes += len(frame.body)
	state.frames++
	a.bufferedBytes += len(frame.body)
	a.bufferedFrames++
	if state.body == nil && state.bytes >= a.compactionThreshold() {
		state.compact(a.maxBytes)
	}
	if more {
		return nil, nil
	}

	delete(a.chunks, key)
	a.bufferedBytes -= state.bytes
	a.bufferedFrames -= state.frames
	body := state.body
	if body == nil {
		body = make([]byte, state.bytes)
		offset := 0
		for _, part := range state.parts {
			offset += copy(body[offset:], part)
		}
	}
	flags := (state.first.flags | frame.flags) &^ nativeFlagMoreChunks
	response, err := decodeNativeResponseFrame(state.first, body, flags)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

func (a *nativeResponseAssembler) compactionThreshold() int {
	return max(1, a.maxBytes/2)
}

func (s *nativeResponseChunkState) compact(maxBytes int) {
	capacity := s.bytes
	if capacity <= maxBytes/2 {
		capacity *= 2
	} else {
		capacity = maxBytes
	}
	body := make([]byte, s.bytes, capacity)
	offset := 0
	for _, part := range s.parts {
		offset += copy(body[offset:], part)
	}
	s.body = body
	s.parts = nil
}

func appendNativeResponseChunk(body, next []byte, max int) ([]byte, error) {
	if len(next) > max-len(body) {
		return nil, errors.New("ferricstore native response body is too large")
	}
	required := len(body) + len(next)
	if required > cap(body) {
		capacity := max
		if cap(body) <= max/2 {
			capacity = cap(body) * 2
		}
		if capacity < required {
			capacity = required
		}
		if capacity > max {
			capacity = max
		}
		grown := make([]byte, len(body), capacity)
		copy(grown, body)
		body = grown
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
