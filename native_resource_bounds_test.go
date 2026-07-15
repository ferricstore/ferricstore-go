package ferricstore

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

func TestNativeResponseAssemblerCountsFinalChunkAgainstGlobalByteLimit(t *testing.T) {
	assembler := newNativeResponseAssembler(10, 10)
	frames := []nativeFrame{
		{flags: nativeFlagMoreChunks, laneID: 1, opcode: nativeOpGet, requestID: 1, body: []byte("123456")},
		{flags: nativeFlagMoreChunks, laneID: 1, opcode: nativeOpGet, requestID: 2, body: []byte("12")},
	}
	for _, frame := range frames {
		if response, err := assembler.add(frame); err != nil || response != nil {
			t.Fatalf("add incomplete frame = %#v, %v", response, err)
		}
	}

	_, err := assembler.add(nativeFrame{
		laneID: 1, opcode: nativeOpGet, requestID: 2, body: []byte("12345678"),
	})
	if err == nil || !strings.Contains(err.Error(), "buffered chunk responses") {
		t.Fatalf("final chunk global byte error = %v", err)
	}
}

func TestNativeResponseAssemblerCountsFinalChunkAgainstGlobalFrameLimit(t *testing.T) {
	assembler := newNativeResponseAssembler(1024, 3)
	frames := []nativeFrame{
		{flags: nativeFlagMoreChunks, laneID: 1, opcode: nativeOpGet, requestID: 1, body: []byte{1}},
		{flags: nativeFlagMoreChunks, laneID: 1, opcode: nativeOpGet, requestID: 1, body: []byte{2}},
		{flags: nativeFlagMoreChunks, laneID: 1, opcode: nativeOpGet, requestID: 2, body: []byte{3}},
	}
	for _, frame := range frames {
		if response, err := assembler.add(frame); err != nil || response != nil {
			t.Fatalf("add incomplete frame = %#v, %v", response, err)
		}
	}

	_, err := assembler.add(nativeFrame{
		laneID: 1, opcode: nativeOpGet, requestID: 2, body: []byte{4},
	})
	if err == nil || !strings.Contains(err.Error(), "buffered chunk responses exceed") {
		t.Fatalf("final chunk global frame error = %v", err)
	}
}

func TestNativeResponseAssemblerCountsSingleFrameAgainstBufferedChunks(t *testing.T) {
	assembler := newNativeResponseAssembler(10, 10)
	if response, err := assembler.add(nativeFrame{
		flags: nativeFlagMoreChunks, laneID: 1, opcode: nativeOpGet, requestID: 1, body: []byte("123456"),
	}); err != nil || response != nil {
		t.Fatalf("add incomplete frame = %#v, %v", response, err)
	}

	_, err := assembler.add(nativeFrame{
		laneID: 1, opcode: nativeOpGet, requestID: 2, body: []byte("12345"),
	})
	if err == nil || !strings.Contains(err.Error(), "buffered chunk responses") {
		t.Fatalf("single-frame global byte error = %v", err)
	}
}

type nativeEncodingSignal struct {
	id      int
	encoded chan<- int
}

func (s nativeEncodingSignal) String() string {
	s.encoded <- s.id
	return "payload"
}

func TestNativeWriteBoundsEncodedBodiesBehindBlockedWriter(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	exec := &NativeExecutor{
		conn:                 client,
		writer:               bufio.NewWriter(client),
		maxRequestFrameBytes: nativeDefaultRequestFrameBytes,
	}
	encoded := make(chan int, 3)
	done := make(chan struct{}, 3)
	for id := 1; id <= 3; id++ {
		go func() {
			_, _ = exec.writeRequest(
				context.Background(), nativeOpCommandExec, 1, uint64(id),
				nativeEncodingSignal{id: id, encoded: encoded}, 0, client, false,
			)
			done <- struct{}{}
		}()
	}

	seen := map[int]bool{}
	for len(seen) < 2 {
		select {
		case id := <-encoded:
			seen[id] = true
		case <-time.After(time.Second):
			t.Fatal("fewer than two requests reached native encoding")
		}
	}
	select {
	case id := <-encoded:
		t.Fatalf("third request %d encoded while one writer and one encoded request were blocked", id)
	case <-time.After(25 * time.Millisecond):
	}

	_ = server.Close()
	for range 3 {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("blocked native writer did not exit")
		}
	}
}

func TestNativePipelinePayloadDefersCompactWireAllocation(t *testing.T) {
	payload, flags, err := nativePipelinePayload(
		[][]any{{"SET", "key", []byte("value")}},
		1,
		nativeDefaultRequestFrameBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	if flags&nativeFlagCustomPayload == 0 {
		t.Fatalf("compact pipeline flags = %#x; want custom payload", flags)
	}
	if _, eager := payload.([]byte); eager {
		t.Fatal("compact pipeline allocated its wire body before writer admission")
	}
}

func TestNativeCompactPipelinePlanningDoesNotAllocate(t *testing.T) {
	commands := [][]any{{"SET", "key", []byte("value")}}
	allocs := testing.AllocsPerRun(1_000, func() {
		plan, ok, err := compactPipelinePlanWithLimit(commands, nativeDefaultRequestFrameBytes)
		if err != nil || !ok || plan.size == 0 {
			panic("compact pipeline was not planned")
		}
	})
	if allocs != 0 {
		t.Fatalf("compact pipeline planning allocations = %.0f; want 0", allocs)
	}
}
