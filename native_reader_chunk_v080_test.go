package ferricstore

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"
)

func TestV080ReaderRejectsMismatchedChunkTupleImmediately(t *testing.T) {
	responseCh := make(chan nativeResponse, 1)
	exec, writer, done := newNativeChunkReaderHarness(t, &nativePendingRequest{
		responseCh: responseCh,
		opcode:     nativeOpGet,
		laneID:     1,
	})

	err := writeNativeFrameBody(writer, nativeFrame{
		laneID: 1, opcode: nativeOpSet, requestID: 1,
	}, nativeFlagMoreChunks, []byte{0, 0})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case response := <-responseCh:
		if response.err == nil || !strings.Contains(response.err.Error(), "response mismatch") {
			t.Fatalf("mismatched chunk error = %v", response.err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("mismatched partial chunk remained buffered")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reader did not close after a mismatched chunk tuple")
	}
	_ = exec.Close()
}

func TestV080ReaderCountsPartialChunksAsConnectionActivity(t *testing.T) {
	exec, writer, _ := newNativeChunkReaderHarness(t, &nativePendingRequest{
		responseCh: make(chan nativeResponse, 1),
		opcode:     nativeOpGet,
		laneID:     1,
	})
	exec.lastActivityUnixNano.Store(1)

	err := writeNativeFrameBody(writer, nativeFrame{
		laneID: 1, opcode: nativeOpGet, requestID: 1,
	}, nativeFlagMoreChunks, []byte{0, 0})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(100 * time.Millisecond)
	for exec.lastActivityUnixNano.Load() == 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if exec.lastActivityUnixNano.Load() == 1 {
		t.Fatal("partial response chunk did not refresh connection activity")
	}
}

func newNativeChunkReaderHarness(
	t *testing.T,
	pending *nativePendingRequest,
) (*NativeExecutor, *bufio.Writer, <-chan struct{}) {
	t.Helper()
	client, server := net.Pipe()
	exec := NewNativeExecutor("unused", WithNativeHeartbeat(0, 0))
	exec.mu.Lock()
	exec.conn = client
	exec.reader = bufio.NewReader(client)
	exec.writer = bufio.NewWriter(client)
	exec.connectionDone = make(chan struct{})
	exec.pending = map[uint64]*nativePendingRequest{1: pending}
	exec.mu.Unlock()

	done := make(chan struct{})
	go func() {
		exec.readerLoop(client, exec.reader)
		close(done)
	}()
	t.Cleanup(func() {
		_ = server.Close()
		_ = exec.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("native chunk reader did not stop during cleanup")
		}
	})
	return exec, bufio.NewWriter(server), done
}
