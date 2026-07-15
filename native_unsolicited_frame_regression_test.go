package ferricstore

import (
	"bufio"
	"net"
	"testing"
	"time"
)

func TestNativeReaderRejectsInvalidReservedRequestIDFrame(t *testing.T) {
	client, server := net.Pipe()
	exec := NewNativeExecutor("unused")
	exec.mu.Lock()
	exec.conn = client
	exec.reader = bufio.NewReader(client)
	exec.writer = bufio.NewWriter(client)
	exec.connectionDone = make(chan struct{})
	exec.pending = make(map[uint64]*nativePendingRequest)
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
			t.Error("native reader did not stop during cleanup")
		}
	})

	writer := bufio.NewWriter(server)
	err := writeNativeTestResponse(writer, nativeFrame{
		laneID: 1, opcode: nativeOpGet, requestID: 0,
	}, nativeStatusOK, []byte("invalid unsolicited response"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("native reader accepted a data-lane GET with reserved request_id 0")
	}
}
