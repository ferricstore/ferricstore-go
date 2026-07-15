package ferricstore

import (
	"bufio"
	"net"
	"testing"
	"time"
)

func TestNativeStaleReaderDropsBufferedServerEvents(t *testing.T) {
	oldClient, oldServer := net.Pipe()
	newClient, newServer := net.Pipe()
	t.Cleanup(func() { _ = oldClient.Close() })
	t.Cleanup(func() { _ = oldServer.Close() })
	t.Cleanup(func() { _ = newServer.Close() })

	handled := make(chan nativeServerEvent, 1)
	exec := NewNativeExecutor("unused", WithNativeHeartbeat(0, 0))
	exec.opts.eventHandler = func(event nativeServerEvent) { handled <- event }
	exec.mu.Lock()
	exec.conn = newClient
	exec.events = make(chan nativeQueuedEvent, 1)
	exec.eventDeliveryEnabled = true
	exec.connectionDone = make(chan struct{})
	exec.mu.Unlock()
	t.Cleanup(func() { _ = exec.Close() })

	readerDone := make(chan struct{})
	go func() {
		exec.readerLoop(oldClient, bufio.NewReader(oldClient))
		close(readerDone)
	}()
	writer := bufio.NewWriter(oldServer)
	if err := writeNativeTestResponse(
		writer,
		nativeFrame{opcode: nativeOpEvent, requestID: 0},
		nativeStatusOK,
		[]any{"message", "jobs", "stale"},
	); err != nil {
		t.Fatal(err)
	}
	_ = oldServer.Close()
	select {
	case <-readerDone:
	case <-time.After(time.Second):
		t.Fatal("stale reader did not exit")
	}

	select {
	case event := <-handled:
		t.Fatalf("stale reader invoked event handler with %#v", event)
	default:
	}
	exec.mu.Lock()
	queued := len(exec.events)
	exec.mu.Unlock()
	if queued != 0 {
		t.Fatalf("stale reader queued %d server events", queued)
	}
}

func TestNativeStaleWindowUpdateDoesNotMutateReplacementFlow(t *testing.T) {
	oldClient, oldServer := net.Pipe()
	newClient, newServer := net.Pipe()
	defer func() { _ = oldClient.Close() }()
	defer func() { _ = oldServer.Close() }()
	defer func() { _ = newClient.Close() }()
	defer func() { _ = newServer.Close() }()

	flow := newNativeFlowController(17, 11, 11)
	exec := NewNativeExecutor("unused", WithNativeHeartbeat(0, 0))
	exec.mu.Lock()
	exec.conn = newClient
	exec.flow = flow
	exec.mu.Unlock()

	exec.applyFlowControlLimits(oldClient, map[string]any{
		"max_inflight_per_connection": int64(1),
		"max_inflight_per_lane":       int64(1),
	})

	flow.mu.Lock()
	connectionLimit := flow.connectionLimit
	laneLimit := flow.laneLimit
	flow.mu.Unlock()
	if connectionLimit != 17 || laneLimit != 11 {
		t.Fatalf("replacement flow limits = (%d, %d); want (17, 11)", connectionLimit, laneLimit)
	}
}
