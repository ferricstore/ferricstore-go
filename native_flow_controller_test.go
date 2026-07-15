package ferricstore

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

func TestNativeFlowControllerHonorsDisabledAndSerializedLaneQueue(t *testing.T) {
	disabled := newNativeFlowController(10, 10, 0)
	if err := disabled.acquire(context.Background(), 1); err == nil {
		t.Fatal("expected zero STARTUP lane queue to reject requests")
	}
	controller := newNativeFlowController(10, 10, 1)
	if err := controller.acquire(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	acquired := make(chan error, 1)
	go func() {
		acquired <- controller.acquire(context.Background(), 1)
	}()
	select {
	case err := <-acquired:
		controller.release(1)
		if err != nil {
			t.Fatal(err)
		}
		t.Fatal("second request bypassed max_lane_queue=1")
	case <-time.After(20 * time.Millisecond):
	}
	controller.release(1)
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatal(err)
		}
		controller.release(1)
	case <-time.After(time.Second):
		t.Fatal("queued lane request did not resume after credit release")
	}
}

func TestNativeFlowControllerRejectsCanceledAcquireBeforeTakingCredit(t *testing.T) {
	controller := newNativeFlowController(1, 1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := controller.acquire(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled acquire error = %v", err)
	}
	controller.mu.Lock()
	active := controller.activeTotal
	controller.mu.Unlock()
	if active != 0 {
		t.Fatalf("canceled acquire consumed %d credits", active)
	}
}

func TestNativeFlowControllerUncontendedDoesNotAllocate(t *testing.T) {
	controller := newNativeFlowController(4096, 1024, 1024)
	ctx := context.Background()
	allocs := testing.AllocsPerRun(1000, func() {
		if err := controller.acquire(ctx, 1); err != nil {
			panic(err)
		}
		controller.release(1)
	})
	if allocs != 0 {
		t.Fatalf("uncontended flow credit allocated %.0f objects per acquire/release, want 0", allocs)
	}
}

func TestNativeWindowUpdateReopensZeroDataWindow(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	errCh := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{
			"flow_control": map[string]any{
				"max_inflight_per_connection": int64(0),
				"max_inflight_per_lane":       int64(0),
			},
		}); err != nil {
			errCh <- err
			return
		}

		window, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if window.opcode != nativeOpWindowUpdate || window.laneID != 0 {
			errCh <- errUnexpectedFrame(window)
			return
		}
		value, rest, err := decodeNativeValue(window.body)
		if err != nil || len(rest) != 0 {
			errCh <- fmt.Errorf("decode WINDOW_UPDATE payload: value=%#v rest=%d err=%w", value, len(rest), err)
			return
		}
		payload, err := nativeMap(value)
		if err != nil {
			errCh <- err
			return
		}
		if asInt64(payload["max_inflight_per_connection"]) != 1 || asInt64(payload["max_inflight_per_lane"]) != 1 {
			errCh <- fmt.Errorf("unexpected WINDOW_UPDATE payload %#v", payload)
			return
		}
		if err := writeNativeTestResponse(writer, window, nativeStatusOK, map[string]any{
			"accepted": true,
			"limits": map[string]any{
				"max_inflight_per_connection": int64(1),
				"max_inflight_per_lane":       int64(1),
			},
		}); err != nil {
			errCh <- err
			return
		}

		get, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if get.opcode != nativeOpGet {
			errCh <- errUnexpectedFrame(get)
			return
		}
		errCh <- writeNativeTestResponse(writer, get, nativeStatusOK, []byte("value"))
	}()

	exec := NewNativeExecutor(listener.Addr().String(), WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0), WithNativeReconnect(0))
	defer func() { _ = exec.Close() }()
	if _, err := exec.Do(context.Background(), "WINDOW_UPDATE",
		"MAX_INFLIGHT_PER_CONNECTION", 1,
		"MAX_INFLIGHT_PER_LANE", 1,
	); err != nil {
		t.Fatal(err)
	}
	if got, err := exec.Do(context.Background(), "GET", "key"); err != nil || asString(got) != "value" {
		t.Fatalf("GET after WINDOW_UPDATE = %#v, %v", got, err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestNativeFlowControllerBoundsAndFairlyGrantsWaiters(t *testing.T) {
	controller := newNativeFlowController(1, 2, 2, 3)
	if err := controller.acquire(context.Background(), 1); err != nil {
		t.Fatal(err)
	}

	type grant struct {
		name string
		lane uint32
	}
	grants := make(chan grant, 3)
	queue := func(name string, lane uint32) {
		go func() {
			if err := controller.acquire(context.Background(), lane); err == nil {
				grants <- grant{name: name, lane: lane}
			}
		}()
		waitForNativeFlowQueue(t, controller, name)
	}
	queue("lane-1-a", 1)
	queue("lane-1-b", 1)
	queue("lane-2", 2)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := controller.acquire(ctx, 3); err == nil || !strings.Contains(err.Error(), "queue is full") {
		t.Fatalf("overflow acquire error = %v", err)
	}

	controller.release(1)
	first := <-grants
	if first.name != "lane-1-a" {
		t.Fatalf("first queued grant = %q, want lane-1-a", first.name)
	}
	controller.release(first.lane)
	second := <-grants
	if second.name != "lane-2" {
		t.Fatalf("second queued grant = %q, want fair lane-2 grant", second.name)
	}
	controller.release(second.lane)
	third := <-grants
	if third.name != "lane-1-b" {
		t.Fatalf("third queued grant = %q, want lane-1-b", third.name)
	}
	controller.release(third.lane)
}

func TestNativeFlowControllerDoesNotRetainCanceledWaiters(t *testing.T) {
	controller := newNativeFlowController(1, 1, 1024, 1024)
	if err := controller.acquire(context.Background(), 1); err != nil {
		t.Fatal(err)
	}

	anchorCtx, cancelAnchor := context.WithCancel(context.Background())
	anchorDone := make(chan error, 1)
	go func() {
		anchorDone <- controller.acquire(anchorCtx, 1)
	}()
	waitForNativeFlowQueue(t, controller, "anchor")

	for index := 0; index < 2048; index++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := controller.acquire(ctx, 1); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled waiter %d error = %v", index, err)
		}
	}

	controller.mu.Lock()
	queue := controller.queues[1]
	retained := 0
	for waiter := queue.head; waiter != nil; waiter = waiter.next {
		retained++
	}
	waiting := queue.waiting
	controller.mu.Unlock()
	if retained != waiting {
		t.Fatalf("retained waiter slots = %d, active waiters = %d", retained, waiting)
	}

	cancelAnchor()
	if err := <-anchorDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("anchor error = %v", err)
	}
	controller.release(1)
}

func waitForNativeFlowQueue(t *testing.T, controller *nativeFlowController, waiter string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		controller.mu.Lock()
		queued := controller.queuedTotal
		controller.mu.Unlock()
		if queued > 0 && queued >= map[string]int{"lane-1-a": 1, "lane-1-b": 2, "lane-2": 3}[waiter] {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s was not queued", waiter)
		}
		time.Sleep(time.Millisecond)
	}
}
