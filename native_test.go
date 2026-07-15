package ferricstore

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestNativeExecutorTimeoutBoundsFullRequest(t *testing.T) {
	listener, requestRead, release := startStalledNativeServer(t)
	defer close(release)

	client := NewClient(
		listener.Addr().String(),
		WithNativeOptions(
			WithNativeTimeout(40*time.Millisecond),
			WithNativeHeartbeat(0, 0),
			WithNativeReconnect(0),
		),
	)
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := client.Ping(ctx)
		errCh <- err
	}()

	<-requestRead
	select {
	case err := <-errCh:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected full-request deadline, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		cancel()
		<-errCh
		t.Fatal("native request ignored NativeOptions.Timeout while waiting for a response")
	}
}

func TestNativeCloseCancelsInProgressTLSHandshake(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	exec := NewNativeExecutor(
		listener.Addr().String(),
		WithNativeTLS(&tls.Config{InsecureSkipVerify: true}), // test endpoint intentionally has no TLS server
		WithNativeTimeout(time.Second),
		WithNativeHeartbeat(0, 0),
	)
	requestDone := make(chan error, 1)
	go func() {
		_, err := exec.Do(context.Background(), "PING")
		requestDone <- err
	}()
	serverConn := <-accepted
	closeDone := make(chan error, 1)
	started := time.Now()
	go func() { closeDone <- exec.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
			t.Fatalf("Close took %v while canceling TLS handshake", elapsed)
		}
	case <-time.After(250 * time.Millisecond):
		_ = serverConn.Close()
		<-closeDone
		t.Fatal("Close blocked behind an in-progress TLS handshake")
	}
	_ = serverConn.Close()
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("request did not exit after Close canceled connection setup")
	}
}

func TestNativeSharedConnectIsNotOwnedByFirstCallerContext(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	startupRead := make(chan struct{})
	releaseStartup := make(chan struct{})
	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		close(startupRead)
		<-releaseStartup
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			serverErr <- err
			return
		}
		request, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		serverErr <- writeNativeTestResponse(writer, request, nativeStatusOK, []byte("follower"))
	}()

	exec := NewNativeExecutor(
		listener.Addr().String(),
		WithNativeTimeout(time.Second),
		WithNativeHeartbeat(0, 0),
		WithNativeReconnect(0),
	)
	defer func() { _ = exec.Close() }()
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderErr := make(chan error, 1)
	go func() {
		_, err := exec.Do(leaderCtx, "PING", "leader")
		leaderErr <- err
	}()
	<-startupRead

	followerCtx, cancelFollower := context.WithTimeout(context.Background(), time.Second)
	defer cancelFollower()
	followerResult := make(chan struct {
		value any
		err   error
	}, 1)
	go func() {
		value, err := exec.Do(followerCtx, "PING", "follower")
		followerResult <- struct {
			value any
			err   error
		}{value: value, err: err}
	}()

	deadline := time.Now().Add(time.Second)
	for {
		exec.mu.Lock()
		active := exec.activeRequests
		exec.mu.Unlock()
		if active >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("follower did not join the shared connection attempt")
		}
		time.Sleep(time.Millisecond)
	}
	cancelLeader()
	close(releaseStartup)
	if err := <-leaderErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader error = %v, want context cancellation", err)
	}

	result := <-followerResult
	if result.err != nil || asString(result.value) != "follower" {
		t.Fatalf("follower inherited leader cancellation: value=%#v err=%v", result.value, result.err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestNativeExecutorCloseFailsPendingRequests(t *testing.T) {
	listener, requestRead, release := startStalledNativeServer(t)
	defer close(release)

	client := NewClient(
		listener.Addr().String(),
		WithNativeOptions(WithNativeHeartbeat(0, 0), WithNativeReconnect(0)),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := client.Ping(ctx)
		errCh <- err
	}()

	<-requestRead
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("expected close to fail pending request with net.ErrClosed, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		cancel()
		<-errCh
		t.Fatal("client close left a pending native request blocked")
	}
}

func TestNativeExecutorCancellationDoesNotFailUnrelatedPendingRequest(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()

	bothRead := make(chan struct{})
	releaseResponses := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader := bufio.NewReader(conn)
		writer := bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			errCh <- err
			return
		}

		frames := make(map[string]nativeFrame, 2)
		for range 2 {
			frame, err := readNativeRequestFrame(reader)
			if err != nil {
				errCh <- err
				return
			}
			value, rest, err := decodeNativeValue(frame.body)
			if err != nil || len(rest) != 0 {
				errCh <- fmt.Errorf("decode request payload: %w", err)
				return
			}
			payload, err := nativeMap(value)
			if err != nil {
				errCh <- err
				return
			}
			frames[asString(payload["key"])] = frame
		}
		close(bothRead)
		<-releaseResponses
		if err := writeNativeTestResponse(writer, frames["cancel"], nativeStatusOK, []byte("late")); err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, frames["survivor"], nativeStatusOK, []byte("ok")); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	exec := NewNativeExecutor(listener.Addr().String(),
		WithNativeTimeout(time.Second),
		WithNativeHeartbeat(0, 0),
		WithNativeReconnect(0),
		WithNativeLanes(2),
	)
	defer func() { _ = exec.Close() }()

	cancelCtx, cancel := context.WithCancel(context.Background())
	canceled := make(chan error, 1)
	survivor := make(chan autoBatchResult, 1)
	go func() {
		_, err := exec.Do(cancelCtx, "GET", "cancel")
		canceled <- err
	}()
	go func() {
		value, err := exec.Do(context.Background(), "GET", "survivor")
		survivor <- autoBatchResult{value: value, err: err}
	}()

	<-bothRead
	cancel()
	if err := <-canceled; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request error = %v, want context.Canceled", err)
	}
	close(releaseResponses)
	result := <-survivor
	if result.err != nil || asString(result.value) != "ok" {
		t.Fatalf("unrelated request = %#v, %v; want ok", result.value, result.err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestNativeExecutorCancellationBoundsGoAwayDrain(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()

	firstRequestsRead := make(chan struct{})
	serverErr := make(chan error, 1)
	go func() {
		first, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		firstReader, firstWriter := bufio.NewReader(first), bufio.NewWriter(first)
		startup, err := readNativeRequestFrame(firstReader)
		if err != nil {
			serverErr <- err
			return
		}
		if err := writeNativeTestResponse(firstWriter, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			serverErr <- err
			return
		}
		for range 2 {
			if _, err := readNativeRequestFrame(firstReader); err != nil {
				serverErr <- err
				return
			}
		}
		close(firstRequestsRead)
		_ = first.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := readNativeRequestFrame(firstReader); err == nil {
			serverErr <- errors.New("draining connection remained open")
			return
		}
		_ = first.Close()

		second, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer func() { _ = second.Close() }()
		secondReader, secondWriter := bufio.NewReader(second), bufio.NewWriter(second)
		startup, err = readNativeRequestFrame(secondReader)
		if err != nil {
			serverErr <- err
			return
		}
		if err := writeNativeTestResponse(secondWriter, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			serverErr <- err
			return
		}
		request, err := readNativeRequestFrame(secondReader)
		if err != nil {
			serverErr <- err
			return
		}
		serverErr <- writeNativeTestResponse(secondWriter, request, nativeStatusOK, []byte("fresh"))
	}()

	exec := NewNativeExecutor(listener.Addr().String(),
		WithNativeTimeout(time.Second),
		WithNativeGoAwayDrainTimeout(50*time.Millisecond),
		WithNativeHeartbeat(0, 0),
		WithNativeReconnect(0),
		WithNativeLanes(2),
	)
	defer func() { _ = exec.Close() }()

	cancelCtx, cancel := context.WithCancel(context.Background())
	canceled := make(chan error, 1)
	blocked := make(chan error, 1)
	go func() {
		_, err := exec.Do(cancelCtx, "GET", "cancel")
		canceled <- err
	}()
	go func() {
		_, err := exec.Do(context.Background(), "BLPOP", "blocker", 0)
		blocked <- err
	}()

	<-firstRequestsRead
	cancel()
	if err := <-canceled; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request error = %v, want context.Canceled", err)
	}

	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	value, err := exec.Do(ctx, "GET", "fresh")
	if err != nil || asString(value) != "fresh" {
		t.Fatalf("request after bounded drain = %#v, %v; want fresh", value, err)
	}
	select {
	case err := <-blocked:
		if err == nil {
			t.Fatal("infinite blocker unexpectedly succeeded")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("infinite blocker retained the draining connection")
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestNativeCanceledControlRequestsAreRemovedFromPending(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	requestRead := make(chan struct{}, 3)
	releaseServer := make(chan struct{})
	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			serverErr <- err
			return
		}
		for range 3 {
			frame, err := readNativeRequestFrame(reader)
			if err != nil {
				serverErr <- err
				return
			}
			if frame.opcode != nativeOpPing {
				serverErr <- errUnexpectedFrame(frame)
				return
			}
			requestRead <- struct{}{}
		}
		<-releaseServer
		serverErr <- nil
	}()

	exec := NewNativeExecutor(
		listener.Addr().String(),
		WithNativeTimeout(time.Second),
		WithNativeHeartbeat(0, 0),
		WithNativeReconnect(0),
	)
	defer func() { _ = exec.Close() }()
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := exec.Do(ctx, "PING")
			done <- err
		}()
		<-requestRead
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("control request %d error = %v, want cancellation", i, err)
		}
		exec.mu.Lock()
		pending := len(exec.pending)
		exec.mu.Unlock()
		if pending != 0 {
			t.Fatalf("canceled control request %d left %d pending entries", i, pending)
		}
	}
	close(releaseServer)
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestNativeExecutorCancellationReclaimsCreditAndReconnects(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()

	requestRead := make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
		for connection := 0; connection < 2; connection++ {
			conn, err := listener.Accept()
			if err != nil {
				serverDone <- err
				return
			}
			reader := bufio.NewReader(conn)
			writer := bufio.NewWriter(conn)
			startup, err := readNativeRequestFrame(reader)
			if err != nil {
				_ = conn.Close()
				serverDone <- err
				return
			}
			if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{
				"flow_control": map[string]any{
					"max_inflight_per_connection": int64(1),
					"max_inflight_per_lane":       int64(1),
				},
			}); err != nil {
				_ = conn.Close()
				serverDone <- err
				return
			}
			request, err := readNativeRequestFrame(reader)
			if err != nil {
				_ = conn.Close()
				serverDone <- err
				return
			}
			if connection == 0 {
				close(requestRead)
				_ = conn.SetReadDeadline(time.Now().Add(time.Second))
				if _, err := reader.ReadByte(); err == nil {
					_ = conn.Close()
					serverDone <- errors.New("canceled data request left its connection open")
					return
				} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					_ = conn.Close()
					serverDone <- errors.New("timed out waiting for canceled data connection to close")
					return
				}
				_ = conn.Close()
				continue
			}
			if err := writeNativeTestResponse(writer, request, nativeStatusOK, []byte("fresh")); err != nil {
				_ = conn.Close()
				serverDone <- err
				return
			}
			_ = conn.Close()
		}
		serverDone <- nil
	}()

	exec := NewNativeExecutor(listener.Addr().String(),
		WithNativeTimeout(time.Second),
		WithNativeHeartbeat(0, 0),
		WithNativeReconnect(0),
	)
	defer func() { _ = exec.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	requestDone := make(chan error, 1)
	go func() {
		_, err := exec.Do(ctx, "GET", "cancel")
		requestDone <- err
	}()

	<-requestRead
	exec.mu.Lock()
	flow := exec.flow
	exec.mu.Unlock()
	cancel()
	if err := <-requestDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request error = %v", err)
	}
	exec.mu.Lock()
	pending := len(exec.pending)
	exec.mu.Unlock()
	flow.mu.Lock()
	active := flow.activeTotal
	flow.mu.Unlock()
	if active != 0 || pending != 0 {
		t.Fatalf("canceled request retained active=%d pending=%d", active, pending)
	}
	got, err := exec.Do(context.Background(), "GET", "after-cancel")
	if err != nil || asString(got) != "fresh" {
		t.Fatalf("request after cancellation = %#v, %v", got, err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestNativeHeartbeatDoesNotAbortRequestsDuringRetirement(t *testing.T) {
	conn, peer := net.Pipe()
	defer func() { _ = peer.Close() }()

	exec := NewNativeExecutor("127.0.0.1:1")
	exec.mu.Lock()
	exec.conn = conn
	exec.reader = bufio.NewReader(conn)
	exec.writer = bufio.NewWriter(conn)
	exec.retiring = true
	exec.activeRequests = 1
	exec.mu.Unlock()

	done := make(chan struct{})
	go func() {
		exec.heartbeatLoop(make(chan struct{}), time.Millisecond, time.Millisecond)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("heartbeat did not observe retiring executor")
	}

	exec.mu.Lock()
	stillConnected := exec.conn == conn
	exec.mu.Unlock()
	if !stillConnected {
		t.Fatal("heartbeat closed a retiring adapter while another request was still active")
	}
	if err := exec.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNativeHeartbeatSkipsActiveTransactionSession(t *testing.T) {
	conn, peer := net.Pipe()
	defer func() { _ = peer.Close() }()
	exec := NewNativeExecutor("127.0.0.1:1")
	exec.mu.Lock()
	exec.conn = conn
	exec.reader = bufio.NewReader(conn)
	exec.writer = bufio.NewWriter(conn)
	exec.connectionDone = make(chan struct{})
	exec.mu.Unlock()
	if err := exec.sessionGate.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		exec.heartbeatLoop(stop, time.Millisecond, time.Millisecond)
		close(done)
	}()
	time.Sleep(10 * time.Millisecond)
	close(stop)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not stop")
	}
	exec.sessionGate.unlock()
	exec.mu.Lock()
	stillConnected := exec.conn == conn
	exec.mu.Unlock()
	if !stillConnected {
		t.Fatal("heartbeat closed the connection while a transaction owned it")
	}
	if err := exec.Close(); err != nil {
		t.Fatal(err)
	}
}

func startStalledNativeServer(t *testing.T) (net.Listener, <-chan struct{}, chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	requestRead := make(chan struct{})
	release := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		reader := bufio.NewReader(conn)
		writer := bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			return
		}
		if _, err := readNativeRequestFrame(reader); err != nil {
			return
		}
		close(requestRead)
		<-release
	}()
	return listener, requestRead, release
}
