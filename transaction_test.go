package ferricstore

import (
	"bufio"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type transactionRecordingExecutor struct {
	mu    sync.Mutex
	calls chan []any
	gate  sessionGate
}

type blockingTransactionSession struct {
	entered   chan struct{}
	aborted   chan struct{}
	abortOnce sync.Once
}

func (s *blockingTransactionSession) Do(_ context.Context, args ...any) (any, error) {
	if asString(args[0]) == "MULTI" {
		return []byte("OK"), nil
	}
	close(s.entered)
	<-s.aborted
	return nil, context.Canceled
}

func (s *blockingTransactionSession) Abort(_ error) {
	s.abortOnce.Do(func() { close(s.aborted) })
}

func (s *blockingTransactionSession) Release() { s.Abort(nil) }

type blockingTransactionProvider struct {
	session *blockingTransactionSession
}

func (p *blockingTransactionProvider) Do(context.Context, ...any) (any, error) {
	return nil, errors.New("unexpected non-session execution")
}

func (p *blockingTransactionProvider) acquireCommandSession(context.Context, ...any) (commandSession, error) {
	return p.session, nil
}

func (e *transactionRecordingExecutor) Do(ctx context.Context, args ...any) (any, error) {
	if err := e.gate.readLock(ctx); err != nil {
		return nil, err
	}
	defer e.gate.readUnlock()
	return e.do(args...)
}

func (e *transactionRecordingExecutor) do(args ...any) (any, error) {
	call := append([]any(nil), args...)
	e.mu.Lock()
	e.calls <- call
	e.mu.Unlock()
	switch asString(args[0]) {
	case "MULTI":
		return []byte("OK"), nil
	case "EXEC":
		return []any{[]byte("OK")}, nil
	case "COMMAND_EXEC":
		return []byte("QUEUED"), nil
	default:
		return []byte("OK"), nil
	}
}

func (e *transactionRecordingExecutor) acquireCommandSession(ctx context.Context, _ ...any) (commandSession, error) {
	if err := e.gate.lock(ctx); err != nil {
		return nil, err
	}
	session := &executorCommandSession{exec: executorFunc(func(_ context.Context, args ...any) (any, error) {
		return e.do(args...)
	})}
	return &clientCommandSession{commandSession: session, release: e.gate.unlock}, nil
}

func TestTransactionExcludesUnrelatedClientCommands(t *testing.T) {
	exec := &transactionRecordingExecutor{calls: make(chan []any, 8)}
	client := NewClientWithExecutor(exec)
	tx, err := client.Transaction(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := <-exec.calls; asString(got[0]) != "MULTI" {
		t.Fatalf("unexpected first call: %#v", got)
	}

	outsideDone := make(chan error, 1)
	go func() {
		_, err := client.CommandExec(context.Background(), "INCR", "outside")
		outsideDone <- err
	}()
	select {
	case call := <-exec.calls:
		t.Fatalf("unrelated command entered active transaction: %#v", call)
	case <-time.After(50 * time.Millisecond):
	}

	if _, err := tx.Command(context.Background(), "SET", "inside", "1"); err != nil {
		t.Fatal(err)
	}
	if got := <-exec.calls; asString(got[0]) != "COMMAND_EXEC" || asString(got[1]) != "SET" {
		t.Fatalf("unexpected transaction command: %#v", got)
	}
	if _, err := tx.Exec(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := <-exec.calls; asString(got[0]) != "EXEC" {
		t.Fatalf("unexpected exec call: %#v", got)
	}

	select {
	case call := <-exec.calls:
		if asString(call[0]) != "COMMAND_EXEC" || asString(call[1]) != "INCR" {
			t.Fatalf("unexpected outside call after transaction: %#v", call)
		}
	case <-time.After(time.Second):
		t.Fatal("unrelated command remained blocked after transaction ended")
	}
	if err := <-outsideDone; err != nil {
		t.Fatal(err)
	}
}

func TestLegacyMultiQueuesCommandsOnAffineSession(t *testing.T) {
	exec := &transactionRecordingExecutor{calls: make(chan []any, 8)}
	client := NewClientWithExecutor(exec)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := client.Multi(ctx); err != nil {
		t.Fatal(err)
	}
	if got := <-exec.calls; asString(got[0]) != "MULTI" {
		t.Fatalf("unexpected MULTI call: %#v", got)
	}
	if value, err := client.Command(ctx, "SET", "inside", "1"); err != nil || !strings.EqualFold(asString(value), "QUEUED") {
		t.Fatalf("legacy transaction command = %#v, %v; want QUEUED", value, err)
	}
	if got := <-exec.calls; len(got) < 2 || asString(got[0]) != "COMMAND_EXEC" || asString(got[1]) != "SET" {
		t.Fatalf("legacy command did not use affine COMMAND_EXEC: %#v", got)
	}
	if _, err := client.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if got := <-exec.calls; asString(got[0]) != "EXEC" {
		t.Fatalf("unexpected EXEC call: %#v", got)
	}
}

func TestWatchAllowsAffineReadBeforeMulti(t *testing.T) {
	exec := &transactionRecordingExecutor{calls: make(chan []any, 8)}
	client := NewClientWithExecutor(exec)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := client.Watch(ctx, "watched"); err != nil {
		t.Fatal(err)
	}
	<-exec.calls
	if _, err := client.Command(ctx, "GET", "watched"); err != nil {
		t.Fatalf("affine read after WATCH failed: %v", err)
	}
	if got := <-exec.calls; len(got) < 2 || asString(got[0]) != "COMMAND_EXEC" || asString(got[1]) != "GET" {
		t.Fatalf("WATCH read did not use affine COMMAND_EXEC: %#v", got)
	}
	if err := client.Unwatch(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestTransactionWaitHonorsCommandContext(t *testing.T) {
	exec := &transactionRecordingExecutor{calls: make(chan []any, 8)}
	client := NewClientWithExecutor(exec)
	tx, err := client.Transaction(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Discard(context.Background()) }()
	<-exec.calls

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = client.Command(ctx, "PING")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected transaction gate to honor deadline, got %v", err)
	}
	if elapsed := time.Since(started); elapsed > 150*time.Millisecond {
		t.Fatalf("transaction gate ignored deadline for %v", elapsed)
	}
	select {
	case call := <-exec.calls:
		t.Fatalf("timed-out command reached executor: %#v", call)
	default:
	}
}

func TestTransactionRequiresQueuedAcknowledgement(t *testing.T) {
	exec := &fakeExecutor{values: []any{[]byte("OK"), []byte("OK")}}
	client := NewClientWithExecutor(exec)
	tx, err := client.Transaction(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Command(context.Background(), "SET", "key", "value"); err == nil {
		t.Fatal("expected non-QUEUED transaction acknowledgement to fail")
	}
}

func TestTransactionRequiresAffineExecutor(t *testing.T) {
	client := NewClientWithExecutor(executorFunc(func(context.Context, ...any) (any, error) {
		return []byte("OK"), nil
	}))
	if _, err := client.Transaction(context.Background()); err == nil {
		t.Fatal("expected transaction over a non-affine executor to fail")
	}
}

func TestTransactionRequiresOKMultiAcknowledgement(t *testing.T) {
	exec := &transactionRecordingExecutor{calls: make(chan []any, 1)}
	execValue := &sessionResponseProvider{Executor: exec, multiResponse: []byte("QUEUED")}
	client := NewClientWithExecutor(execValue)
	if _, err := client.Transaction(context.Background()); err == nil {
		t.Fatal("expected malformed MULTI acknowledgement to fail")
	}
}

type sessionResponseProvider struct {
	Executor
	multiResponse any
}

func (p *sessionResponseProvider) acquireCommandSession(context.Context, ...any) (commandSession, error) {
	return &responseCommandSession{response: p.multiResponse}, nil
}

type responseCommandSession struct {
	response any
}

func (s *responseCommandSession) Do(context.Context, ...any) (any, error) { return s.response, nil }
func (s *responseCommandSession) Abort(error)                             {}
func (s *responseCommandSession) Release()                                {}

func TestTransactionParentCancellationInterruptsActiveCommand(t *testing.T) {
	session := &blockingTransactionSession{entered: make(chan struct{}), aborted: make(chan struct{})}
	client := NewClientWithExecutor(&blockingTransactionProvider{session: session})
	txCtx, cancel := context.WithCancel(context.Background())
	tx, err := client.Transaction(txCtx)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := tx.Command(context.Background(), "SET", "key", "value")
		done <- err
	}()
	select {
	case <-session.entered:
	case <-time.After(time.Second):
		t.Fatal("transaction command did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("transaction command error = %v, want context cancellation", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("transaction context cancellation did not interrupt the active command")
	}
}

func TestGenericCommandRejectsConnectionStateOperations(t *testing.T) {
	exec := &transactionRecordingExecutor{calls: make(chan []any, 1)}
	client := NewClientWithExecutor(exec)
	for _, args := range [][]any{{"MULTI"}, {"COMMAND_EXEC", "WATCH", "key"}} {
		if _, err := client.Command(context.Background(), args...); err == nil {
			t.Fatalf("generic command accepted connection-state operation %#v", args)
		}
	}
	select {
	case call := <-exec.calls:
		t.Fatalf("rejected stateful command reached executor: %#v", call)
	default:
	}
}

func TestNativeTransactionDoesNotReconnectOntoAnotherSession(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()

	secondCommand := make(chan nativeFrame, 1)
	errCh := make(chan error, 1)
	go func() {
		first, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		reader := bufio.NewReader(first)
		writer := bufio.NewWriter(first)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			errCh <- err
			return
		}
		multi, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, multi, nativeStatusOK, []byte("OK")); err != nil {
			errCh <- err
			return
		}
		_ = first.Close()

		_ = listener.(*net.TCPListener).SetDeadline(time.Now().Add(250 * time.Millisecond))
		second, err := listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				errCh <- nil
				return
			}
			errCh <- err
			return
		}
		defer func() { _ = second.Close() }()
		reader = bufio.NewReader(second)
		writer = bufio.NewWriter(second)
		startup, err = readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			errCh <- err
			return
		}
		command, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		secondCommand <- command
		_ = writeNativeTestResponse(writer, command, nativeStatusOK, []byte("QUEUED"))
		errCh <- nil
	}()

	client := NewClient(listener.Addr().String(), WithNativeOptions(
		WithNativeTimeout(time.Second),
		WithNativeReconnect(1),
	))
	defer func() { _ = client.Close() }()
	tx, err := client.Transaction(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, err := tx.Command(context.Background(), "SET", "key", "value"); err == nil {
		t.Fatal("expected disconnected transaction session to fail")
	}
	select {
	case frame := <-secondCommand:
		t.Fatalf("transaction command was replayed on a new connection: opcode=%d", frame.opcode)
	case <-time.After(300 * time.Millisecond):
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

type executorFunc func(context.Context, ...any) (any, error)

func (f executorFunc) Do(ctx context.Context, args ...any) (any, error) {
	return f(ctx, args...)
}
