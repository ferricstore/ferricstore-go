package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func (e *NativeExecutor) request(ctx context.Context, opcode uint16, laneID uint32, payload any, flags byte) (any, error) {
	return e.requestWithBudget(ctx, opcode, laneID, payload, flags, nativeRequestBudget{})
}

func (e *NativeExecutor) requestWithBudget(ctx context.Context, opcode uint16, laneID uint32, payload any, flags byte, budget nativeRequestBudget) (any, error) {
	if err := e.sessionGate.readLock(ctx); err != nil {
		return nil, err
	}
	defer e.sessionGate.readUnlock()
	return e.requestWithoutSessionGate(ctx, opcode, laneID, payload, flags, budget)
}

func (e *NativeExecutor) requestWithoutSessionGate(ctx context.Context, opcode uint16, laneID uint32, payload any, flags byte, budget nativeRequestBudget) (any, error) {
	if err := e.beginRequest(); err != nil {
		return nil, err
	}
	defer e.endRequest()
	if ctx == nil {
		ctx = context.Background()
	}
	useDefaultWriteTimeout := !budget.disableDefault
	if timeout := nativeEffectiveTimeout(e.opts.Timeout, budget); timeout > 0 {
		if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > timeout {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
	}
	maxRetries := e.opts.ReconnectMaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	transportAttempts := 0
	serverRetries := 0
	for {
		value, err, retryable := e.requestOnce(ctx, opcode, laneID, payload, flags, useDefaultWriteTimeout)
		if err == nil {
			return value, nil
		}
		if errors.Is(err, errNativeGoAway) && ctx.Err() == nil {
			// GOAWAY is observed before this request is written. Waiting for the
			// old connection to drain and resubmitting is safe and is not a
			// transport-retry budget event.
			continue
		}
		disposition := nativeServerRetryDisposition(err)
		if disposition.busy && disposition.retryable && serverRetries < nativeMaxServerRetries && ctx.Err() == nil {
			serverRetries++
			if waitErr := waitNativeRetry(ctx, disposition.retryAfter); waitErr != nil {
				return nil, waitErr
			}
			continue
		}
		if !retryable || transportAttempts >= maxRetries || ctx.Err() != nil {
			return nil, err
		}
		transportAttempts++
		e.mu.Lock()
		closed := e.isClosed
		e.mu.Unlock()
		if closed {
			return nil, err
		}
		_ = e.closeConnAndFailPending(err)
	}
}

func nativeEffectiveTimeout(base time.Duration, budget nativeRequestBudget) time.Duration {
	if budget.disableDefault || base <= 0 {
		return 0
	}
	if budget.extension <= 0 {
		return base
	}
	if budget.extension > time.Duration(1<<63-1)-base {
		return 0
	}
	return base + budget.extension
}

func (e *NativeExecutor) beginRequest() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.isClosed || e.retiring {
		return net.ErrClosed
	}
	e.activeRequests++
	return nil
}

func (e *NativeExecutor) endRequest() {
	e.mu.Lock()
	if e.activeRequests > 0 {
		e.activeRequests--
	}
	if e.activeRequests != 0 || !e.retiring || e.isClosed {
		e.mu.Unlock()
		return
	}
	e.markClosedLocked()
	pending := e.takePendingLocked()
	_ = e.closeConnLocked()
	e.mu.Unlock()
	failNativePending(pending, net.ErrClosed)
}

func (e *NativeExecutor) retire() {
	e.mu.Lock()
	if e.isClosed || e.retiring {
		e.mu.Unlock()
		return
	}
	e.retiring = true
	if e.connectInFlight != nil && e.connectInFlight.cancel != nil {
		e.connectInFlight.cancel()
	}
	e.stopHeartbeatLocked()
	if e.activeRequests != 0 {
		e.mu.Unlock()
		return
	}
	e.markClosedLocked()
	pending := e.takePendingLocked()
	_ = e.closeConnLocked()
	e.mu.Unlock()
	failNativePending(pending, net.ErrClosed)
}

func (e *NativeExecutor) markClosedLocked() {
	if e.isClosed {
		return
	}
	e.isClosed = true
	if e.connectInFlight != nil && e.connectInFlight.cancel != nil {
		e.connectInFlight.cancel()
	}
	if e.closed == nil {
		e.closed = make(chan struct{})
	}
	close(e.closed)
}

func (e *NativeExecutor) requestOnce(ctx context.Context, opcode uint16, laneID uint32, payload any, flags byte, useDefaultWriteTimeout bool) (any, error, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := e.ensureConnectedLocked(ctx); err != nil {
		return nil, err, isNativeReconnectableTransportError(err)
	}
	if laneID == nativeAutoLaneID {
		laneID = e.nextDataLane()
	}
	e.mu.Lock()
	conn := e.conn
	e.mu.Unlock()
	return e.requestOnceOnConnection(ctx, opcode, laneID, payload, flags, conn, useDefaultWriteTimeout)
}

func (e *NativeExecutor) requestOnceOnConnection(ctx context.Context, opcode uint16, laneID uint32, payload any, flags byte, expected net.Conn, useDefaultWriteTimeout bool) (any, error, bool) {
	if expected == nil {
		return nil, errTransactionConnectionLost, false
	}
	var flowCredit *nativeFlowController
	if nativeOpcodeUsesFlowCredit(opcode) {
		var err error
		flowCredit, err = e.acquireFlowCredit(ctx, expected, laneID)
		if err != nil {
			return nil, err, errors.Is(err, errNativeConnectionUnavailable)
		}
	}
	responseCh := make(chan nativeResponse, 1)
	pendingRequest := &nativePendingRequest{
		responseCh: responseCh,
		opcode:     opcode,
		laneID:     laneID,
		flowCredit: flowCredit,
	}
	requestID := e.addPendingRequest(pendingRequest)

	conn, err := e.writeRequest(ctx, opcode, laneID, requestID, payload, flags, expected, useDefaultWriteTimeout)
	if err != nil {
		releaseNativePending(e.removePending(requestID))
		if errors.Is(err, errNativeGoAway) {
			return nil, err, true
		}
		if conn != nil {
			e.closeConnAndFailPendingIfCurrent(conn, err)
		}
		return nil, err, errors.Is(err, errNativeConnectionUnavailable)
	}

	select {
	case frame := <-responseCh:
		if frame.err != nil {
			return nil, frame.err, false
		}
		if frame.opcode != opcode || frame.laneID != laneID {
			mismatchErr := fmt.Errorf("ferricstore native response mismatch: got lane=%d opcode=%d request=%d", frame.laneID, frame.opcode, frame.requestID)
			e.closeConnAndFailPendingIfCurrent(conn, mismatchErr)
			return nil, mismatchErr, false
		}
		if frame.status != nativeStatusOK {
			return nil, NativeError{Status: frame.status, Value: frame.value}, false
		}
		if opcode == nativeOpWindowUpdate {
			e.applyFlowControlLimits(conn, frame.value)
		}
		e.lastActivityUnixNano.Store(time.Now().UnixNano())
		return frame.value, nil, false
	case <-ctx.Done():
		if nativeOpcodeDrainsOnCancellation(opcode) {
			// The native protocol has no per-request cancellation frame. Drain the
			// connection without admitting new work, preserve unrelated requests
			// already in flight, then reset it so an infinite blocker cannot retain
			// a flow-control credit or prevent a topology refresh retry forever.
			e.cancelPendingDataRequest(requestID, conn)
		} else {
			// Control requests do not consume flow credit, so a late response can
			// be ignored safely without retaining canceled entries indefinitely.
			releaseNativePending(e.removePending(requestID))
		}
		return nil, ctx.Err(), false
	}
}

func (e *NativeExecutor) addPendingRequest(request *nativePendingRequest) uint64 {
	e.mu.Lock()
	if e.pending == nil {
		e.pending = make(map[uint64]*nativePendingRequest)
	}
	requestID := e.nextRequestIDLocked()
	e.pending[requestID] = request
	e.mu.Unlock()
	return requestID
}

func (e *NativeExecutor) nextRequestID() uint64 {
	e.mu.Lock()
	requestID := e.nextRequestIDLocked()
	e.mu.Unlock()
	return requestID
}

func (e *NativeExecutor) nextRequestIDLocked() uint64 {
	for {
		requestID := atomic.AddUint64(&e.nextID, 1)
		if requestID == 0 {
			continue
		}
		if _, exists := e.pending[requestID]; !exists {
			return requestID
		}
	}
}

func nativeOpcodeUsesFlowCredit(opcode uint16) bool {
	return opcode >= nativeOpCommandExec || opcode == nativeOpPipeline
}

func nativeOpcodeDrainsOnCancellation(opcode uint16) bool {
	return nativeOpcodeUsesFlowCredit(opcode) || opcode == nativeOpShards
}

type nativeCommandSession struct {
	exec   *NativeExecutor
	conn   net.Conn
	lane   uint32
	once   sync.Once
	closed atomic.Bool
	mu     sync.Mutex
}

func (e *NativeExecutor) acquireCommandSession(ctx context.Context, _ ...any) (commandSession, error) {
	return e.acquireCommandSessionOnLane(ctx, nativeAutoLaneID)
}

func (e *NativeExecutor) acquireCommandSessionOnLane(ctx context.Context, lane uint32) (commandSession, error) {
	if err := e.sessionGate.lock(ctx); err != nil {
		return nil, err
	}
	if err := e.beginRequest(); err != nil {
		e.sessionGate.unlock()
		return nil, err
	}
	if err := e.ensureConnectedLocked(ctx); err != nil {
		e.endRequest()
		e.sessionGate.unlock()
		return nil, err
	}
	e.mu.Lock()
	conn := e.conn
	e.mu.Unlock()
	if conn == nil {
		e.endRequest()
		e.sessionGate.unlock()
		return nil, errTransactionConnectionLost
	}
	if lane == nativeAutoLaneID {
		lane = e.nextDataLane()
	}
	if lane == 0 {
		e.endRequest()
		e.sessionGate.unlock()
		return nil, errors.New("ferricstore transaction requires a data lane")
	}
	return &nativeCommandSession{exec: e, conn: conn, lane: lane}, nil
}

func (s *nativeCommandSession) Do(ctx context.Context, args ...any) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return nil, errTransactionConnectionLost
	}
	command, err := buildNativeCommand(args)
	if err != nil {
		return nil, err
	}
	command.budget = blockingCommandBudget(args)
	if command.laneID != 0 {
		command.laneID = s.lane
	}
	requestCtx, cancel := nativeContextWithBudget(ctx, s.exec.opts.Timeout, command.budget)
	if cancel != nil {
		defer cancel()
	}
	value, err, _ := s.exec.requestOnceOnConnection(requestCtx, command.opcode, command.laneID, command.payload, command.flags, s.conn, !command.budget.disableDefault)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", command.name, err)
	}
	return value, nil
}

func nativeContextWithBudget(ctx context.Context, base time.Duration, budget nativeRequestBudget) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := nativeEffectiveTimeout(base, budget)
	if timeout <= 0 {
		return ctx, nil
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= timeout {
		return ctx, nil
	}
	return context.WithTimeout(ctx, timeout)
}

func (s *nativeCommandSession) Abort(err error) {
	s.once.Do(func() {
		s.closed.Store(true)
		if err == nil {
			err = errTransactionConnectionLost
		}
		s.exec.closeConnAndFailPendingIfCurrent(s.conn, err)
		s.exec.endRequest()
		s.exec.sessionGate.unlock()
	})
}

func (s *nativeCommandSession) Release() {
	s.once.Do(func() {
		s.closed.Store(true)
		s.exec.endRequest()
		s.exec.sessionGate.unlock()
	})
}

func isNativeReconnectableTransportError(err error) bool {
	return errors.Is(err, errNativeConnectionUnavailable) ||
		errors.Is(err, errNativeGoAway) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE)
}
