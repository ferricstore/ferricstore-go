package ferricstore

import (
	"context"
	"errors"
	"net"
	"time"
)

func (e *NativeExecutor) takePendingLocked() map[uint64]*nativePendingRequest {
	pending := e.pending
	e.pending = make(map[uint64]*nativePendingRequest)
	return pending
}

func failNativePending(pending map[uint64]*nativePendingRequest, err error) {
	for _, request := range pending {
		releaseNativePending(request)
		if !request.abandoned {
			request.responseCh <- nativeResponse{err: err}
		}
	}
}

func releaseNativePending(request *nativePendingRequest) {
	if request != nil && request.flowCredit != nil {
		request.flowCredit.release(request.laneID)
		request.flowCredit = nil
	}
}

func (e *NativeExecutor) removePending(requestID uint64) *nativePendingRequest {
	e.mu.Lock()
	request := e.pending[requestID]
	delete(e.pending, requestID)
	var drained map[uint64]*nativePendingRequest
	if e.goAway && !e.hasLivePendingLocked() {
		drained = e.takePendingLocked()
		_ = e.closeConnLocked()
	}
	e.mu.Unlock()
	failNativePending(drained, errNativeConnectionUnavailable)
	return request
}

func (e *NativeExecutor) hasLivePendingLocked() bool {
	for _, request := range e.pending {
		if !request.abandoned {
			return true
		}
	}
	return false
}

func (e *NativeExecutor) cancelPendingDataRequest(requestID uint64, conn net.Conn) {
	// Serialize the draining transition with request writes. A concurrent
	// request is either already on the wire and allowed to finish, or sees
	// GOAWAY locally and waits for the replacement connection.
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	e.mu.Lock()
	request := e.pending[requestID]
	if request == nil {
		e.mu.Unlock()
		return
	}
	request.abandoned = true
	if e.conn != conn {
		delete(e.pending, requestID)
		e.mu.Unlock()
		releaseNativePending(request)
		return
	}
	var drainDone <-chan struct{}
	if !e.goAway {
		e.goAway = true
		e.goAwayDone = make(chan struct{})
		drainDone = e.goAwayDone
		e.stopHeartbeatLocked()
	}
	var pending map[uint64]*nativePendingRequest
	if !e.hasLivePendingLocked() {
		pending = e.takePendingLocked()
		_ = e.closeConnLocked()
	}
	e.mu.Unlock()
	failNativePending(pending, errNativeConnectionUnavailable)
	if drainDone != nil {
		e.scheduleGoAwayDrain(conn, drainDone)
	}
}

var errNativeGoAwayDrainTimeout = errors.New("ferricstore native GOAWAY drain timed out")

type nativeQueuedEvent struct {
	value any
	bytes int
}

func (e *NativeExecutor) scheduleGoAwayDrain(conn net.Conn, done <-chan struct{}) {
	if conn == nil || done == nil {
		return
	}
	timeout := e.opts.GoAwayDrainTimeout
	if timeout <= 0 {
		timeout = nativeDefaultGoAwayDrainTimeout
	}
	go func() {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-done:
			return
		case <-timer.C:
			e.closeConnAndFailPendingIfCurrent(conn, errNativeGoAwayDrainTimeout)
		}
	}()
}

func (e *NativeExecutor) deliverEvent(value any) {
	size := nativeBufferedEventSize(value)
	e.mu.Lock()
	events := e.events
	isClosed := e.isClosed
	if isClosed || events == nil {
		e.mu.Unlock()
		return
	}
	if size > nativeMaxBufferedEventBytes-e.eventBufferedBytes {
		e.mu.Unlock()
		e.droppedEvents.Add(1)
		return
	}
	select {
	case events <- nativeQueuedEvent{value: value, bytes: size}:
		e.eventBufferedBytes += size
		e.mu.Unlock()
	default:
		e.mu.Unlock()
		e.droppedEvents.Add(1)
	}
}

func (e *NativeExecutor) enableEventDelivery() {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.isClosed {
		return
	}
	e.eventDeliveryEnabled = true
	if e.events == nil {
		e.events = make(chan nativeQueuedEvent, nativeEventBufferCapacity)
	}
}

func nativeBufferedEventSize(value any) int {
	switch event := value.(type) {
	case nativeServerEvent:
		if event.wireBytes > 0 {
			return event.wireBytes
		}
		return nativeBufferedEventSize(event.value)
	case []byte:
		if len(event) > 0 {
			return len(event)
		}
	case string:
		if len(event) > 0 {
			return len(event)
		}
	}
	return 1
}

func (e *NativeExecutor) consumeEvent(event nativeQueuedEvent) any {
	e.mu.Lock()
	e.eventBufferedBytes -= event.bytes
	if e.eventBufferedBytes < 0 {
		e.eventBufferedBytes = 0
	}
	e.mu.Unlock()
	return event.value
}

func (e *NativeExecutor) nextEvent(ctx context.Context) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	e.mu.Lock()
	events := e.events
	isClosed := e.isClosed
	generationBefore := e.connectionGeneration
	e.mu.Unlock()
	if isClosed {
		return nil, net.ErrClosed
	}
	if events != nil {
		select {
		case event := <-events:
			return e.consumeEvent(event), nil
		default:
		}
	}
	if err := e.ensureConnected(ctx); err != nil {
		return nil, err
	}
	e.mu.Lock()
	events = e.events
	closed := e.closed
	connectionDone := e.connectionDone
	isClosed = e.isClosed
	generationAfter := e.connectionGeneration
	e.mu.Unlock()
	if isClosed || events == nil {
		return nil, net.ErrClosed
	}
	if generationBefore != 0 && generationAfter != generationBefore {
		return nil, errNativeConnectionUnavailable
	}
	select {
	case event := <-events:
		return e.consumeEvent(event), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-closed:
		return nil, net.ErrClosed
	case <-connectionDone:
		return nil, errNativeConnectionUnavailable
	}
}

func (e *NativeExecutor) ensureConnected(ctx context.Context) error {
	if err := e.sessionGate.readLock(ctx); err != nil {
		return err
	}
	defer e.sessionGate.readUnlock()
	return e.ensureConnectedLocked(ctx)
}

func (e *NativeExecutor) currentConnectionGeneration() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.connectionGeneration
}
