package ferricstore

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

func (e *NativeExecutor) readerLoop(conn net.Conn, reader *bufio.Reader, policies ...nativeResponsePolicy) {
	policy := nativeResponsePolicy{maxBytes: nativeMaxFrameBytes}
	if len(policies) > 0 {
		policy = policies[0]
	}
	assembler := newNativeResponseAssembler(policy.maxBytes, nativeMaxResponseChunkFrames, policy.codecs)
	for {
		wireFrame, err := readNativeFrameWithLimit(reader, policy.maxBytes)
		if err != nil {
			e.closeConnAndFailPendingIfCurrent(conn, err)
			return
		}
		current, err := e.validateNativeWireFrameTuple(conn, wireFrame)
		if err != nil {
			e.closeConnAndFailPendingIfCurrent(conn, err)
			return
		}
		if !current {
			return
		}
		assembled, err := assembler.add(wireFrame)
		if err != nil {
			e.closeConnAndFailPendingIfCurrent(conn, err)
			return
		}
		if assembled == nil {
			continue
		}
		frame := *assembled
		if frame.requestID == 0 {
			if err := validateNativeServerInitiatedResponse(frame); err != nil {
				e.closeConnAndFailPendingIfCurrent(conn, err)
				return
			}
			e.mu.Lock()
			current := e.conn == conn
			e.mu.Unlock()
			if !current {
				return
			}
			if frame.opcode == nativeOpGoAway {
				e.handleGoAway(conn)
			}
			event := nativeServerEvent{
				flags: frame.flags, laneID: frame.laneID, opcode: frame.opcode, value: frame.value,
				wireBytes: frame.wireBytes,
			}
			if handler := nativeConfiguredEventHandler(e.opts.eventSubscription); handler != nil {
				handler(event)
			}
			e.deliverEvent(event)
			continue
		}
		e.mu.Lock()
		if e.conn != conn {
			e.mu.Unlock()
			return
		}
		pending := e.pending[frame.requestID]
		if pending != nil && (pending.opcode != frame.opcode || pending.laneID != frame.laneID) {
			e.mu.Unlock()
			mismatchErr := fmt.Errorf(
				"ferricstore native response mismatch: got lane=%d opcode=%d request=%d",
				frame.laneID, frame.opcode, frame.requestID,
			)
			e.closeConnAndFailPendingIfCurrent(conn, mismatchErr)
			return
		}
		delete(e.pending, frame.requestID)
		var drained map[uint64]*nativePendingRequest
		if e.conn == conn && e.goAway && !e.hasLivePendingLocked() {
			drained = e.takePendingLocked()
			_ = e.closeConnLocked()
		}
		abandoned := pending != nil && pending.abandoned
		e.mu.Unlock()
		if pending != nil {
			releaseNativePending(pending)
			if !abandoned {
				pending.responseCh <- frame
			}
		}
		failNativePending(drained, errNativeConnectionUnavailable)
	}
}

func (e *NativeExecutor) validateNativeWireFrameTuple(conn net.Conn, frame nativeFrame) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.conn != conn {
		return false, nil
	}
	e.lastActivityUnixNano.Store(time.Now().UnixNano())
	if frame.requestID == 0 {
		return true, nil
	}
	pending := e.pending[frame.requestID]
	if pending == nil {
		// Canceled control requests are removed immediately and their late lane-0
		// replies are harmless. Data-lane requests remain pending while draining,
		// so an unknown data request ID can only be a protocol violation.
		if frame.laneID == 0 {
			return true, nil
		}
		return true, fmt.Errorf(
			"ferricstore native response uses unknown data request ID %d", frame.requestID,
		)
	}
	if pending.opcode == frame.opcode && pending.laneID == frame.laneID {
		return true, nil
	}
	return true, fmt.Errorf(
		"ferricstore native response mismatch: got lane=%d opcode=%d request=%d",
		frame.laneID, frame.opcode, frame.requestID,
	)
}

func (e *NativeExecutor) handleGoAway(conn net.Conn) {
	// Serialize the transition with request writes: every request is either
	// fully written before draining starts, or rejected without writing.
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	e.mu.Lock()
	if e.conn != conn || e.goAway {
		e.mu.Unlock()
		return
	}
	e.goAway = true
	e.goAwayDone = make(chan struct{})
	done := e.goAwayDone
	e.stopHeartbeatLocked()
	var pending map[uint64]*nativePendingRequest
	if !e.hasLivePendingLocked() {
		pending = e.takePendingLocked()
		_ = e.closeConnLocked()
	}
	e.mu.Unlock()
	failNativePending(pending, errNativeConnectionUnavailable)
	e.scheduleGoAwayDrain(conn, done)
}

func (e *NativeExecutor) closeConnAndFailPendingIfCurrent(conn net.Conn, err error) bool {
	e.mu.Lock()
	if e.conn != conn {
		e.mu.Unlock()
		return false
	}
	pending := e.takePendingLocked()
	_ = e.closeConnLocked()
	e.mu.Unlock()
	failNativePending(pending, err)
	return true
}

func (e *NativeExecutor) closeConnAndFailPending(err error) error {
	e.mu.Lock()
	pending := e.takePendingLocked()
	closeErr := e.closeConnLocked()
	e.mu.Unlock()
	failNativePending(pending, err)
	return closeErr
}

func (e *NativeExecutor) closeConnAndFailPendingUnlessRetiring(err error) error {
	e.mu.Lock()
	if e.retiring || e.isClosed {
		e.mu.Unlock()
		return nil
	}
	pending := e.takePendingLocked()
	closeErr := e.closeConnLocked()
	e.mu.Unlock()
	failNativePending(pending, err)
	return closeErr
}
