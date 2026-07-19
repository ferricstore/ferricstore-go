package ferricstore

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"time"
)

func (e *NativeExecutor) ensureConnectedLocked(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		e.mu.Lock()
		if e.conn != nil && e.goAway {
			done := e.goAwayDone
			e.mu.Unlock()
			if done == nil {
				return errNativeGoAway
			}
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if e.conn != nil {
			e.mu.Unlock()
			return nil
		}
		if e.isClosed || e.retiring {
			e.mu.Unlock()
			return net.ErrClosed
		}
		attempt := e.connectInFlight
		startAttempt := false
		var options NativeOptions
		var replayWindow map[string]any
		var connectCtx context.Context
		if attempt == nil {
			options = e.opts
			replayWindow = make(map[string]any, len(e.replayWindowUpdate))
			for key, value := range e.replayWindowUpdate {
				replayWindow[key] = value
			}
			var cancel context.CancelFunc
			connectCtx, cancel = nativeConnectAttemptContext(options.Timeout)
			attempt = &nativeConnectAttempt{done: make(chan struct{}), cancel: cancel}
			e.connectInFlight = attempt
			startAttempt = true
		}
		attempt.waiters++
		e.mu.Unlock()
		if startAttempt {
			go e.runConnectAttempt(connectCtx, attempt, options, replayWindow)
		}

		completed := false
		select {
		case <-attempt.done:
			completed = true
		case <-ctx.Done():
		}
		e.releaseConnectWaiter(attempt)
		if err := ctx.Err(); err != nil {
			return err
		}
		if completed && attempt.err != nil {
			return attempt.err
		}
	}
}

func (e *NativeExecutor) releaseConnectWaiter(attempt *nativeConnectAttempt) {
	var cancel context.CancelFunc
	e.mu.Lock()
	if attempt.waiters > 0 {
		attempt.waiters--
	}
	if attempt.waiters == 0 && e.connectInFlight == attempt && e.conn == nil {
		e.connectInFlight = nil
		cancel = attempt.cancel
	}
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func nativeConnectAttemptContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout > 0 {
		return context.WithTimeout(context.Background(), timeout)
	}
	return context.WithCancel(context.Background())
}

func (e *NativeExecutor) runConnectAttempt(ctx context.Context, attempt *nativeConnectAttempt, options NativeOptions, replayWindow map[string]any) {
	transport, err := e.openNativeConnection(ctx, options, replayWindow)
	attempt.cancel()

	e.mu.Lock()
	if err == nil {
		if e.isClosed || e.retiring || e.connectInFlight != attempt {
			err = net.ErrClosed
		} else {
			e.installNativeConnectionLocked(transport)
			go e.readerLoop(transport.conn, transport.reader, nativeResponsePolicy{
				maxBytes: transport.contract.maxResponseBytes,
				codecs:   transport.contract.responseCodecs,
			})
		}
	}
	attempt.err = err
	if e.connectInFlight == attempt {
		e.connectInFlight = nil
	}
	close(attempt.done)
	e.mu.Unlock()

	if err != nil && transport != nil && transport.conn != nil {
		_ = transport.conn.Close()
	}
}

func (e *NativeExecutor) openNativeConnection(ctx context.Context, options NativeOptions, replayWindow map[string]any) (*nativeConnectedTransport, error) {
	startupEvents := nativeStartupEvents(options)
	dialer := options.Dialer
	if dialer == nil {
		dialer = &net.Dialer{Timeout: options.Timeout, KeepAlive: 30 * time.Second}
	}
	conn, err := dialer.DialContext(ctx, "tcp", options.Addr)
	if err != nil {
		return nil, err
	}
	rawConn := conn
	succeeded := false
	defer func() {
		if !succeeded {
			_ = rawConn.Close()
		}
	}()
	stopCancel := context.AfterFunc(ctx, func() { _ = rawConn.Close() })
	defer stopCancel()
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
	}
	if options.TLS {
		config := options.TLSConfig
		if config == nil {
			config = &tls.Config{MinVersion: tls.VersionTLS12}
		} else {
			config = config.Clone()
		}
		if config.ServerName == "" {
			host, _, splitErr := net.SplitHostPort(options.Addr)
			if splitErr == nil {
				config.ServerName = host
			}
		}
		tlsConn := tls.Client(conn, config)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			return nil, err
		}
		conn = tlsConn
	}
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	hello := map[string]any{
		"client_name": nativeClientName(options.ClientName),
		"compression": "none",
	}
	helloResponse, err := e.nativeHandshakeRequest(
		ctx, options.Timeout, conn, reader, writer,
		nativeUnauthenticatedFrameBytes, nativeOpHello, hello, options.MaxResponseBytes,
	)
	if err != nil {
		return nil, err
	}
	contract, err := parseNativeHelloContract(helloResponse, options.MaxResponseBytes)
	if err != nil {
		return nil, err
	}
	if contract.authRequired && !options.credentialsSet {
		return nil, errors.New("ferricstore native HELLO requires authentication but no credentials were configured")
	}
	if options.credentialsSet {
		username := options.Username
		if username == "" {
			username = "default"
		}
		auth := map[string]any{"username": username, "password": options.Password}
		if _, err := e.nativeHandshakeRequest(
			ctx, options.Timeout, conn, reader, writer,
			nativeUnauthenticatedFrameBytes, nativeOpAuth, auth, contract.maxResponseBytes,
		); err != nil {
			return nil, err
		}
	}
	contract = constrainNativeContractForAuthentication(contract, options.credentialsSet)
	var windowResponse any
	if len(replayWindow) > 0 {
		windowResponse, err = e.nativeHandshakeRequest(ctx, options.Timeout, conn, reader, writer, contract.maxRequestFrameBytes, nativeOpWindowUpdate, replayWindow, contract.maxResponseBytes)
		if err != nil {
			return nil, err
		}
	}
	if contract.supportsEvents(startupEvents) {
		payload := map[string]any{"events": startupEvents}
		if _, err := e.nativeHandshakeRequest(ctx, options.Timeout, conn, reader, writer, contract.maxRequestFrameBytes, nativeOpSubscribeEvents, payload, contract.maxResponseBytes); err != nil {
			return nil, err
		}
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	succeeded = true
	return &nativeConnectedTransport{
		conn: conn, reader: reader, writer: writer,
		helloResponse: helloResponse, windowResponse: windowResponse, contract: contract,
	}, nil
}

func (e *NativeExecutor) nativeHandshakeRequest(ctx context.Context, timeout time.Duration, conn net.Conn, reader *bufio.Reader, writer *bufio.Writer, maxFrameBytes int, opcode uint16, payload any, maxResponseBytes ...int) (any, error) {
	body, err := encodeNativeValueWithLimit(payload, maxFrameBytes)
	if err != nil {
		return nil, err
	}
	deadline, hasDeadline := ctx.Deadline()
	if timeout > 0 {
		transportDeadline := time.Now().Add(timeout)
		if !hasDeadline || transportDeadline.Before(deadline) {
			deadline = transportDeadline
			hasDeadline = true
		}
	}
	if hasDeadline {
		if err := conn.SetDeadline(deadline); err != nil {
			return nil, err
		}
	}
	requestID := e.nextRequestID()
	header := make([]byte, nativeHeaderLen)
	copy(header[0:4], nativeMagic)
	header[4] = nativeRequestVersion
	binary.BigEndian.PutUint16(header[10:12], opcode)
	binary.BigEndian.PutUint64(header[12:20], requestID)
	binary.BigEndian.PutUint32(header[20:24], uint32(len(body)))
	if _, err := writer.Write(header); err != nil {
		return nil, err
	}
	if len(body) > 0 {
		if _, err := writer.Write(body); err != nil {
			return nil, err
		}
	}
	if err := writer.Flush(); err != nil {
		return nil, err
	}
	responseLimit := nativeMaxFrameBytes
	if len(maxResponseBytes) > 0 && maxResponseBytes[0] > 0 {
		responseLimit = maxResponseBytes[0]
	}
	frame, err := readNativeResponseWithPolicy(reader, nativeResponsePolicy{maxBytes: responseLimit})
	if err != nil {
		return nil, err
	}
	if err := nativeConnectionLevelError(frame); err != nil {
		return nil, err
	}
	if frame.requestID != requestID || frame.opcode != opcode || frame.laneID != 0 {
		return nil, fmt.Errorf("ferricstore native response mismatch: got lane=%d opcode=%d request=%d", frame.laneID, frame.opcode, frame.requestID)
	}
	if frame.status != nativeStatusOK {
		return nil, NativeError{Status: frame.status, Value: frame.value}
	}
	return frame.value, nil
}

func (e *NativeExecutor) installNativeConnectionLocked(transport *nativeConnectedTransport) {
	e.conn = transport.conn
	e.reader = transport.reader
	e.writer = transport.writer
	contract := transport.contract
	e.applyHelloContractLocked(contract)
	if transport.windowResponse != nil {
		connectionLimit, laneLimit := nativeFlowControlLimits(transport.windowResponse)
		e.flow.updateLimits(connectionLimit, laneLimit)
	}
	if e.pending == nil {
		e.pending = make(map[uint64]*nativePendingRequest)
	}
	if e.eventDeliveryEnabled && e.events == nil {
		e.events = make(chan nativeQueuedEvent, nativeEventBufferCapacity)
	}
	e.connectionDone = make(chan struct{})
	e.connectionGeneration++
	if e.connectionGeneration == 0 {
		e.connectionGeneration++
	}
	e.lastActivityUnixNano.Store(time.Now().UnixNano())
	e.startHeartbeatLocked()
}

func (e *NativeExecutor) startHeartbeatLocked() {
	interval := e.opts.HeartbeatInterval
	if interval <= 0 || e.heartbeatStop != nil {
		return
	}
	stop := make(chan struct{})
	e.heartbeatStop = stop
	go e.heartbeatLoop(stop, interval, e.opts.HeartbeatTimeout)
}

func (e *NativeExecutor) stopHeartbeatLocked() {
	if e.heartbeatStop == nil {
		return
	}
	close(e.heartbeatStop)
	e.heartbeatStop = nil
}

func (e *NativeExecutor) heartbeatLoop(stop <-chan struct{}, interval, timeout time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			last := e.lastActivityUnixNano.Load()
			if last != 0 && time.Since(time.Unix(0, last)) < interval {
				continue
			}
			if !e.sessionGate.tryReadLock() {
				continue
			}
			ctx := context.Background()
			var cancel context.CancelFunc
			if timeout > 0 {
				ctx, cancel = context.WithTimeout(ctx, timeout)
			}
			command, buildErr := buildNativeCommand([]any{"PING"})
			var err error
			if buildErr != nil {
				err = buildErr
			} else {
				_, err = e.requestWithoutSessionGate(ctx, command.opcode, command.laneID, command.payload, command.flags, nativeRequestBudget{})
			}
			if cancel != nil {
				cancel()
			}
			e.sessionGate.readUnlock()
			if err != nil {
				_ = e.closeConnAndFailPendingUnlessRetiring(err)
				return
			}
		}
	}
}

func (e *NativeExecutor) writeRequest(ctx context.Context, opcode uint16, laneID uint32, requestID uint64, payload any, flags byte, expected net.Conn, _ bool) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var body []byte
	var err error
	e.mu.Lock()
	maxFrameBytes := e.maxRequestFrameBytes
	e.mu.Unlock()
	if maxFrameBytes <= 0 {
		maxFrameBytes = nativeDefaultRequestFrameBytes
	}
	// Allow one request to encode while the current writer is blocked. Holding
	// this admission lock until writeMu is acquired bounds the connection to at
	// most one writing body plus one encoded waiter without giving up encode/I/O
	// overlap.
	if err := e.writeEncodeMu.LockContext(ctx); err != nil {
		return nil, err
	}
	if preencoded, ok := payload.(nativePreencodedPayload); ok {
		body = preencoded.body
	} else if flags&nativeFlagCustomPayload != 0 {
		switch raw := payload.(type) {
		case []byte:
			body = raw
		case nativeCustomPayloadEncoder:
			body, err = raw.encodeNativeCustomPayload(maxFrameBytes)
			if err != nil {
				e.writeEncodeMu.Unlock()
				return nil, err
			}
		default:
			e.writeEncodeMu.Unlock()
			return nil, errors.New("ferricstore native custom payload must be raw bytes")
		}
	} else {
		body, err = encodeNativeValueWithLimit(payload, maxFrameBytes)
		if err != nil {
			e.writeEncodeMu.Unlock()
			return nil, err
		}
	}
	if len(body) > math.MaxUint32 {
		e.writeEncodeMu.Unlock()
		return nil, errors.New("ferricstore native request body is too large")
	}
	if len(body) > maxFrameBytes {
		e.writeEncodeMu.Unlock()
		return nil, fmt.Errorf("ferricstore native request body exceeds server-advertised %d-byte frame limit", maxFrameBytes)
	}

	if err := e.writeMu.LockContext(ctx); err != nil {
		e.writeEncodeMu.Unlock()
		return nil, err
	}
	e.writeEncodeMu.Unlock()
	defer e.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.mu.Lock()
	conn := e.conn
	writer := e.writer
	draining := e.goAway && conn == expected
	e.mu.Unlock()
	if conn == nil || writer == nil || conn != expected {
		return conn, fmt.Errorf("%w: %w", errNativeConnectionUnavailable, net.ErrClosed)
	}
	if draining {
		return conn, errNativeGoAway
	}
	deadline, hasDeadline := ctx.Deadline()
	if e.opts.Timeout > 0 {
		transportDeadline := time.Now().Add(e.opts.Timeout)
		if !hasDeadline || transportDeadline.Before(deadline) {
			deadline = transportDeadline
			hasDeadline = true
		}
	}
	if hasDeadline {
		_ = conn.SetWriteDeadline(deadline)
	}
	cancelWriteDone := make(chan struct{})
	stopCancelWrite := context.AfterFunc(ctx, func() {
		_ = conn.SetWriteDeadline(time.Now())
		close(cancelWriteDone)
	})
	defer func() {
		if !stopCancelWrite() {
			<-cancelWriteDone
		}
		_ = conn.SetWriteDeadline(time.Time{})
	}()

	header := make([]byte, nativeHeaderLen)
	copy(header[0:4], nativeMagic)
	header[4] = nativeRequestVersion
	header[5] = flags
	binary.BigEndian.PutUint32(header[6:10], laneID)
	binary.BigEndian.PutUint16(header[10:12], opcode)
	binary.BigEndian.PutUint64(header[12:20], requestID)
	binary.BigEndian.PutUint32(header[20:24], uint32(len(body)))

	if _, err := writer.Write(header); err != nil {
		return conn, nativeWriteContextError(ctx, err)
	}
	if len(body) > 0 {
		if _, err := writer.Write(body); err != nil {
			return conn, nativeWriteContextError(ctx, err)
		}
	}
	return conn, nativeWriteContextError(ctx, writer.Flush())
}

func nativeWriteContextError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	// A socket deadline and its context timer share the same absolute deadline,
	// but the kernel can report the write timeout before the timer goroutine has
	// published ctx.Err(). Preserve the caller-visible context contract in that
	// small notification window.
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return err
}
