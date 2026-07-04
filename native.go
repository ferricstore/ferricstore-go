package ferricstore

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	nativeMagic             = "FSNP"
	nativeRequestVersion    = 0x01
	nativeResponseVersion   = 0x81
	nativeHeaderLen         = 24
	nativeDefaultPort       = "6388"
	nativeDefaultTLSPort    = "6389"
	nativeMaxFrameBytes     = 128 * 1024 * 1024
	nativeMaxContainerItems = 1_000_000

	nativeFlagCompressed    = 0x08
	nativeFlagCustomPayload = 0x02
	nativeFlagMoreChunks    = 0x20

	nativeStatusOK = 0

	nativeOpAuth                   = 0x0002
	nativeOpPing                   = 0x0003
	nativeOpStartup                = 0x000C
	nativeOpPipeline               = 0x000E
	nativeOpEvent                  = 0x0010
	nativeOpSubscribeEvents        = 0x0011
	nativeOpUnsubscribeEvents      = 0x0012
	nativeOpCommandExec            = 0x0100
	nativeOpGet                    = 0x0101
	nativeOpSet                    = 0x0102
	nativeOpDel                    = 0x0103
	nativeOpMGet                   = 0x0104
	nativeOpMSet                   = 0x0105
	nativeOpFlowClaimDue           = 0x0203
	nativeOpFlowCreateMany         = 0x020F
	nativeOpFlowCompleteMany       = 0x0210
	nativeOpFlowScheduleCreate     = 0x0225
	nativeOpFlowScheduleGet        = 0x0226
	nativeOpFlowScheduleDelete     = 0x0227
	nativeOpFlowScheduleFireDue    = 0x0228
	nativeOpFlowScheduleList       = 0x0229
	nativeOpFlowScheduleFire       = 0x022A
	nativeOpFlowSchedulePause      = 0x022B
	nativeOpFlowScheduleResume     = 0x022C
	nativeOpFlowEffectReserve      = 0x0240
	nativeOpFlowEffectConfirm      = 0x0241
	nativeOpFlowEffectFail         = 0x0242
	nativeOpFlowEffectCompensate   = 0x0243
	nativeOpFlowEffectGet          = 0x0244
	nativeOpFlowGovernanceLedger   = 0x0245
	nativeOpFlowApprovalRequest    = 0x0246
	nativeOpFlowApprovalApprove    = 0x0247
	nativeOpFlowApprovalReject     = 0x0248
	nativeOpFlowApprovalGet        = 0x0249
	nativeOpFlowCircuitOpen        = 0x024A
	nativeOpFlowCircuitClose       = 0x024B
	nativeOpFlowCircuitGet         = 0x024C
	nativeOpFlowBudgetReserve      = 0x024D
	nativeOpFlowBudgetGet          = 0x024E
	nativeOpFlowLimitLease         = 0x024F
	nativeOpFlowLimitSpend         = 0x0250
	nativeOpFlowLimitRelease       = 0x0251
	nativeOpFlowLimitGet           = 0x0252
	nativeOpFlowApprovalList       = 0x0253
	nativeOpFlowGovernanceOverview = 0x0254
	nativeOpFlowBudgetList         = 0x0255
	nativeOpFlowLimitList          = 0x0256
	nativeOpFlowBudgetCommit       = 0x0257
	nativeOpFlowBudgetRelease      = 0x0258

	nativeCompactFlowClaimJobs    = 0x80
	nativeCompactOKList           = 0x81
	nativeCompactKVGet            = 0x82
	nativeCompactKVMGet           = 0x83
	nativeCompactKVMGetFixed      = 0x89
	nativeCompactPipelineRequest  = 0x94
	nativeCompactPipelineResponse = 0x95
)

type nativeCompactOKCount int

type NativeOptions struct {
	Addr                string
	Username            string
	Password            string
	TLS                 bool
	TLSConfig           *tls.Config
	ClientName          string
	Timeout             time.Duration
	Dialer              *net.Dialer
	HeartbeatInterval   time.Duration
	HeartbeatTimeout    time.Duration
	ReconnectMaxRetries int
}

type NativeOption func(*NativeOptions)

func WithNativeCredentials(username, password string) NativeOption {
	return func(opts *NativeOptions) {
		opts.Username = username
		opts.Password = password
	}
}

func WithNativeTLS(config *tls.Config) NativeOption {
	return func(opts *NativeOptions) {
		opts.TLS = true
		opts.TLSConfig = config
	}
}

func WithNativeTimeout(timeout time.Duration) NativeOption {
	return func(opts *NativeOptions) {
		opts.Timeout = timeout
		if opts.Dialer == nil {
			opts.Dialer = &net.Dialer{}
		}
		opts.Dialer.Timeout = timeout
	}
}

func WithNativeClientName(name string) NativeOption {
	return func(opts *NativeOptions) {
		opts.ClientName = name
	}
}

func WithNativeHeartbeat(interval, timeout time.Duration) NativeOption {
	return func(opts *NativeOptions) {
		opts.HeartbeatInterval = interval
		opts.HeartbeatTimeout = timeout
	}
}

func WithNativeReconnect(maxRetries int) NativeOption {
	return func(opts *NativeOptions) {
		if maxRetries < 0 {
			maxRetries = 0
		}
		opts.ReconnectMaxRetries = maxRetries
	}
}

type NativeExecutor struct {
	opts NativeOptions

	mu                   sync.Mutex
	writeMu              sync.Mutex
	conn                 net.Conn
	reader               *bufio.Reader
	writer               *bufio.Writer
	nextID               uint64
	isClosed             bool
	deadlineSet          bool
	heartbeatStop        chan struct{}
	lastActivityUnixNano atomic.Int64
	pending              map[uint64]chan nativeResponse
	events               chan any
	droppedEvents        atomic.Uint64
}

type NativeError struct {
	Status uint16
	Value  any
}

var errNativeConnectionUnavailable = errors.New("ferricstore native connection unavailable")

func (e NativeError) Error() string {
	if message := nativeErrorMessage(e.Value); message != "" {
		return message
	}
	return fmt.Sprintf("ferricstore native error status %d", e.Status)
}

func NewNativeExecutor(addr string, opts ...NativeOption) *NativeExecutor {
	options := defaultNativeOptions(addr, false)
	for _, opt := range opts {
		opt(&options)
	}
	if options.Addr == "" {
		options.Addr = nativeAddressWithPort(addr, nativeDefaultPort)
	}
	return &NativeExecutor{opts: options}
}

func NewNativeExecutorFromURL(rawurl string, opts ...NativeOption) (*NativeExecutor, error) {
	if !strings.Contains(rawurl, "://") {
		return NewNativeExecutor(rawurl, opts...), nil
	}

	parsed, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}

	var tlsEnabled bool
	var defaultPort string
	switch strings.ToLower(parsed.Scheme) {
	case "ferric":
		defaultPort = nativeDefaultPort
	case "ferrics":
		tlsEnabled = true
		defaultPort = nativeDefaultTLSPort
	default:
		return nil, fmt.Errorf("unsupported ferricstore native URL scheme %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return nil, errors.New("ferricstore native URL requires a host")
	}

	options := defaultNativeOptions(parsed.Host, tlsEnabled)
	options.Addr = nativeAddressWithPort(parsed.Host, defaultPort)
	if parsed.User != nil {
		options.Username = parsed.User.Username()
		options.Password, _ = parsed.User.Password()
	}
	if timeout := parsed.Query().Get("timeout"); timeout != "" {
		duration, err := time.ParseDuration(timeout)
		if err != nil {
			return nil, fmt.Errorf("invalid ferricstore native timeout: %w", err)
		}
		options.Timeout = duration
		options.Dialer.Timeout = duration
	}
	for _, opt := range opts {
		opt(&options)
	}
	return &NativeExecutor{opts: options}, nil
}

func (e *NativeExecutor) Do(ctx context.Context, args ...any) (any, error) {
	return e.command(ctx, args...)
}

func (e *NativeExecutor) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.isClosed = true
	return e.closeConnLocked()
}

func (e *NativeExecutor) DroppedEvents() uint64 {
	if e == nil {
		return 0
	}
	return e.droppedEvents.Load()
}

func (e *NativeExecutor) command(ctx context.Context, args ...any) (any, error) {
	command, err := buildNativeCommand(args)
	if err != nil {
		return nil, err
	}
	value, err := e.request(ctx, command.opcode, command.laneID, command.payload, command.flags)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", command.name, err)
	}
	return value, nil
}

func (e *NativeExecutor) Pipeline(ctx context.Context, commands [][]any) ([]any, error) {
	if len(commands) == 0 {
		return nil, nil
	}
	if payload, ok, err := compactPipelinePayload(commands); ok || err != nil {
		if err != nil {
			return nil, err
		}
		value, err := e.request(ctx, nativeOpPipeline, 1, payload, nativeFlagCustomPayload)
		if err != nil {
			return nil, fmt.Errorf("PIPELINE: %w", err)
		}
		return pipelineValues(value, len(commands))
	}
	items := make([]any, 0, len(commands))
	for idx, args := range commands {
		command, err := buildNativeCommand(args)
		if err != nil {
			return nil, err
		}
		if command.flags != 0 {
			return nil, fmt.Errorf("%s: custom pipeline payloads are not supported by the Go SDK yet", command.name)
		}
		items = append(items, map[string]any{
			"opcode":     int64(command.opcode),
			"lane_id":    int64(command.laneID),
			"request_id": int64(idx + 1),
			"body":       command.payload,
		})
	}
	value, err := e.request(ctx, nativeOpPipeline, 1, map[string]any{
		"atomicity": "none",
		"commands":  items,
		"return":    "compact",
	}, 0)
	if err != nil {
		return nil, fmt.Errorf("PIPELINE: %w", err)
	}
	return pipelineValues(value, len(commands))
}

type nativeCommand struct {
	name    string
	opcode  uint16
	laneID  uint32
	payload any
	flags   byte
}

func buildNativeCommand(args []any) (nativeCommand, error) {
	if len(args) == 0 {
		return nativeCommand{}, errors.New("ferricstore command requires at least a command name")
	}
	command := strings.ToUpper(asString(args[0]))
	if command == "" {
		return nativeCommand{}, errors.New("ferricstore command name is empty")
	}
	if built, ok, err := buildBasicNativeCommand(command, args[1:]); ok || err != nil {
		return built, err
	}
	if built, ok, err := buildFlowNativeCommand(command, args[1:]); ok || err != nil {
		return built, err
	}
	return nativeCommand{
		name:   command,
		opcode: nativeOpCommandExec,
		laneID: 1,
		payload: map[string]any{
			"command": command,
			"args":    nativeCommandArgs(args[1:]),
		},
	}, nil
}

func nativeCommandArgs(args []any) []any {
	out := make([]any, 0, len(args))
	for _, arg := range args {
		out = append(out, nativeCommandArg(arg))
	}
	return out
}

func nativeCommandArg(arg any) any {
	switch arg.(type) {
	case nil, string, []byte, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, bool, float32, float64:
		return arg
	default:
		encoded, err := json.Marshal(arg)
		if err != nil {
			return fmt.Sprint(arg)
		}
		return encoded
	}
}

func buildBasicNativeCommand(name string, args []any) (nativeCommand, bool, error) {
	switch name {
	case "PING":
		payload := map[string]any{}
		if len(args) > 0 {
			payload["message"] = args[0]
		}
		return nativeCommand{name: name, opcode: nativeOpPing, laneID: 0, payload: payload}, true, nil
	case "GET":
		if len(args) < 1 {
			return nativeCommand{}, true, errors.New("GET requires key")
		}
		return nativeCommand{name: name, opcode: nativeOpGet, laneID: 1, payload: map[string]any{"key": args[0]}}, true, nil
	case "SET":
		if len(args) < 2 {
			return nativeCommand{}, true, errors.New("SET requires key and value")
		}
		if len(args) > 2 {
			return nativeCommand{}, false, nil
		}
		return nativeCommand{name: name, opcode: nativeOpSet, laneID: 1, payload: map[string]any{"key": args[0], "value": args[1]}}, true, nil
	case "DEL":
		return nativeCommand{name: name, opcode: nativeOpDel, laneID: 1, payload: map[string]any{"keys": append([]any(nil), args...)}}, true, nil
	case "MGET":
		return nativeCommand{name: name, opcode: nativeOpMGet, laneID: 1, payload: map[string]any{"keys": append([]any(nil), args...)}}, true, nil
	case "MSET":
		if len(args)%2 != 0 {
			return nativeCommand{}, true, errors.New("MSET requires key/value pairs")
		}
		pairs := make([]any, 0, len(args)/2)
		for idx := 0; idx < len(args); idx += 2 {
			pairs = append(pairs, []any{args[idx], args[idx+1]})
		}
		return nativeCommand{name: name, opcode: nativeOpMSet, laneID: 1, payload: map[string]any{"pairs": pairs}}, true, nil
	default:
		return nativeCommand{}, false, nil
	}
}

func (e *NativeExecutor) request(ctx context.Context, opcode uint16, laneID uint32, payload any, flags byte) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	maxRetries := e.opts.ReconnectMaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	for attempt := 0; ; attempt++ {
		value, err, retryable := e.requestOnce(ctx, opcode, laneID, payload, flags)
		if err == nil {
			return value, nil
		}
		if !retryable || attempt >= maxRetries || ctx.Err() != nil {
			return nil, err
		}
		e.mu.Lock()
		if e.isClosed {
			e.mu.Unlock()
			return nil, err
		}
		_ = e.closeConnLocked()
		e.mu.Unlock()
	}
}

func (e *NativeExecutor) requestOnce(ctx context.Context, opcode uint16, laneID uint32, payload any, flags byte) (any, error, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := e.ensureConnectedLocked(ctx); err != nil {
		return nil, err, isNativeReconnectableTransportError(err)
	}
	requestID := atomic.AddUint64(&e.nextID, 1)
	responseCh := make(chan nativeResponse, 1)
	e.mu.Lock()
	if e.pending == nil {
		e.pending = make(map[uint64]chan nativeResponse)
	}
	e.pending[requestID] = responseCh
	e.mu.Unlock()

	if err := e.writeRequest(ctx, opcode, laneID, requestID, payload, flags); err != nil {
		e.removePending(requestID)
		e.mu.Lock()
		_ = e.closeConnLocked()
		e.mu.Unlock()
		return nil, err, errors.Is(err, errNativeConnectionUnavailable)
	}

	select {
	case frame := <-responseCh:
		if frame.err != nil {
			return nil, frame.err, false
		}
		if frame.opcode != opcode || frame.laneID != laneID {
			e.mu.Lock()
			_ = e.closeConnLocked()
			e.mu.Unlock()
			return nil, fmt.Errorf("ferricstore native response mismatch: got lane=%d opcode=%d request=%d", frame.laneID, frame.opcode, frame.requestID), false
		}
		if frame.status != nativeStatusOK {
			return nil, NativeError{Status: frame.status, Value: frame.value}, false
		}
		e.lastActivityUnixNano.Store(time.Now().UnixNano())
		return frame.value, nil, false
	case <-ctx.Done():
		e.removePending(requestID)
		return nil, ctx.Err(), false
	}
}

func isNativeReconnectableTransportError(err error) bool {
	return errors.Is(err, errNativeConnectionUnavailable) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF)
}

func (e *NativeExecutor) ensureConnectedLocked(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.conn != nil {
		return nil
	}
	if e.isClosed {
		return net.ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}

	dialer := e.opts.Dialer
	if dialer == nil {
		dialer = &net.Dialer{Timeout: e.opts.Timeout, KeepAlive: 30 * time.Second}
	}

	conn, err := dialer.DialContext(ctx, "tcp", e.opts.Addr)
	if err != nil {
		return err
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
	}

	if e.opts.TLS {
		config := e.opts.TLSConfig
		if config == nil {
			config = &tls.Config{MinVersion: tls.VersionTLS12}
		} else {
			config = config.Clone()
		}
		if config.ServerName == "" {
			host, _, splitErr := net.SplitHostPort(e.opts.Addr)
			if splitErr == nil {
				config.ServerName = host
			}
		}
		tlsConn := tls.Client(conn, config)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return err
		}
		conn = tlsConn
	}

	reader := bufio.NewReader(conn)
	e.conn = conn
	e.reader = reader
	e.writer = bufio.NewWriter(conn)

	startup := map[string]any{
		"client_name": e.clientName(),
		"driver_name": "ferricstore-go",
		"compression": "none",
	}
	if _, err := e.requestLocked(ctx, nativeOpStartup, 0, startup, 0); err != nil {
		_ = e.closeConnLocked()
		return err
	}
	if e.opts.Password != "" {
		username := e.opts.Username
		if username == "" {
			username = "default"
		}
		auth := map[string]any{"username": username, "password": e.opts.Password}
		if _, err := e.requestLocked(ctx, nativeOpAuth, 0, auth, 0); err != nil {
			_ = e.closeConnLocked()
			return err
		}
	}
	if e.pending == nil {
		e.pending = make(map[uint64]chan nativeResponse)
	}
	if e.events == nil {
		e.events = make(chan any, 4096)
	}
	_ = conn.SetDeadline(time.Time{})
	e.deadlineSet = false
	e.lastActivityUnixNano.Store(time.Now().UnixNano())
	go e.readerLoop(conn, reader)
	e.startHeartbeatLocked()
	return nil
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
			ctx := context.Background()
			var cancel context.CancelFunc
			if timeout > 0 {
				ctx, cancel = context.WithTimeout(ctx, timeout)
			}
			_, err := e.command(ctx, "PING")
			if cancel != nil {
				cancel()
			}
			if err != nil {
				e.mu.Lock()
				_ = e.closeConnLocked()
				e.mu.Unlock()
				return
			}
		}
	}
}

func (e *NativeExecutor) requestLocked(ctx context.Context, opcode uint16, laneID uint32, payload any, flags byte) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var body []byte
	var err error
	if flags&nativeFlagCustomPayload != 0 {
		raw, ok := payload.([]byte)
		if !ok {
			return nil, errors.New("ferricstore native custom payload must be raw bytes")
		}
		body = raw
	} else {
		body, err = encodeNativeValue(payload)
		if err != nil {
			return nil, err
		}
	}
	if len(body) > math.MaxUint32 {
		return nil, errors.New("ferricstore native request body is too large")
	}
	if err := e.setDeadlineLocked(ctx); err != nil {
		return nil, err
	}

	requestID := atomic.AddUint64(&e.nextID, 1)
	header := make([]byte, nativeHeaderLen)
	copy(header[0:4], nativeMagic)
	header[4] = nativeRequestVersion
	header[5] = flags
	binary.BigEndian.PutUint32(header[6:10], laneID)
	binary.BigEndian.PutUint16(header[10:12], opcode)
	binary.BigEndian.PutUint64(header[12:20], requestID)
	binary.BigEndian.PutUint32(header[20:24], uint32(len(body)))

	if _, err := e.writer.Write(header); err != nil {
		_ = e.closeConnLocked()
		return nil, err
	}
	if len(body) > 0 {
		if _, err := e.writer.Write(body); err != nil {
			_ = e.closeConnLocked()
			return nil, err
		}
	}
	if err := e.writer.Flush(); err != nil {
		_ = e.closeConnLocked()
		return nil, err
	}

	frame, err := readNativeResponse(e.reader)
	if err != nil {
		_ = e.closeConnLocked()
		return nil, err
	}
	if frame.requestID != requestID || frame.opcode != opcode || frame.laneID != laneID {
		_ = e.closeConnLocked()
		return nil, fmt.Errorf("ferricstore native response mismatch: got lane=%d opcode=%d request=%d", frame.laneID, frame.opcode, frame.requestID)
	}
	if frame.status != nativeStatusOK {
		return nil, NativeError{Status: frame.status, Value: frame.value}
	}
	e.lastActivityUnixNano.Store(time.Now().UnixNano())
	return frame.value, nil
}

func (e *NativeExecutor) writeRequest(ctx context.Context, opcode uint16, laneID uint32, requestID uint64, payload any, flags byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var body []byte
	var err error
	if flags&nativeFlagCustomPayload != 0 {
		raw, ok := payload.([]byte)
		if !ok {
			return errors.New("ferricstore native custom payload must be raw bytes")
		}
		body = raw
	} else {
		body, err = encodeNativeValue(payload)
		if err != nil {
			return err
		}
	}
	if len(body) > math.MaxUint32 {
		return errors.New("ferricstore native request body is too large")
	}

	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	e.mu.Lock()
	conn := e.conn
	writer := e.writer
	e.mu.Unlock()
	if conn == nil || writer == nil {
		return fmt.Errorf("%w: %w", errNativeConnectionUnavailable, net.ErrClosed)
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetWriteDeadline(deadline)
		defer func() { _ = conn.SetWriteDeadline(time.Time{}) }()
	} else if e.opts.Timeout > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(e.opts.Timeout))
		defer func() { _ = conn.SetWriteDeadline(time.Time{}) }()
	}

	header := make([]byte, nativeHeaderLen)
	copy(header[0:4], nativeMagic)
	header[4] = nativeRequestVersion
	header[5] = flags
	binary.BigEndian.PutUint32(header[6:10], laneID)
	binary.BigEndian.PutUint16(header[10:12], opcode)
	binary.BigEndian.PutUint64(header[12:20], requestID)
	binary.BigEndian.PutUint32(header[20:24], uint32(len(body)))

	if _, err := writer.Write(header); err != nil {
		return err
	}
	if len(body) > 0 {
		if _, err := writer.Write(body); err != nil {
			return err
		}
	}
	return writer.Flush()
}

func (e *NativeExecutor) readerLoop(conn net.Conn, reader *bufio.Reader) {
	for {
		frame, err := readNativeResponse(reader)
		if err != nil {
			e.closeConnAndFailPendingIfCurrent(conn, err)
			return
		}
		e.lastActivityUnixNano.Store(time.Now().UnixNano())
		if frame.requestID == 0 {
			e.deliverEvent(frame.value)
			continue
		}
		e.mu.Lock()
		responseCh := e.pending[frame.requestID]
		delete(e.pending, frame.requestID)
		e.mu.Unlock()
		if responseCh != nil {
			responseCh <- frame
		}
	}
}

func (e *NativeExecutor) closeConnAndFailPendingIfCurrent(conn net.Conn, err error) bool {
	e.mu.Lock()
	if e.conn != conn {
		e.mu.Unlock()
		return false
	}
	pending := e.pending
	e.pending = make(map[uint64]chan nativeResponse)
	_ = e.closeConnLocked()
	e.mu.Unlock()
	for _, responseCh := range pending {
		responseCh <- nativeResponse{err: err}
	}
	return true
}

func (e *NativeExecutor) removePending(requestID uint64) {
	e.mu.Lock()
	delete(e.pending, requestID)
	e.mu.Unlock()
}

func (e *NativeExecutor) deliverEvent(value any) {
	e.mu.Lock()
	events := e.events
	e.mu.Unlock()
	if events == nil {
		return
	}
	select {
	case events <- value:
	default:
		e.droppedEvents.Add(1)
	}
}

func (e *NativeExecutor) nextEvent(ctx context.Context) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	e.mu.Lock()
	events := e.events
	e.mu.Unlock()
	if events != nil {
		select {
		case event := <-events:
			return event, nil
		default:
		}
	}
	if err := e.ensureConnectedLocked(ctx); err != nil {
		return nil, err
	}
	e.mu.Lock()
	events = e.events
	e.mu.Unlock()
	if events == nil {
		return nil, net.ErrClosed
	}
	select {
	case event := <-events:
		return event, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (e *NativeExecutor) setDeadlineLocked(ctx context.Context) error {
	var deadline time.Time
	if ctxDeadline, ok := ctx.Deadline(); ok {
		deadline = ctxDeadline
	} else if e.opts.Timeout > 0 {
		deadline = time.Now().Add(e.opts.Timeout)
	}
	if deadline.IsZero() {
		if !e.deadlineSet {
			return nil
		}
		e.deadlineSet = false
		return e.conn.SetDeadline(time.Time{})
	}
	e.deadlineSet = true
	return e.conn.SetDeadline(deadline)
}

func readNativeResponse(reader *bufio.Reader) (nativeResponse, error) {
	first, err := readNativeFrame(reader)
	if err != nil {
		return nativeResponse{}, err
	}
	body := append([]byte(nil), first.body...)
	flags := first.flags

	for flags&nativeFlagMoreChunks != 0 {
		next, err := readNativeFrame(reader)
		if err != nil {
			return nativeResponse{}, err
		}
		if next.requestID != first.requestID || next.opcode != first.opcode || next.laneID != first.laneID {
			return nativeResponse{}, errors.New("ferricstore native chunk metadata mismatch")
		}
		body, err = appendNativeResponseChunk(body, next.body, nativeMaxFrameBytes)
		if err != nil {
			return nativeResponse{}, err
		}
		flags = next.flags
	}

	if flags&nativeFlagCompressed != 0 {
		return nativeResponse{}, errors.New("ferricstore native compressed responses are not negotiated")
	}
	if len(body) < 2 {
		return nativeResponse{}, errors.New("ferricstore native response body is truncated")
	}

	status := binary.BigEndian.Uint16(body[:2])
	if len(body) > 2 {
		value, ok, err := decodeNativeCompactValue(first.opcode, body[2:])
		if err != nil {
			return nativeResponse{}, err
		}
		if ok {
			return nativeResponse{
				laneID:    first.laneID,
				opcode:    first.opcode,
				requestID: first.requestID,
				status:    status,
				value:     value,
			}, nil
		}
	}
	value, rest, err := decodeNativeValue(body[2:])
	if err != nil {
		return nativeResponse{}, err
	}
	if len(rest) != 0 {
		return nativeResponse{}, errors.New("ferricstore native response value has trailing bytes")
	}
	return nativeResponse{
		laneID:    first.laneID,
		opcode:    first.opcode,
		requestID: first.requestID,
		status:    status,
		value:     value,
	}, nil
}

func (e *NativeExecutor) closeConnLocked() error {
	conn := e.conn
	e.conn = nil
	e.reader = nil
	e.writer = nil
	e.deadlineSet = false
	if e.heartbeatStop != nil {
		close(e.heartbeatStop)
		e.heartbeatStop = nil
	}
	if conn == nil {
		return nil
	}
	return conn.Close()
}

func (e *NativeExecutor) clientName() string {
	if e.opts.ClientName != "" {
		return e.opts.ClientName
	}
	return "ferricstore-go"
}

func defaultNativeOptions(addr string, tlsEnabled bool) NativeOptions {
	defaultPort := nativeDefaultPort
	if tlsEnabled {
		defaultPort = nativeDefaultTLSPort
	}
	timeout := 30 * time.Second
	return NativeOptions{
		Addr:                nativeAddressWithPort(addr, defaultPort),
		TLS:                 tlsEnabled,
		ClientName:          "ferricstore-go",
		Timeout:             timeout,
		Dialer:              &net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second},
		HeartbeatInterval:   30 * time.Second,
		HeartbeatTimeout:    30 * time.Second,
		ReconnectMaxRetries: 1,
	}
}

func nativeAddressWithPort(addr, defaultPort string) string {
	if addr == "" {
		addr = "127.0.0.1"
	}
	if _, _, err := net.SplitHostPort(addr); err == nil {
		return addr
	}
	if strings.Count(addr, ":") > 1 && !strings.HasPrefix(addr, "[") {
		return net.JoinHostPort(addr, defaultPort)
	}
	return net.JoinHostPort(addr, defaultPort)
}
