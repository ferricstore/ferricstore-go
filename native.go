package ferricstore

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	nativeMagic           = "FSNP"
	nativeRequestVersion  = 0x01
	nativeResponseVersion = 0x81
	nativeHeaderLen       = 24
	nativeDefaultPort     = "6388"
	nativeDefaultTLSPort  = "6389"
	nativeMaxFrameBytes   = 128 * 1024 * 1024

	nativeFlagCompressed = 0x08
	nativeFlagMoreChunks = 0x20

	nativeStatusOK = 0

	nativeOpAuth        = 0x0002
	nativeOpStartup     = 0x000C
	nativeOpCommandExec = 0x0100
)

type NativeOptions struct {
	Addr       string
	Username   string
	Password   string
	TLS        bool
	TLSConfig  *tls.Config
	ClientName string
	Timeout    time.Duration
	Dialer     *net.Dialer
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

type NativeExecutor struct {
	opts NativeOptions

	mu       sync.Mutex
	conn     net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
	nextID   uint64
	isClosed bool
}

type NativeError struct {
	Status uint16
	Value  any
}

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

func (e *NativeExecutor) command(ctx context.Context, args ...any) (any, error) {
	if len(args) == 0 {
		return nil, errors.New("ferricstore command requires at least a command name")
	}
	command := strings.ToUpper(asString(args[0]))
	if command == "" {
		return nil, errors.New("ferricstore command name is empty")
	}
	payload := map[string]any{
		"command": command,
		"args":    append([]any(nil), args[1:]...),
	}
	value, err := e.request(ctx, nativeOpCommandExec, 1, payload, 0)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", command, err)
	}
	return value, nil
}

func (e *NativeExecutor) request(ctx context.Context, opcode uint16, laneID uint32, payload any, flags byte) (any, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.ensureConnectedLocked(ctx); err != nil {
		return nil, err
	}
	return e.requestLocked(ctx, opcode, laneID, payload, flags)
}

func (e *NativeExecutor) ensureConnectedLocked(ctx context.Context) error {
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

	e.conn = conn
	e.reader = bufio.NewReader(conn)
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
	return nil
}

func (e *NativeExecutor) requestLocked(ctx context.Context, opcode uint16, laneID uint32, payload any, flags byte) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	body, err := encodeNativeValue(payload)
	if err != nil {
		return nil, err
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

	frame, err := e.readResponseLocked()
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
	return frame.value, nil
}

func (e *NativeExecutor) setDeadlineLocked(ctx context.Context) error {
	var deadline time.Time
	if ctxDeadline, ok := ctx.Deadline(); ok {
		deadline = ctxDeadline
	} else if e.opts.Timeout > 0 {
		deadline = time.Now().Add(e.opts.Timeout)
	}
	return e.conn.SetDeadline(deadline)
}

func (e *NativeExecutor) readResponseLocked() (nativeResponse, error) {
	first, err := readNativeFrame(e.reader)
	if err != nil {
		return nativeResponse{}, err
	}
	body := append([]byte(nil), first.body...)
	flags := first.flags

	for flags&nativeFlagMoreChunks != 0 {
		next, err := readNativeFrame(e.reader)
		if err != nil {
			return nativeResponse{}, err
		}
		if next.requestID != first.requestID || next.opcode != first.opcode || next.laneID != first.laneID {
			return nativeResponse{}, errors.New("ferricstore native chunk metadata mismatch")
		}
		body = append(body, next.body...)
		flags = next.flags
	}

	if flags&nativeFlagCompressed != 0 {
		return nativeResponse{}, errors.New("ferricstore native compressed responses are not negotiated")
	}
	if len(body) < 2 {
		return nativeResponse{}, errors.New("ferricstore native response body is truncated")
	}

	status := binary.BigEndian.Uint16(body[:2])
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

type nativeFrame struct {
	flags     byte
	laneID    uint32
	opcode    uint16
	requestID uint64
	body      []byte
}

type nativeResponse struct {
	laneID    uint32
	opcode    uint16
	requestID uint64
	status    uint16
	value     any
}

func readNativeFrame(reader io.Reader) (nativeFrame, error) {
	header := make([]byte, nativeHeaderLen)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nativeFrame{}, err
	}
	if string(header[0:4]) != nativeMagic {
		return nativeFrame{}, errors.New("ferricstore native response has invalid magic")
	}
	if header[4] != nativeResponseVersion {
		return nativeFrame{}, fmt.Errorf("ferricstore native response has unsupported version 0x%x", header[4])
	}
	bodyLen := binary.BigEndian.Uint32(header[20:24])
	if bodyLen > nativeMaxFrameBytes {
		return nativeFrame{}, errors.New("ferricstore native response frame is too large")
	}
	body := make([]byte, int(bodyLen))
	if _, err := io.ReadFull(reader, body); err != nil {
		return nativeFrame{}, err
	}
	return nativeFrame{
		flags:     header[5],
		laneID:    binary.BigEndian.Uint32(header[6:10]),
		opcode:    binary.BigEndian.Uint16(header[10:12]),
		requestID: binary.BigEndian.Uint64(header[12:20]),
		body:      body,
	}, nil
}

func defaultNativeOptions(addr string, tlsEnabled bool) NativeOptions {
	defaultPort := nativeDefaultPort
	if tlsEnabled {
		defaultPort = nativeDefaultTLSPort
	}
	timeout := 30 * time.Second
	return NativeOptions{
		Addr:       nativeAddressWithPort(addr, defaultPort),
		TLS:        tlsEnabled,
		ClientName: "ferricstore-go",
		Timeout:    timeout,
		Dialer:     &net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second},
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

func encodeNativeValue(value any) ([]byte, error) {
	var buf bytes.Buffer
	if err := writeNativeValue(&buf, value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeNativeValue(buf *bytes.Buffer, value any) error {
	switch v := value.(type) {
	case nil:
		buf.WriteByte(0)
	case bool:
		if v {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(2)
		}
	case int:
		writeNativeInt(buf, int64(v))
	case int8:
		writeNativeInt(buf, int64(v))
	case int16:
		writeNativeInt(buf, int64(v))
	case int32:
		writeNativeInt(buf, int64(v))
	case int64:
		writeNativeInt(buf, v)
	case uint:
		return writeNativeUint(buf, uint64(v))
	case uint8:
		return writeNativeUint(buf, uint64(v))
	case uint16:
		return writeNativeUint(buf, uint64(v))
	case uint32:
		return writeNativeUint(buf, uint64(v))
	case uint64:
		return writeNativeUint(buf, v)
	case float32:
		writeNativeFloat(buf, float64(v))
	case float64:
		writeNativeFloat(buf, v)
	case string:
		writeNativeBytes(buf, []byte(v))
	case []byte:
		writeNativeBytes(buf, v)
	case []any:
		return writeNativeArray(buf, v)
	case map[string]any:
		return writeNativeMap(buf, v)
	default:
		return writeNativeReflect(buf, value)
	}
	return nil
}

func writeNativeReflect(buf *bytes.Buffer, value any) error {
	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		buf.WriteByte(0)
		return nil
	}
	for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			buf.WriteByte(0)
			return nil
		}
		rv = rv.Elem()
	}
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		if rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
			writeNativeBytes(buf, rv.Bytes())
			return nil
		}
		items := make([]any, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			items = append(items, rv.Index(i).Interface())
		}
		return writeNativeArray(buf, items)
	case reflect.Map:
		mapping := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			mapping[fmt.Sprint(iter.Key().Interface())] = iter.Value().Interface()
		}
		return writeNativeMap(buf, mapping)
	default:
		writeNativeBytes(buf, []byte(fmt.Sprint(value)))
		return nil
	}
}

func writeNativeInt(buf *bytes.Buffer, value int64) {
	buf.WriteByte(3)
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], uint64(value))
	buf.Write(raw[:])
}

func writeNativeUint(buf *bytes.Buffer, value uint64) error {
	if value > math.MaxInt64 {
		return fmt.Errorf("ferricstore native integer overflows int64: %d", value)
	}
	writeNativeInt(buf, int64(value))
	return nil
}

func writeNativeFloat(buf *bytes.Buffer, value float64) {
	buf.WriteByte(7)
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], math.Float64bits(value))
	buf.Write(raw[:])
}

func writeNativeBytes(buf *bytes.Buffer, value []byte) {
	buf.WriteByte(4)
	var raw [4]byte
	binary.BigEndian.PutUint32(raw[:], uint32(len(value)))
	buf.Write(raw[:])
	buf.Write(value)
}

func writeNativeArray(buf *bytes.Buffer, values []any) error {
	buf.WriteByte(5)
	var raw [4]byte
	binary.BigEndian.PutUint32(raw[:], uint32(len(values)))
	buf.Write(raw[:])
	for _, value := range values {
		if err := writeNativeValue(buf, value); err != nil {
			return err
		}
	}
	return nil
}

func writeNativeMap(buf *bytes.Buffer, values map[string]any) error {
	buf.WriteByte(6)
	var raw [4]byte
	binary.BigEndian.PutUint32(raw[:], uint32(len(values)))
	buf.Write(raw[:])
	for key, value := range values {
		binary.BigEndian.PutUint32(raw[:], uint32(len(key)))
		buf.Write(raw[:])
		buf.WriteString(key)
		if err := writeNativeValue(buf, value); err != nil {
			return err
		}
	}
	return nil
}

func decodeNativeValue(data []byte) (any, []byte, error) {
	if len(data) == 0 {
		return nil, nil, errors.New("ferricstore native value is empty")
	}
	tag := data[0]
	rest := data[1:]
	switch tag {
	case 0:
		return nil, rest, nil
	case 1:
		return true, rest, nil
	case 2:
		return false, rest, nil
	case 3:
		if len(rest) < 8 {
			return nil, nil, errors.New("ferricstore native integer is truncated")
		}
		return int64(binary.BigEndian.Uint64(rest[:8])), rest[8:], nil
	case 4:
		if len(rest) < 4 {
			return nil, nil, errors.New("ferricstore native binary length is truncated")
		}
		size := int(binary.BigEndian.Uint32(rest[:4]))
		rest = rest[4:]
		if len(rest) < size {
			return nil, nil, errors.New("ferricstore native binary is truncated")
		}
		return append([]byte(nil), rest[:size]...), rest[size:], nil
	case 5:
		if len(rest) < 4 {
			return nil, nil, errors.New("ferricstore native array length is truncated")
		}
		count := int(binary.BigEndian.Uint32(rest[:4]))
		rest = rest[4:]
		items := make([]any, 0, count)
		for i := 0; i < count; i++ {
			value, next, err := decodeNativeValue(rest)
			if err != nil {
				return nil, nil, err
			}
			items = append(items, value)
			rest = next
		}
		return items, rest, nil
	case 6:
		if len(rest) < 4 {
			return nil, nil, errors.New("ferricstore native map length is truncated")
		}
		count := int(binary.BigEndian.Uint32(rest[:4]))
		rest = rest[4:]
		mapping := make(map[string]any, count)
		for i := 0; i < count; i++ {
			if len(rest) < 4 {
				return nil, nil, errors.New("ferricstore native map key length is truncated")
			}
			keySize := int(binary.BigEndian.Uint32(rest[:4]))
			rest = rest[4:]
			if len(rest) < keySize {
				return nil, nil, errors.New("ferricstore native map key is truncated")
			}
			key := string(rest[:keySize])
			rest = rest[keySize:]
			value, next, err := decodeNativeValue(rest)
			if err != nil {
				return nil, nil, err
			}
			mapping[key] = value
			rest = next
		}
		return mapping, rest, nil
	case 7:
		if len(rest) < 8 {
			return nil, nil, errors.New("ferricstore native float is truncated")
		}
		return math.Float64frombits(binary.BigEndian.Uint64(rest[:8])), rest[8:], nil
	default:
		return nil, nil, fmt.Errorf("ferricstore native value has unknown tag %d", tag)
	}
}

func nativeErrorMessage(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	case map[string]any:
		if message := asString(v["message"]); message != "" {
			return message
		}
		if code := asString(v["code"]); code != "" {
			return code
		}
	}
	return fmt.Sprint(value)
}
