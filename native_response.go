package ferricstore

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
)

type nativeResponsePolicy struct {
	maxBytes int
	codecs   nativeResponseCodecs
}

func readNativeResponseWithPolicy(reader *bufio.Reader, policy nativeResponsePolicy) (nativeResponse, error) {
	assembler := newNativeResponseAssembler(policy.maxBytes, nativeMaxResponseChunkFrames, policy.codecs)
	for {
		frame, err := readNativeFrameWithLimit(reader, policy.maxBytes)
		if err != nil {
			return nativeResponse{}, err
		}
		response, err := assembler.add(frame)
		if err != nil {
			return nativeResponse{}, err
		}
		if response != nil {
			return *response, nil
		}
	}
}

func decodeNativeResponseFrame(first nativeFrame, body []byte, flags byte) (nativeResponse, error) {
	return decodeNativeResponseFrameWithCodecs(first, body, flags, nativeResponseCodecs{})
}

func decodeNativeResponseFrameWithCodecs(first nativeFrame, body []byte, flags byte, codecs nativeResponseCodecs) (nativeResponse, error) {
	if err := validateNativeResponseFlags(flags, false); err != nil {
		return nativeResponse{}, err
	}
	if flags&nativeFlagCompressed != 0 {
		return nativeResponse{}, errors.New("ferricstore native compressed responses are not negotiated")
	}
	if len(body) < 2 {
		return nativeResponse{}, errors.New("ferricstore native response body is truncated")
	}

	status := binary.BigEndian.Uint16(body[:2])
	if flags&nativeFlagCustomPayload != 0 {
		if len(body) == 2 {
			return nativeResponse{}, errors.New("ferricstore native custom response payload is empty")
		}
		value, ok, err := decodeNativeCompactValueWithCodecs(first.opcode, body[2:], codecs)
		if err != nil {
			return nativeResponse{}, err
		}
		if !ok {
			return nativeResponse{}, fmt.Errorf(
				"ferricstore native custom response payload marker 0x%x is unsupported",
				body[2],
			)
		}
		return nativeResponse{
			flags:     flags,
			laneID:    first.laneID,
			opcode:    first.opcode,
			requestID: first.requestID,
			status:    status,
			value:     value,
			wireBytes: len(body),
		}, nil
	}
	value, rest, err := decodeNativeOwnedValue(body[2:])
	if err != nil {
		return nativeResponse{}, err
	}
	if len(rest) != 0 {
		return nativeResponse{}, errors.New("ferricstore native response value has trailing bytes")
	}
	return nativeResponse{
		flags:     flags,
		laneID:    first.laneID,
		opcode:    first.opcode,
		requestID: first.requestID,
		status:    status,
		value:     value,
		wireBytes: len(body),
	}, nil
}

func validateNativeResponseFlags(flags byte, chunksAllowed bool) error {
	if flags&^nativeResponseWireFlags != 0 || flags&nativeFlagNoReply != 0 {
		return fmt.Errorf("ferricstore native response uses unsupported flags %#x", flags)
	}
	if !chunksAllowed && flags&nativeFlagMoreChunks != 0 {
		return errors.New("ferricstore native response is still marked as chunked")
	}
	return nil
}

func validateNativeServerInitiatedResponse(frame nativeResponse) error {
	if frame.requestID != 0 {
		return nil
	}
	if err := nativeConnectionLevelError(frame); err != nil {
		return err
	}
	if frame.laneID != 0 {
		return fmt.Errorf("ferricstore native server-initiated frame uses data lane %d", frame.laneID)
	}
	if frame.status != nativeStatusOK {
		return fmt.Errorf("ferricstore native server-initiated frame has status %d", frame.status)
	}
	switch frame.opcode {
	case nativeOpEvent, nativeOpGoAway:
		return nil
	default:
		return fmt.Errorf("ferricstore native opcode %d cannot use reserved request_id 0", frame.opcode)
	}
}

func nativeConnectionLevelError(frame nativeResponse) error {
	if frame.requestID == 0 && frame.opcode == 0 && frame.laneID == 0 && frame.status != nativeStatusOK {
		return NativeError{Status: frame.status, Value: frame.value}
	}
	return nil
}

func (e *NativeExecutor) closeConnLocked() error {
	conn := e.conn
	flow := e.flow
	e.conn = nil
	e.reader = nil
	e.writer = nil
	e.flow = nil
	if e.connectionDone != nil {
		close(e.connectionDone)
		e.connectionDone = nil
	}
	if e.goAwayDone != nil {
		close(e.goAwayDone)
		e.goAwayDone = nil
	}
	e.goAway = false
	if e.heartbeatStop != nil {
		e.stopHeartbeatLocked()
	}
	flow.close(errNativeConnectionUnavailable)
	if conn == nil {
		return nil
	}
	return conn.Close()
}

func nativeClientName(name string) string {
	if name != "" {
		return name
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
		GoAwayDrainTimeout:  nativeDefaultGoAwayDrainTimeout,
		ReconnectMaxRetries: 1,
		ProtocolLanes:       nativeDefaultProtocolLanes,
		MaxQueuedRequests:   nativeDefaultQueuedRequests,
		MaxResponseBytes:    nativeDefaultResponseBytes,
		addressInput:        addr,
		addressUsesDefault:  !nativeAddressHasExplicitPort(addr),
	}
}

func applyNativeOptions(options *NativeOptions, opts ...NativeOption) {
	addressInput := options.addressInput
	addressUsesDefault := options.addressUsesDefault
	credentialsSet := options.credentialsSet
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		previousAddr := options.Addr
		previousUsername, previousPassword := options.Username, options.Password
		opt(options)
		if options.Addr != previousAddr {
			// An empty override retains the constructor's fallback behavior;
			// any non-empty override is an exact endpoint owned by the option.
			addressUsesDefault = options.Addr == ""
		}
		if options.credentialsSet || options.Username != previousUsername || options.Password != previousPassword {
			credentialsSet = true
		}
		// Preserve constructor provenance even when a user-defined option
		// replaces the exported NativeOptions value wholesale.
		options.addressInput = addressInput
		options.addressUsesDefault = addressUsesDefault
		options.credentialsSet = credentialsSet
	}
	if options.addressUsesDefault || options.Addr == "" {
		defaultPort := nativeDefaultPort
		if options.TLS {
			defaultPort = nativeDefaultTLSPort
		}
		options.Addr = nativeAddressWithPort(options.addressInput, defaultPort)
	}
}

func nativeAddressHasExplicitPort(addr string) bool {
	_, port, err := net.SplitHostPort(addr)
	return err == nil && port != ""
}

func nativeAddressWithPort(addr, defaultPort string) string {
	if addr == "" {
		addr = "127.0.0.1"
	}
	if host, port, err := net.SplitHostPort(addr); err == nil {
		if port != "" {
			return addr
		}
		addr = host
	}
	if strings.HasPrefix(addr, "[") && strings.HasSuffix(addr, "]") {
		addr = strings.TrimSuffix(strings.TrimPrefix(addr, "["), "]")
	}
	return net.JoinHostPort(addr, defaultPort)
}
