package ferricstore

import (
	"crypto/tls"
	"net"
	"time"
)

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
	GoAwayDrainTimeout  time.Duration
	ReconnectMaxRetries int
	ProtocolLanes       uint32
	MaxQueuedRequests   int
	eventSubscription   *nativeEventSubscription
	addressInput        string
	addressUsesDefault  bool
	credentialsSet      bool
}

type nativeEventSubscription struct {
	events  []string
	handler func(nativeServerEvent)
}

type NativeOption func(*NativeOptions)

func WithNativeCredentials(username, password string) NativeOption {
	return func(opts *NativeOptions) {
		opts.Username = username
		opts.Password = password
		opts.credentialsSet = true
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
		if timeout < 0 {
			timeout = 0
		}
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

// WithNativeGoAwayDrainTimeout bounds how long an old connection may retain
// live requests after it stops admitting new work. This is especially
// important when another request on the connection blocks indefinitely.
func WithNativeGoAwayDrainTimeout(timeout time.Duration) NativeOption {
	return func(opts *NativeOptions) {
		if timeout <= 0 {
			timeout = nativeDefaultGoAwayDrainTimeout
		}
		opts.GoAwayDrainTimeout = timeout
	}
}

// WithNativeLanes caps automatic data-lane use. The server-advertised STARTUP
// limit may reduce this value further.
func WithNativeLanes(lanes uint32) NativeOption {
	return func(opts *NativeOptions) {
		if lanes == 0 {
			lanes = 1
		}
		opts.ProtocolLanes = lanes
	}
}

// WithNativeMaxQueuedRequests bounds requests waiting for server-advertised
// native flow-control credits. A zero limit rejects instead of queueing.
func WithNativeMaxQueuedRequests(limit int) NativeOption {
	return func(opts *NativeOptions) {
		if limit < 0 {
			limit = 0
		}
		opts.MaxQueuedRequests = limit
	}
}

func withNativeEventSubscription(events []string, handler func(nativeServerEvent)) NativeOption {
	return func(opts *NativeOptions) {
		opts.eventSubscription = &nativeEventSubscription{
			events:  append([]string(nil), events...),
			handler: handler,
		}
	}
}

func nativeStartupEvents(options NativeOptions) []string {
	if options.eventSubscription == nil {
		return nil
	}
	return append([]string(nil), options.eventSubscription.events...)
}

func nativeConfiguredEventHandler(subscription *nativeEventSubscription) func(nativeServerEvent) {
	if subscription == nil {
		return nil
	}
	return subscription.handler
}
