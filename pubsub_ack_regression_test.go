package ferricstore

import (
	"context"
	"testing"
	"time"
)

func TestPubSubMessageParserUsesFinalCommandAcknowledgement(t *testing.T) {
	tests := []struct {
		name        string
		value       []any
		wantKind    string
		wantTarget  string
		wantCount   int64
		wantPattern bool
	}{
		{
			name: "subscribe many",
			value: []any{
				[]any{"subscribe", "jobs", int64(1)},
				[]any{"subscribe", "alerts", int64(2)},
			},
			wantKind: "subscribe", wantTarget: "alerts", wantCount: 2,
		},
		{
			name: "unsubscribe all",
			value: []any{
				[]any{"unsubscribe", "jobs", int64(1)},
				[]any{"unsubscribe", "alerts", int64(0)},
			},
			wantKind: "unsubscribe", wantTarget: "alerts", wantCount: 0,
		},
		{
			name: "punsubscribe all",
			value: []any{
				[]any{"punsubscribe", "jobs:*", int64(1)},
				[]any{"punsubscribe", "alerts:*", int64(0)},
			},
			wantKind: "punsubscribe", wantTarget: "alerts:*", wantCount: 0, wantPattern: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			message := pubSubMessageFromNative(tc.value)
			target := message.Channel
			if tc.wantPattern {
				target = message.Pattern
			}
			if message.Kind != tc.wantKind || target != tc.wantTarget || message.Count != tc.wantCount {
				t.Fatalf("decoded acknowledgement = %#v; want kind=%q target=%q count=%d", message, tc.wantKind, tc.wantTarget, tc.wantCount)
			}
		})
	}
}

func TestPubSubCommandRejectsMalformedAcknowledgementBeforeTracking(t *testing.T) {
	listener, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any {
		return []any{[]any{"subscribe", "jobs", "many"}}
	})
	pubsub := NewPubSub(
		listener.Addr().String(),
		WithNativeTimeout(time.Second),
		WithNativeHeartbeat(0, 0),
		WithNativeReconnect(0),
	)
	defer func() { _ = pubsub.Close() }()

	if message, err := pubsub.Subscribe(context.Background(), "jobs"); err == nil {
		t.Fatalf("malformed acknowledgement succeeded as %#v", message)
	}
	pubsub.mu.Lock()
	tracked := len(pubsub.channels)
	pubsub.mu.Unlock()
	if tracked != 0 {
		t.Fatalf("malformed acknowledgement tracked %d channels", tracked)
	}
}

func TestPubSubCommandRejectsPartialAcknowledgementBeforeTracking(t *testing.T) {
	listener, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any {
		return []any{[]any{"subscribe", "jobs", int64(1)}}
	})
	pubsub := NewPubSub(
		listener.Addr().String(),
		WithNativeTimeout(time.Second),
		WithNativeHeartbeat(0, 0),
		WithNativeReconnect(0),
	)
	defer func() { _ = pubsub.Close() }()

	if message, err := pubsub.Subscribe(context.Background(), "jobs", "alerts"); err == nil {
		t.Fatalf("partial acknowledgement succeeded as %#v", message)
	}
	pubsub.mu.Lock()
	tracked := len(pubsub.channels)
	pubsub.mu.Unlock()
	if tracked != 0 {
		t.Fatalf("partial acknowledgement tracked %d channels", tracked)
	}
}
