package ferricstore

import (
	"context"
	"testing"
)

func TestV080NativeCommandLimitsRejectBeforeCodecOrTransport(t *testing.T) {
	tooLargeTTL := maxRelativeExpiryMillisV080 + 1
	tooLargeSeconds := maxRelativeExpirySecsV080 + 1
	tooLargeWindow := maxRelativeExpiryMillisV080/2 + 1
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{name: "CAS expiry", call: func(client *Client) error {
			_, err := client.CAS(context.Background(), "key", "old", "new", &tooLargeSeconds)
			return err
		}},
		{name: "LOCK ttl", call: func(client *Client) error {
			_, err := client.Lock(context.Background(), "key", "owner", tooLargeTTL)
			return err
		}},
		{name: "EXTEND ttl", call: func(client *Client) error {
			_, err := client.ExtendLock(context.Background(), "key", "owner", tooLargeTTL)
			return err
		}},
		{name: "RATELIMIT.ADD window", call: func(client *Client) error {
			_, err := client.RateLimitAdd(context.Background(), "key", tooLargeWindow, 1, 1)
			return err
		}},
		{name: "FETCH_OR_COMPUTE ttl", call: func(client *Client) error {
			_, err := client.FetchOrCompute(context.Background(), "key", tooLargeTTL, "")
			return err
		}},
		{name: "FETCH_OR_COMPUTE_RESULT ttl", call: func(client *Client) error {
			_, err := client.FetchOrComputeResult(context.Background(), "key", "token", "value", tooLargeTTL)
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			codec := &countingKVCodec{}
			if err := test.call(NewClientWithExecutor(exec, WithConcurrentCodec(codec))); err == nil {
				t.Fatal("request exceeding a FerricStore 0.8 native-command limit succeeded")
			}
			if codec.encodes.Load() != 0 {
				t.Fatalf("invalid request invoked codec %d times", codec.encodes.Load())
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid request reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestV080FetchOrComputeResultAllowsZeroTTL(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	ok, err := NewClientWithExecutor(exec).FetchOrComputeResult(
		context.Background(), "key", "token", "value", 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("zero-TTL fetch completion was not acknowledged")
	}
	assertCall(t, exec, []any{"FETCH_OR_COMPUTE_RESULT", "key", "token", "value", int64(0)})
}

func TestV080NativeCommandLimitBoundariesAreAccepted(t *testing.T) {
	if err := validateNativeTTLMSV080("LOCK", maxRelativeExpiryMillisV080, false); err != nil {
		t.Fatalf("maximum native TTL rejected: %v", err)
	}
	if err := validateNativeTTLSecondsV080("CAS", maxRelativeExpirySecsV080); err != nil {
		t.Fatalf("maximum native TTL seconds rejected: %v", err)
	}
	if err := validateNativeWindowMSV080(maxNativeWindowMSV080); err != nil {
		t.Fatalf("maximum native rate-limit window rejected: %v", err)
	}
}
