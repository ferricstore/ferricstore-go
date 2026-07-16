package ferricstore

import (
	"context"
	"testing"
)

func TestLockAndRateLimitRejectInvalidPositiveArgumentsBeforeTransport(t *testing.T) {
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{
			name: "lock zero ttl",
			call: func(client *Client) error {
				_, err := client.Lock(context.Background(), "key", "owner", 0)
				return err
			},
		},
		{
			name: "lock negative ttl",
			call: func(client *Client) error {
				_, err := client.Lock(context.Background(), "key", "owner", -1)
				return err
			},
		},
		{
			name: "extend zero ttl",
			call: func(client *Client) error {
				_, err := client.ExtendLock(context.Background(), "key", "owner", 0)
				return err
			},
		},
		{
			name: "extend negative ttl",
			call: func(client *Client) error {
				_, err := client.ExtendLock(context.Background(), "key", "owner", -1)
				return err
			},
		},
		{
			name: "rate limit zero window",
			call: func(client *Client) error {
				_, err := client.RateLimitAdd(context.Background(), "key", 0, 1, 1)
				return err
			},
		},
		{
			name: "rate limit negative maximum",
			call: func(client *Client) error {
				_, err := client.RateLimitAdd(context.Background(), "key", 1, -1, 1)
				return err
			},
		},
		{
			name: "rate limit zero count",
			call: func(client *Client) error {
				_, err := client.RateLimitAdd(context.Background(), "key", 1, 1, 0)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			if err := test.call(NewClientWithExecutor(exec)); err == nil {
				t.Fatal("invalid command arguments succeeded")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid command reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestRateLimitAddRejectsOutOfContractResponseValues(t *testing.T) {
	tests := []struct {
		name     string
		response []any
	}{
		{name: "unknown status", response: []any{"maybe", int64(1), int64(9), int64(500)}},
		{name: "negative count", response: []any{"allowed", int64(-1), int64(9), int64(500)}},
		{name: "negative remaining", response: []any{"allowed", int64(1), int64(-1), int64(500)}},
		{name: "remaining above maximum", response: []any{"allowed", int64(1), int64(11), int64(500)}},
		{name: "negative reset", response: []any{"allowed", int64(1), int64(9), int64(-1)}},
		{name: "reset above window", response: []any{"allowed", int64(1), int64(9), int64(1001)}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := NewClientWithExecutor(&fakeExecutor{value: test.response})
			if _, err := client.RateLimitAdd(context.Background(), "rate", 1000, 10, 1); err == nil {
				t.Fatalf("accepted out-of-contract response %#v", test.response)
			}
		})
	}
}

func TestKeyInfoRejectsOutOfContractValues(t *testing.T) {
	valid := map[string]any{
		"type":             "string",
		"value_size":       int64(5),
		"ttl_ms":           int64(-1),
		"hot_cache_status": "hot",
		"last_write_shard": int64(2),
	}
	tests := []struct {
		name  string
		field string
		value any
	}{
		{name: "empty type", field: "type", value: ""},
		{name: "negative value size", field: "value_size", value: int64(-1)},
		{name: "invalid ttl sentinel", field: "ttl_ms", value: int64(-3)},
		{name: "unknown cache status", field: "hot_cache_status", value: "warm"},
		{name: "negative shard", field: "last_write_shard", value: int64(-1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := make(map[string]any, len(valid))
			for key, value := range valid {
				response[key] = value
			}
			response[test.field] = test.value
			client := NewClientWithExecutor(&fakeExecutor{value: response})
			if _, err := client.KeyInfo(context.Background(), "key"); err == nil {
				t.Fatalf("accepted out-of-contract key_info %s=%v", test.field, test.value)
			}
		})
	}
}

func TestUnlockAndExtendRejectNonSuccessIntegerResponses(t *testing.T) {
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{
			name: "unlock zero",
			call: func(client *Client) error {
				_, err := client.Unlock(context.Background(), "key", "owner")
				return err
			},
		},
		{
			name: "extend two",
			call: func(client *Client) error {
				_, err := client.ExtendLock(context.Background(), "key", "owner", 1)
				return err
			},
		},
	}
	responses := []any{int64(0), int64(2)}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := NewClientWithExecutor(&fakeExecutor{value: responses[index]})
			if err := test.call(client); err == nil {
				t.Fatalf("accepted non-success response %v", responses[index])
			}
		})
	}
}
