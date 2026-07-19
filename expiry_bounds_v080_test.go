package ferricstore

import (
	"context"
	"math"
	"testing"
)

func TestV080TypedExpiryOptionsRejectServerIntegerOverflowBeforeTransport(t *testing.T) {
	t.Parallel()

	overflowSeconds := int64(math.MaxInt64/1000 + 1)
	overflowMillis := int64(math.MaxInt64)
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{
			name: "SET EXAT",
			call: func(client *Client) error {
				_, err := client.KV().SetWithOptions(context.Background(), "key", "value", SetOptions{EXATSeconds: &overflowSeconds})
				return err
			},
		},
		{
			name: "SET PX",
			call: func(client *Client) error {
				_, err := client.KV().SetWithOptions(context.Background(), "key", "value", SetOptions{PXMilliseconds: &overflowMillis})
				return err
			},
		},
		{
			name: "GETEX EXAT",
			call: func(client *Client) error {
				_, err := client.KV().GetEX(context.Background(), "key", GetEXOptions{EXATSeconds: &overflowSeconds})
				return err
			},
		},
		{
			name: "HGETEX EX",
			call: func(client *Client) error {
				_, err := client.Hash().GetEX(context.Background(), "key", []string{"field"}, HashGetEXOptions{EXSeconds: &overflowSeconds})
				return err
			},
		},
		{
			name: "HSETEX",
			call: func(client *Client) error {
				_, err := client.Hash().SetEX(context.Background(), "key", map[string]any{"field": "value"}, HashSetEXOptions{EXSeconds: &overflowSeconds})
				return err
			},
		},
		{
			name: "SETEX",
			call: func(client *Client) error {
				return client.KV().SetEX(context.Background(), "key", overflowSeconds, "value")
			},
		},
		{
			name: "PSETEX",
			call: func(client *Client) error {
				return client.KV().PSetEX(context.Background(), "key", overflowMillis, "value")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: "OK"}
			if err := test.call(NewClientWithExecutor(exec)); err == nil {
				t.Fatal("expiry outside FerricStore 0.8 integer range was accepted")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid expiry reached transport: %#v", exec.calls)
			}
		})
	}
}

func TestV080RawSETRejectsAbsoluteSecondsOverflowBeforeTransport(t *testing.T) {
	t.Parallel()

	exec := &fakeExecutor{value: "OK"}
	client := NewClientWithExecutor(exec)
	if _, err := client.Command(
		context.Background(), "SET", "key", "value", "EXAT", int64(math.MaxInt64/1000+1),
	); err == nil {
		t.Fatal("raw SET accepted overflowing EXAT value")
	}
	if len(exec.calls) != 0 {
		t.Fatalf("invalid raw SET reached transport: %#v", exec.calls)
	}
}

func TestV080AbsoluteSecondsMaximumRemainsSupported(t *testing.T) {
	t.Parallel()

	maximum := int64(math.MaxInt64 / 1000)
	if err := validateSetOptions(SetOptions{EXATSeconds: &maximum}); err != nil {
		t.Fatalf("SET EXAT maximum rejected: %v", err)
	}
	if _, err := parseSETCommandOptions([]any{"EXAT", maximum}); err != nil {
		t.Fatalf("raw SET EXAT maximum rejected: %v", err)
	}
}
