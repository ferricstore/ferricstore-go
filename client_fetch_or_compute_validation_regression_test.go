package ferricstore

import (
	"context"
	"testing"
)

func TestFetchOrComputeRejectsInvalidTTLsBeforeCodecOrTransport(t *testing.T) {
	tests := []struct {
		name string
		call func(*Client) error
	}{
		{
			name: "zero fetch TTL",
			call: func(client *Client) error {
				_, err := client.FetchOrCompute(context.Background(), "key", 0, "")
				return err
			},
		},
		{
			name: "negative result TTL",
			call: func(client *Client) error {
				_, err := client.FetchOrComputeResult(context.Background(), "key", "value", -1)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			codec := &countingKVCodec{}
			client := NewClientWithExecutor(exec, WithConcurrentCodec(codec))
			if err := test.call(client); err == nil {
				t.Fatal("invalid fetch-or-compute TTL succeeded")
			}
			if codec.encodes.Load() != 0 {
				t.Fatalf("invalid TTL invoked codec %d times", codec.encodes.Load())
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid TTL reached transport: %#v", exec.calls)
			}
		})
	}
}
