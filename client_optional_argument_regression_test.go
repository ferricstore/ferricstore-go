package ferricstore

import (
	"context"
	"testing"
)

func TestSingleOptionalArgumentHelpersRejectExtraValues(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Client) error
	}{
		{name: "PING", run: func(client *Client) error {
			_, err := client.Ping(context.Background(), "first", "second")
			return err
		}},
		{name: "INFO", run: func(client *Client) error {
			_, err := client.ServerInfo(context.Background(), "default", "memory")
			return err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			if err := test.run(NewClientWithExecutor(exec)); err == nil {
				t.Fatal("helper silently ignored an extra optional argument")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid helper call reached executor: %#v", exec.calls)
			}
		})
	}
}
