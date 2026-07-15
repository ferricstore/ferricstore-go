package ferricstore

import (
	"context"
	"testing"
)

func TestClientKeyWritesTreatEmptyInputAsNoOp(t *testing.T) {
	for _, call := range []struct {
		name string
		run  func(*Client) (int64, error)
	}{
		{name: "DEL", run: func(client *Client) (int64, error) {
			return client.Delete(context.Background())
		}},
		{name: "UNLINK", run: func(client *Client) (int64, error) {
			return client.Unlink(context.Background())
		}},
	} {
		t.Run(call.name, func(t *testing.T) {
			exec := &fakeExecutor{value: int64(99)}
			count, err := call.run(NewClientWithExecutor(exec))
			if err != nil || count != 0 {
				t.Fatalf("empty %s = %d, %v; want 0, nil", call.name, count, err)
			}
			if len(exec.calls) != 0 {
				t.Fatalf("empty %s reached executor: %#v", call.name, exec.calls)
			}
		})
	}
}

func TestClientKeyWritesRejectImpossibleCounts(t *testing.T) {
	for _, command := range []string{"DEL", "UNLINK"} {
		for _, response := range []any{int64(-1), int64(3), float64(1)} {
			t.Run(command, func(t *testing.T) {
				client := NewClientWithExecutor(&fakeExecutor{value: response})
				var err error
				if command == "DEL" {
					_, err = client.Delete(context.Background(), "one", "two")
				} else {
					_, err = client.Unlink(context.Background(), "one", "two")
				}
				if err == nil {
					t.Fatalf("%s accepted impossible count %#v", command, response)
				}
			})
		}
	}
}
