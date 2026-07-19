package ferricstore

import (
	"context"
	"strings"
	"testing"
)

func TestV080MSetAndMSetNXRejectCrossSlotBeforeEncoding(t *testing.T) {
	for _, command := range []string{"MSET", "MSETNX"} {
		t.Run(command, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			codec := &countingKVCodec{}
			client := NewClientWithExecutor(exec, WithConcurrentCodec(codec))
			values := map[string]any{"slot-a": "a", "slot-b": "b"}
			var err error
			if command == "MSET" {
				err = client.KV().MSet(context.Background(), values)
			} else {
				_, err = client.KV().MSetNX(context.Background(), values)
			}
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), "hash slot") {
				t.Fatalf("%s error = %v", command, err)
			}
			if codec.encodes.Load() != 0 || len(exec.calls) != 0 {
				t.Fatalf("%s performed work: encodes=%d calls=%#v", command, codec.encodes.Load(), exec.calls)
			}
		})
	}
}

func TestV080MSetAcceptsSharedHashTag(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	client := NewClientWithExecutor(exec)
	if err := client.KV().MSet(context.Background(), map[string]any{
		"user:{42}:name": "Ada",
		"user:{42}:role": "admin",
	}); err != nil {
		t.Fatal(err)
	}
	if len(exec.calls) != 1 {
		t.Fatalf("MSET calls = %#v", exec.calls)
	}
}

func TestV080TopologyCannotEnableCrossSlotMSet(t *testing.T) {
	exec := &TopologyNativeExecutor{crossShardWrites: CrossShardWritePerShard}
	keys := []string{"slot-a", "slot-b"}

	if _, err := exec.keyValueMSet(context.Background(), keys, []any{"a", "b"}); err == nil ||
		!strings.Contains(err.Error(), "requires keys in one hash slot") {
		t.Fatalf("typed topology MSET error = %v", err)
	}
	if _, err := exec.routeDataInSnapshot(
		[]any{"MSET", keys[0], "a", keys[1], "b"},
		topologyRoutingSnapshot{},
	); err == nil || !strings.Contains(err.Error(), "requires keys in one hash slot") {
		t.Fatalf("raw topology MSET error = %v", err)
	}
}

func TestV080RawAndPipelineMSetRejectCrossSlotLocally(t *testing.T) {
	for _, call := range []struct {
		name string
		run  func(*Client) error
	}{
		{name: "raw MSET", run: func(client *Client) error {
			_, err := client.Command(context.Background(), "MSET", "slot-a", "a", "slot-b", "b")
			return err
		}},
		{name: "raw MSETNX", run: func(client *Client) error {
			_, err := client.Command(context.Background(), "MSETNX", "slot-a", "a", "slot-b", "b")
			return err
		}},
		{name: "pipeline", run: func(client *Client) error {
			_, err := client.Pipeline(context.Background(), [][]any{{"MSET", "slot-a", "a", "slot-b", "b"}})
			return err
		}},
	} {
		t.Run(call.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			err := call.run(NewClientWithExecutor(exec))
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), "hash slot") {
				t.Fatalf("cross-slot error = %v", err)
			}
			if len(exec.calls) != 0 {
				t.Fatalf("cross-slot request reached executor: %#v", exec.calls)
			}
		})
	}
}
