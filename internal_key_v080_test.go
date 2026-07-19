package ferricstore

import (
	"context"
	"strings"
	"testing"
)

func TestV080PublicCommandsRejectReservedInternalKeysLocally(t *testing.T) {
	reserved := []string{
		"f:{f}:route",
		"f:{fa:7}:v:result",
		"f:{flow-governance}:gov:catalog",
		"f:{__server__}:catalog:acl:entries:x",
		"H:user\x00field",
	}
	for _, key := range reserved {
		t.Run(key, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("value")}
			client := NewClientWithExecutor(exec)
			if _, err := client.Command(context.Background(), "GET", key); err == nil ||
				!strings.Contains(strings.ToLower(err.Error()), "internal key") {
				t.Fatalf("GET reserved key error = %v", err)
			}
			if len(exec.calls) != 0 {
				t.Fatalf("reserved key reached executor: %#v", exec.calls)
			}
		})
	}
}

func TestV080PublicCommandsRejectNamedReservedInternalKeysLocally(t *testing.T) {
	reserved := []any{
		namedCommandString("f:{f}:route"),
		namedCommandBytes("H:user\x00field"),
	}
	for _, key := range reserved {
		exec := &fakeExecutor{value: []byte("value")}
		client := NewClientWithExecutor(exec)
		if _, err := client.Command(context.Background(), "GET", key); err == nil ||
			!strings.Contains(strings.ToLower(err.Error()), "internal key") {
			t.Fatalf("GET named reserved key %T error = %v", key, err)
		}
		if len(exec.calls) != 0 {
			t.Fatalf("named reserved key reached executor: %#v", exec.calls)
		}
	}
}

func TestV080KeyIntrospectionRejectsReservedInternalKeysLocally(t *testing.T) {
	tests := [][]any{
		{"CLUSTER.KEYSLOT", "f:{f}:route"},
		{"ROUTE", "f:{fa:7}:v:result"},
		{"ROUTE_BATCH", "safe", "H:user\x00field"},
	}
	for _, command := range tests {
		exec := &fakeExecutor{value: int64(1)}
		client := NewClientWithExecutor(exec)
		if _, err := client.Command(context.Background(), command...); err == nil ||
			!strings.Contains(strings.ToLower(err.Error()), "internal key") {
			t.Fatalf("%s reserved-key error = %v", command[0], err)
		}
		if len(exec.calls) != 0 {
			t.Fatalf("%s reserved key reached executor: %#v", command[0], exec.calls)
		}
	}
}

func TestV080CommandGetKeysRejectsNestedReservedInternalKeysLocally(t *testing.T) {
	exec := &fakeExecutor{value: []any{"f:{f}:route"}}
	client := NewClientWithExecutor(exec)

	if _, err := client.CommandGetKeys(context.Background(), "GET", "f:{f}:route"); err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "internal key") {
		t.Fatalf("COMMAND GETKEYS reserved-key error = %v", err)
	}
	if len(exec.calls) != 0 {
		t.Fatalf("nested reserved key reached executor: %#v", exec.calls)
	}
}

func TestV080TypedKVRejectsReservedKeyBeforeEncoding(t *testing.T) {
	tests := []struct {
		name string
		call func(*KeyValueStore) error
	}{
		{name: "SET", call: func(store *KeyValueStore) error {
			return store.Set(context.Background(), "f:{f}:route", "value")
		}},
		{name: "SET options", call: func(store *KeyValueStore) error {
			_, err := store.SetWithOptions(context.Background(), "f:{f}:route", "value", SetOptions{})
			return err
		}},
		{name: "SETNX", call: func(store *KeyValueStore) error {
			_, err := store.SetNX(context.Background(), "f:{f}:route", "value")
			return err
		}},
		{name: "SETEX", call: func(store *KeyValueStore) error {
			return store.SetEX(context.Background(), "f:{f}:route", 1, "value")
		}},
		{name: "PSETEX", call: func(store *KeyValueStore) error {
			return store.PSetEX(context.Background(), "f:{f}:route", 1, "value")
		}},
		{name: "GETSET", call: func(store *KeyValueStore) error {
			_, err := store.GetSet(context.Background(), "f:{f}:route", "value")
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			codec := &countingKVCodec{}
			store := NewClientWithExecutor(exec, WithConcurrentCodec(codec)).KV()
			err := test.call(store)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), "internal key") {
				t.Fatalf("reserved key error = %v", err)
			}
			if codec.encodes.Load() != 0 || len(exec.calls) != 0 {
				t.Fatalf("reserved key performed work: encodes=%d calls=%#v", codec.encodes.Load(), exec.calls)
			}
		})
	}
}

func TestV080InternalKeyValidationDoesNotOverRejectLookalikesOrFlowRefs(t *testing.T) {
	exec := &fakeExecutor{value: []byte("value")}
	client := NewClientWithExecutor(exec)
	if _, err := client.Command(context.Background(), "GET", "f:{customer}:profile"); err != nil {
		t.Fatalf("valid public lookalike rejected: %v", err)
	}
	if _, err := client.Command(context.Background(), "FLOW.VALUE.MGET", "f:{fa:7}:v:result", "MAX_BYTES", 10); err != nil {
		t.Fatalf("opaque Flow value ref rejected: %v", err)
	}
	if len(exec.calls) != 2 {
		t.Fatalf("valid calls = %#v", exec.calls)
	}
}

func TestV080FlowRoutingUsesSlotsWithoutConstructingInternalKeys(t *testing.T) {
	for _, command := range [][]any{
		{"FLOW.GET", "flow-1"},
		{"FLOW.COMPLETE", "flow-1", "lease", "PARTITION", "tenant-1"},
		{"FLOW.EFFECT.GET", "flow-1", "EFFECT_KEY", "effect-1"},
	} {
		target, ok := routingKeyForCommand(command)
		if !ok {
			t.Fatalf("routing target missing for %#v", command)
		}
		if _, exposed := target.(string); exposed {
			t.Fatalf("routing target exposes internal key %q", target)
		}
		if _, ok := target.(topologyRouteSlot); !ok {
			t.Fatalf("routing target = %T, want topologyRouteSlot", target)
		}
	}
}

func TestV080PublicRouteKeyRejectsReservedInternalKey(t *testing.T) {
	if _, err := (&RoutingTopology{}).RouteKey("f:{f}:route"); err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "internal key") {
		t.Fatalf("RouteKey reserved-key error = %v", err)
	}
}

func TestV080ExplicitRoutingAndTransactionsRejectReservedInternalKeys(t *testing.T) {
	client := NewClientWithExecutor(&fakeExecutor{value: []byte("OK")})
	assertRejected := func(name string, run func() error) {
		t.Helper()
		err := run()
		if err == nil || !strings.Contains(strings.ToLower(err.Error()), "internal key") {
			t.Fatalf("%s reserved-key error = %v", name, err)
		}
	}

	assertRejected("CommandForKey", func() error {
		_, err := client.CommandForKey(
			context.Background(), "f:{fa:7}:v:result", "MODULE.CUSTOM", "argument",
		)
		return err
	})
	assertRejected("Watch", func() error {
		return client.Watch(context.Background(), "f:{f}:watch")
	})
	assertRejected("TransactionForKeys", func() error {
		_, err := client.TransactionForKeys(context.Background(), "f:{flow-governance}:transaction")
		return err
	})
}

func TestV080TypedCommandRejectsReservedKeyInsideAffineWatchSession(t *testing.T) {
	exec := &transactionRecordingExecutor{calls: make(chan []any, 2)}
	client := NewClientWithExecutor(exec)
	if err := client.Watch(context.Background(), "safe-watch-key"); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Unwatch(context.Background()) }()
	<-exec.calls // WATCH

	_, err := client.Hash().Get(context.Background(), "H:private\x00field", "field")
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "internal key") {
		t.Fatalf("affine typed reserved-key error = %v", err)
	}
	select {
	case call := <-exec.calls:
		t.Fatalf("reserved affine typed command reached session: %#v", call)
	default:
	}
}
