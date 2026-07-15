package ferricstore

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
)

type affineBulkTestExecutor struct {
	mu            sync.Mutex
	calls         [][]any
	bulkMGetCalls int
	bulkMSetCalls int
	bulkMSetReply any
	queueResponse any
}

func (e *affineBulkTestExecutor) Do(context.Context, ...any) (any, error) {
	return nil, errors.New("unexpected non-session execution")
}

func (e *affineBulkTestExecutor) keyValueMGet(context.Context, []string) (any, error) {
	e.mu.Lock()
	e.bulkMGetCalls++
	e.mu.Unlock()
	return []any{[]byte("bulk")}, nil
}

func (e *affineBulkTestExecutor) keyValueMSet(context.Context, []string, []any) (any, error) {
	e.mu.Lock()
	e.bulkMSetCalls++
	response := e.bulkMSetReply
	e.mu.Unlock()
	if response != nil {
		return response, nil
	}
	return []byte("OK"), nil
}

func (e *affineBulkTestExecutor) acquireCommandSession(context.Context, ...any) (commandSession, error) {
	return &affineBulkTestSession{owner: e}, nil
}

func (e *affineBulkTestExecutor) record(args []any) {
	e.mu.Lock()
	e.calls = append(e.calls, append([]any(nil), args...))
	e.mu.Unlock()
}

type affineBulkTestSession struct {
	owner *affineBulkTestExecutor
}

func (s *affineBulkTestSession) Do(_ context.Context, args ...any) (any, error) {
	s.owner.record(args)
	switch commandName(args) {
	case "WATCH", "UNWATCH", "MULTI":
		return []byte("OK"), nil
	case "COMMAND_EXEC":
		if len(args) > 1 && commandPart(args[1]) == "MGET" {
			return []any{[]byte("session")}, nil
		}
		if s.owner.queueResponse != nil {
			return s.owner.queueResponse, nil
		}
		return []byte("QUEUED"), nil
	case "EXEC":
		return []any{[]byte("OK")}, nil
	default:
		return nil, errors.New("unexpected session command")
	}
}

func (*affineBulkTestSession) Abort(error) {}
func (*affineBulkTestSession) Release()    {}

func TestKeyValueMGetUsesWatchedSessionBeforeNativeBulkPath(t *testing.T) {
	exec := &affineBulkTestExecutor{}
	client := NewClientWithExecutor(exec)
	ctx := context.Background()

	if err := client.Watch(ctx, "watched"); err != nil {
		t.Fatal(err)
	}
	values, err := client.KV().MGet(ctx, "watched")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(values, []any{[]byte("session")}) {
		t.Fatalf("MGET after WATCH = %#v; want affine-session response", values)
	}
	if exec.bulkMGetCalls != 0 {
		t.Fatalf("MGET used native bulk path %d times during WATCH", exec.bulkMGetCalls)
	}
	if len(exec.calls) < 2 || commandName(exec.calls[1]) != "COMMAND_EXEC" || commandPart(exec.calls[1][1]) != "MGET" {
		t.Fatalf("MGET did not use watched session: %#v", exec.calls)
	}
	if err := client.Unwatch(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestKeyValueMSetQueuesOnLegacyMultiBeforeNativeBulkPath(t *testing.T) {
	exec := &affineBulkTestExecutor{}
	client := NewClientWithExecutor(exec)
	ctx := context.Background()

	if err := client.Multi(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.KV().MSet(ctx, map[string]any{"key": "value"}); err != nil {
		t.Fatal(err)
	}
	if exec.bulkMSetCalls != 0 {
		t.Fatalf("MSET used native bulk path %d times during MULTI", exec.bulkMSetCalls)
	}
	if len(exec.calls) < 2 || commandName(exec.calls[1]) != "COMMAND_EXEC" || commandPart(exec.calls[1][1]) != "MSET" {
		t.Fatalf("MSET did not queue on legacy session: %#v", exec.calls)
	}
	if _, err := client.Exec(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestKeyValueReplyMethodsRejectLegacyMultiBeforeQueueing(t *testing.T) {
	tests := []struct {
		name string
		call func(*KeyValueStore) error
	}{
		{name: "get", call: func(store *KeyValueStore) error { _, err := store.Get(context.Background(), "key"); return err }},
		{name: "mget", call: func(store *KeyValueStore) error { _, err := store.MGet(context.Background(), "key"); return err }},
		{name: "del", call: func(store *KeyValueStore) error { _, err := store.Del(context.Background(), "key"); return err }},
		{name: "exists", call: func(store *KeyValueStore) error { _, err := store.Exists(context.Background(), "key"); return err }},
		{name: "incr", call: func(store *KeyValueStore) error { _, err := store.Incr(context.Background(), "key"); return err }},
		{name: "decr", call: func(store *KeyValueStore) error { _, err := store.Decr(context.Background(), "key"); return err }},
		{name: "expire", call: func(store *KeyValueStore) error { _, err := store.Expire(context.Background(), "key", 1); return err }},
		{name: "ttl", call: func(store *KeyValueStore) error { _, err := store.TTL(context.Background(), "key"); return err }},
		{name: "set options", call: func(store *KeyValueStore) error {
			_, err := store.SetWithOptions(context.Background(), "key", "value", SetOptions{})
			return err
		}},
		{name: "msetnx", call: func(store *KeyValueStore) error {
			_, err := store.MSetNX(context.Background(), map[string]any{"key": "value"})
			return err
		}},
		{name: "incrby", call: func(store *KeyValueStore) error { _, err := store.IncrBy(context.Background(), "key", 1); return err }},
		{name: "decrby", call: func(store *KeyValueStore) error { _, err := store.DecrBy(context.Background(), "key", 1); return err }},
		{name: "incrbyfloat", call: func(store *KeyValueStore) error {
			_, err := store.IncrByFloat(context.Background(), "key", 1)
			return err
		}},
		{name: "append", call: func(store *KeyValueStore) error {
			_, err := store.Append(context.Background(), "key", "value")
			return err
		}},
		{name: "strlen", call: func(store *KeyValueStore) error { _, err := store.StrLen(context.Background(), "key"); return err }},
		{name: "getset", call: func(store *KeyValueStore) error {
			_, err := store.GetSet(context.Background(), "key", "value")
			return err
		}},
		{name: "getdel", call: func(store *KeyValueStore) error { _, err := store.GetDel(context.Background(), "key"); return err }},
		{name: "getex", call: func(store *KeyValueStore) error {
			_, err := store.GetEX(context.Background(), "key", GetEXOptions{})
			return err
		}},
		{name: "setnx", call: func(store *KeyValueStore) error {
			_, err := store.SetNX(context.Background(), "key", "value")
			return err
		}},
		{name: "getrange", call: func(store *KeyValueStore) error {
			_, err := store.GetRange(context.Background(), "key", 0, 1)
			return err
		}},
		{name: "setrange", call: func(store *KeyValueStore) error {
			_, err := store.SetRange(context.Background(), "key", 0, "value")
			return err
		}},
		{name: "pttl", call: func(store *KeyValueStore) error { _, err := store.PTTL(context.Background(), "key"); return err }},
		{name: "pexpire", call: func(store *KeyValueStore) error { _, err := store.PExpire(context.Background(), "key", 1); return err }},
		{name: "expireat", call: func(store *KeyValueStore) error { _, err := store.ExpireAt(context.Background(), "key", 1); return err }},
		{name: "pexpireat", call: func(store *KeyValueStore) error {
			_, err := store.PExpireAt(context.Background(), "key", 1)
			return err
		}},
		{name: "persist", call: func(store *KeyValueStore) error { _, err := store.Persist(context.Background(), "key"); return err }},
		{name: "expiretime", call: func(store *KeyValueStore) error { _, err := store.ExpireTime(context.Background(), "key"); return err }},
		{name: "pexpiretime", call: func(store *KeyValueStore) error { _, err := store.PExpireTime(context.Background(), "key"); return err }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exec := &affineBulkTestExecutor{}
			client := NewClientWithExecutor(exec)
			ctx := context.Background()
			if err := client.Multi(ctx); err != nil {
				t.Fatal(err)
			}
			exec.mu.Lock()
			callsBefore := len(exec.calls)
			exec.mu.Unlock()

			err := tc.call(client.KV())
			if !errors.Is(err, ErrTypedReplyInTransaction) {
				t.Fatalf("typed call error = %T %v; want ErrTypedReplyInTransaction", err, err)
			}
			exec.mu.Lock()
			callsAfter := len(exec.calls)
			exec.mu.Unlock()
			if callsAfter != callsBefore {
				t.Fatalf("typed call queued %d commands; want none", callsAfter-callsBefore)
			}
		})
	}
}

func TestKeyValueStatusMethodsRequireExactNormalAndQueuedResponses(t *testing.T) {
	statusCalls := []struct {
		name string
		call func(*KeyValueStore) error
	}{
		{name: "set", call: func(store *KeyValueStore) error { return store.Set(context.Background(), "key", "value") }},
		{name: "mset", call: func(store *KeyValueStore) error {
			return store.MSet(context.Background(), map[string]any{"key": "value"})
		}},
		{name: "setex", call: func(store *KeyValueStore) error { return store.SetEX(context.Background(), "key", 1, "value") }},
		{name: "psetex", call: func(store *KeyValueStore) error { return store.PSetEX(context.Background(), "key", 1, "value") }},
	}

	for _, tc := range statusCalls {
		t.Run(tc.name+" normal", func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("QUEUED")}
			if err := tc.call(NewClientWithExecutor(exec).KV()); err == nil {
				t.Fatal("accepted QUEUED outside MULTI")
			}
		})

		t.Run(tc.name+" queued", func(t *testing.T) {
			exec := &affineBulkTestExecutor{queueResponse: []byte("NOT-QUEUED")}
			client := NewClientWithExecutor(exec)
			if err := client.Multi(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := tc.call(client.KV()); err == nil {
				t.Fatal("accepted malformed queued response")
			}
		})
	}
}

func TestKeyValueMSetValidatesNativeBulkStatus(t *testing.T) {
	exec := &affineBulkTestExecutor{bulkMSetReply: []byte("QUEUED")}
	err := NewClientWithExecutor(exec).KV().MSet(context.Background(), map[string]any{"key": "value"})
	if err == nil {
		t.Fatal("native bulk MSET accepted QUEUED outside MULTI")
	}
	if exec.bulkMSetCalls != 1 {
		t.Fatalf("native bulk MSET calls = %d; want 1", exec.bulkMSetCalls)
	}
}

func TestKeyValueStatusAcceptsOnlySingleCompactOK(t *testing.T) {
	for _, tc := range []struct {
		name    string
		count   nativeCompactOKCount
		wantErr bool
	}{
		{name: "single", count: 1},
		{name: "empty", count: 0, wantErr: true},
		{name: "multiple", count: 2, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := NewClientWithExecutor(&fakeExecutor{value: tc.count}).KV().Set(context.Background(), "key", "value")
			if (err != nil) != tc.wantErr {
				t.Fatalf("compact OK count %d error = %v, wantErr %t", tc.count, err, tc.wantErr)
			}
		})
	}
}
