package ferricstore

import (
	"reflect"
	"testing"
)

func TestV080CoordinationCommandsUseDedicatedNativeSchemas(t *testing.T) {
	tests := []struct {
		name   string
		opcode uint16
		args   []any
		want   map[string]any
	}{
		{
			name: "cas", opcode: 0x0106,
			args: []any{"CAS", "key", []byte("old"), []byte("new"), "EX", int64(2)},
			want: map[string]any{
				"key": "key", "expected": []byte("old"), "value": []byte("new"), "ttl": int64(2000),
			},
		},
		{name: "lock", opcode: 0x0107, args: []any{"LOCK", "key", "owner", int64(5000)}, want: map[string]any{"key": "key", "owner": "owner", "ttl_ms": int64(5000)}},
		{name: "unlock", opcode: 0x0108, args: []any{"UNLOCK", "key", "owner"}, want: map[string]any{"key": "key", "owner": "owner"}},
		{name: "extend", opcode: 0x0109, args: []any{"EXTEND", "key", "owner", int64(5000)}, want: map[string]any{"key": "key", "owner": "owner", "ttl_ms": int64(5000)}},
		{name: "ratelimit", opcode: 0x010A, args: []any{"RATELIMIT.ADD", "key", int64(1000), int64(10), int64(2)}, want: map[string]any{"key": "key", "window_ms": int64(1000), "max": int64(10), "count": int64(2)}},
		{name: "fetch", opcode: 0x010B, args: []any{"FETCH_OR_COMPUTE", "key", int64(5000), "hint"}, want: map[string]any{"key": "key", "ttl_ms": int64(5000), "hint": "hint"}},
		{name: "fetch result", opcode: 0x010C, args: []any{"FETCH_OR_COMPUTE_RESULT", "key", "token", []byte("value"), int64(5000)}, want: map[string]any{"key": "key", "token": "token", "value": []byte("value"), "ttl_ms": int64(5000)}},
		{name: "fetch error", opcode: 0x010D, args: []any{"FETCH_OR_COMPUTE_ERROR", "key", "token", "failed"}, want: map[string]any{"key": "key", "token": "token", "message": "failed"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertV080NativeCoreMap(t, test.args, test.opcode, test.want)
		})
	}
}

func TestV080CollectionCommandsUseDedicatedNativeSchemas(t *testing.T) {
	tests := []struct {
		name   string
		opcode uint16
		args   []any
		want   map[string]any
	}{
		{name: "hset", opcode: 0x0110, args: []any{"HSET", "hash", "a", []byte("1"), "b", []byte("2")}, want: map[string]any{"key": "hash", "fields": map[string]any{"a": []byte("1"), "b": []byte("2")}}},
		{name: "hget", opcode: 0x0111, args: []any{"HGET", "hash", "a"}, want: map[string]any{"key": "hash", "field": "a"}},
		{name: "hmget", opcode: 0x0112, args: []any{"HMGET", "hash", "a", "b"}, want: map[string]any{"key": "hash", "fields": []string{"a", "b"}}},
		{name: "hgetall", opcode: 0x0113, args: []any{"HGETALL", "hash"}, want: map[string]any{"key": "hash"}},
		{name: "lpush", opcode: 0x0120, args: []any{"LPUSH", "list", []byte("a"), []byte("b")}, want: map[string]any{"key": "list", "values": []any{[]byte("a"), []byte("b")}}},
		{name: "rpush", opcode: 0x0121, args: []any{"RPUSH", "list", []byte("a")}, want: map[string]any{"key": "list", "values": []any{[]byte("a")}}},
		{name: "lpop", opcode: 0x0122, args: []any{"LPOP", "list", 2}, want: map[string]any{"key": "list", "count": 2}},
		{name: "rpop", opcode: 0x0123, args: []any{"RPOP", "list"}, want: map[string]any{"key": "list"}},
		{name: "lrange", opcode: 0x0124, args: []any{"LRANGE", "list", int64(0), int64(-1)}, want: map[string]any{"key": "list", "start": int64(0), "stop": int64(-1)}},
		{name: "sadd", opcode: 0x0130, args: []any{"SADD", "set", []byte("a"), []byte("b")}, want: map[string]any{"key": "set", "members": []any{[]byte("a"), []byte("b")}}},
		{name: "srem", opcode: 0x0131, args: []any{"SREM", "set", []byte("a")}, want: map[string]any{"key": "set", "members": []any{[]byte("a")}}},
		{name: "smembers", opcode: 0x0132, args: []any{"SMEMBERS", "set"}, want: map[string]any{"key": "set"}},
		{name: "sismember", opcode: 0x0133, args: []any{"SISMEMBER", "set", []byte("a")}, want: map[string]any{"key": "set", "member": []byte("a")}},
		{name: "zadd", opcode: 0x0140, args: []any{"ZADD", "sorted", 1.5, []byte("a"), 2.5, []byte("b")}, want: map[string]any{"key": "sorted", "items": []any{[]any{1.5, []byte("a")}, []any{2.5, []byte("b")}}}},
		{name: "zrem", opcode: 0x0141, args: []any{"ZREM", "sorted", []byte("a")}, want: map[string]any{"key": "sorted", "members": []any{[]byte("a")}}},
		{name: "zrange", opcode: 0x0142, args: []any{"ZRANGE", "sorted", int64(0), int64(-1), "WITHSCORES"}, want: map[string]any{"key": "sorted", "start": int64(0), "stop": int64(-1), "withscores": true}},
		{name: "zscore", opcode: 0x0143, args: []any{"ZSCORE", "sorted", []byte("a")}, want: map[string]any{"key": "sorted", "member": []byte("a")}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertV080NativeCoreMap(t, test.args, test.opcode, test.want)
		})
	}
}

func assertV080NativeCoreMap(t *testing.T, args []any, opcode uint16, want map[string]any) {
	t.Helper()
	command, err := buildNativeCommand(args)
	if err != nil {
		t.Fatal(err)
	}
	if command.opcode != opcode || command.laneID != 1 || command.flags != 0 {
		t.Fatalf("command = opcode %#x lane %d flags %#x, want %#x/1/0", command.opcode, command.laneID, command.flags, opcode)
	}
	payload, ok := command.payload.(map[string]any)
	if !ok || !reflect.DeepEqual(payload, want) {
		t.Fatalf("payload = %#v, want %#v", command.payload, want)
	}
	if _, err := encodeNativeValue(payload); err != nil {
		t.Fatalf("encode payload: %v", err)
	}
}

func TestV080TypedCorePreservesRawCommandScalarCoercion(t *testing.T) {
	tests := [][]any{
		{"GET", 42},
		{"SET", 42, "value"},
		{"DEL", 42},
		{"MGET", 42},
		{"CAS", 42, "old", "new"},
		{"LOCK", "lock", "owner", "5000"},
		{"FETCH_OR_COMPUTE", "cache", "5000"},
		{"HGET", "hash", 42},
		{"HGETALL", 42},
		{"LPUSH", "list", 42},
		{"LPOP", "list", "2"},
		{"LRANGE", "list", "0", "-1"},
		{"SADD", "set", 42},
		{"SISMEMBER", "set", 42},
		{"ZADD", "sorted", 1.5, 42},
		{"ZREM", "sorted", 42},
		{"ZRANGE", "sorted", "0", "-1"},
		{"ZSCORE", "sorted", 42},
	}
	for _, args := range tests {
		command, err := buildNativeCommand(args)
		if err != nil {
			t.Fatalf("%s: %v", args[0], err)
		}
		if command.opcode != nativeOpCommandExec {
			t.Fatalf("%s raw scalar opcode = %#x, want COMMAND_EXEC coercion", args[0], command.opcode)
		}
	}
}

func TestV080TypedHSetAllowsEmptyField(t *testing.T) {
	assertV080NativeCoreMap(t,
		[]any{"HSET", "hash", "", []byte("value")},
		nativeOpHSet,
		map[string]any{"key": "hash", "fields": map[string]any{"": []byte("value")}},
	)
}

type nonBinaryCoreCodec struct{}

func (nonBinaryCoreCodec) Encode(any) (any, error)       { return 42, nil }
func (nonBinaryCoreCodec) Decode(value any) (any, error) { return value, nil }

func TestV080TypedCollectionsDoNotAssumeCustomCodecProducesBinary(t *testing.T) {
	deferred := nativeDeferredCodecValue{codec: nonBinaryCoreCodec{}, value: "member"}
	for _, args := range [][]any{
		{"LPUSH", "list", deferred},
		{"SADD", "set", deferred},
		{"ZADD", "sorted", 1.5, deferred},
	} {
		command, err := buildNativeCommand(args)
		if err != nil {
			t.Fatalf("%s: %v", args[0], err)
		}
		if command.opcode != nativeOpCommandExec {
			t.Fatalf("%s custom-codec opcode = %#x, want COMMAND_EXEC", args[0], command.opcode)
		}
	}
}
