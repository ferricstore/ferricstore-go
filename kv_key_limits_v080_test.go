package ferricstore

import (
	"context"
	"strings"
	"testing"
)

func TestV080StringCommandsRejectInvalidKeysLocally(t *testing.T) {
	tooLarge := strings.Repeat("k", 65_536)
	tests := []struct {
		name string
		args []any
		want string
	}{
		{name: "GET empty", args: []any{"GET", ""}, want: "empty"},
		{name: "GET too large", args: []any{"GET", tooLarge}, want: "too large"},
		{name: "SET empty", args: []any{"SET", "", "value"}, want: "empty"},
		{name: "SET too large", args: []any{"SET", tooLarge, "value"}, want: "too large"},
		{name: "MSET empty", args: []any{"MSET", "key", "value", "", "value"}, want: "empty"},
		{name: "MSET too large", args: []any{"MSET", tooLarge, "value"}, want: "too large"},
		{name: "MSETNX empty", args: []any{"MSETNX", "", "value"}, want: "empty"},
		{name: "MSETNX too large", args: []any{"MSETNX", tooLarge, "value"}, want: "too large"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			_, err := NewClientWithExecutor(exec).Command(context.Background(), test.args...)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), test.want) {
				t.Fatalf("invalid key error = %v", err)
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid key reached executor: %#v", exec.calls)
			}
		})
	}
}

func TestV080TypedSetRejectsInvalidKeyBeforeEncoding(t *testing.T) {
	for _, key := range []string{"", strings.Repeat("k", 65_536)} {
		exec := &fakeExecutor{value: []byte("OK")}
		codec := &countingKVCodec{}
		store := NewClientWithExecutor(exec, WithConcurrentCodec(codec)).KV()
		if err := store.Set(context.Background(), key, "value"); err == nil {
			t.Fatalf("Set key length %d succeeded", len(key))
		}
		if codec.encodes.Load() != 0 || len(exec.calls) != 0 {
			t.Fatalf("invalid Set performed work: encodes=%d calls=%#v", codec.encodes.Load(), exec.calls)
		}
	}
}

func TestV080StringKeyLimitIsInclusiveAndCommandSpecific(t *testing.T) {
	maximum := strings.Repeat("k", 65_535)
	exec := &fakeExecutor{value: []byte("value")}
	client := NewClientWithExecutor(exec)
	if _, err := client.Command(context.Background(), "GET", maximum); err != nil {
		t.Fatalf("GET maximum-size key: %v", err)
	}
	for _, command := range [][]any{
		{"APPEND", "", "value"},
		{"GETEX", strings.Repeat("k", 65_536)},
		{"MGET", ""},
	} {
		if _, err := client.Command(context.Background(), command...); err != nil {
			t.Fatalf("%s was narrowed beyond v0.8.0 contract: %v", command[0], err)
		}
	}
	if len(exec.calls) != 4 {
		t.Fatalf("valid calls = %#v", exec.calls)
	}
}
