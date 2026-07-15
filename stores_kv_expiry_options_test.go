package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestKeyValueConditionalExpiryCommandsBuildExactArguments(t *testing.T) {
	tests := []struct {
		name string
		call func(*KeyValueStore) (bool, error)
		want []any
	}{
		{
			name: "expire nx",
			call: func(store *KeyValueStore) (bool, error) {
				return store.ExpireWithOptions(context.Background(), "key", 10, ExpireOptions{NX: true})
			},
			want: []any{"EXPIRE", "key", int64(10), "NX"},
		},
		{
			name: "pexpire xx",
			call: func(store *KeyValueStore) (bool, error) {
				return store.PExpireWithOptions(context.Background(), "key", 20, ExpireOptions{XX: true})
			},
			want: []any{"PEXPIRE", "key", int64(20), "XX"},
		},
		{
			name: "expireat gt",
			call: func(store *KeyValueStore) (bool, error) {
				return store.ExpireAtWithOptions(context.Background(), "key", 30, ExpireOptions{GT: true})
			},
			want: []any{"EXPIREAT", "key", int64(30), "GT"},
		},
		{
			name: "pexpireat lt",
			call: func(store *KeyValueStore) (bool, error) {
				return store.PExpireAtWithOptions(context.Background(), "key", 40, ExpireOptions{LT: true})
			},
			want: []any{"PEXPIREAT", "key", int64(40), "LT"},
		},
		{
			name: "no condition",
			call: func(store *KeyValueStore) (bool, error) {
				return store.ExpireWithOptions(context.Background(), "key", 50, ExpireOptions{})
			},
			want: []any{"EXPIRE", "key", int64(50)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: int64(1)}
			applied, err := test.call(NewClientWithExecutor(exec).KV())
			if err != nil || !applied {
				t.Fatalf("conditional expiry = %v, %v; want true, nil", applied, err)
			}
			if len(exec.calls) != 1 || !reflect.DeepEqual(exec.calls[0], test.want) {
				t.Fatalf("command calls = %#v; want %#v", exec.calls, test.want)
			}
		})
	}
}

func TestKeyValueConditionalExpiryRejectsConflictsBeforeTransport(t *testing.T) {
	for _, options := range []ExpireOptions{
		{NX: true, XX: true},
		{NX: true, GT: true},
		{XX: true, LT: true},
		{GT: true, LT: true},
	} {
		exec := &fakeExecutor{value: int64(1)}
		_, err := NewClientWithExecutor(exec).KV().ExpireWithOptions(
			context.Background(), "key", 10, options,
		)
		if err == nil {
			t.Fatalf("conflicting options %#v succeeded", options)
		}
		if len(exec.calls) != 0 {
			t.Fatalf("conflicting options %#v reached executor: %#v", options, exec.calls)
		}
	}
}
