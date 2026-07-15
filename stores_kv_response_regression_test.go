package ferricstore

import (
	"context"
	"strconv"
	"testing"
)

func TestKeyValueBooleanCommandsRejectStatusAcknowledgements(t *testing.T) {
	tests := []struct {
		name string
		call func(*KeyValueStore) error
	}{
		{name: "SETNX", call: func(store *KeyValueStore) error {
			_, err := store.SetNX(context.Background(), "key", "value")
			return err
		}},
		{name: "MSETNX", call: func(store *KeyValueStore) error {
			_, err := store.MSetNX(context.Background(), map[string]any{"key": "value"})
			return err
		}},
		{name: "EXPIRE", call: func(store *KeyValueStore) error {
			_, err := store.Expire(context.Background(), "key", 1)
			return err
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := NewClientWithExecutor(&fakeExecutor{value: []byte("OK")}).KV()
			if err := tc.call(store); err == nil {
				t.Fatal("boolean command accepted status acknowledgement as true")
			}
		})
	}
}

func TestKeyValueCountCommandsRejectImpossibleCounts(t *testing.T) {
	for _, command := range []struct {
		name string
		call func(*KeyValueStore) error
	}{
		{name: "DEL", call: func(store *KeyValueStore) error {
			_, err := store.Del(context.Background(), "one", "two")
			return err
		}},
		{name: "EXISTS", call: func(store *KeyValueStore) error {
			_, err := store.Exists(context.Background(), "one", "two")
			return err
		}},
	} {
		for _, count := range []int64{-1, 3} {
			t.Run(command.name+"/"+strconv.FormatInt(count, 10), func(t *testing.T) {
				store := NewClientWithExecutor(&fakeExecutor{value: count}).KV()
				if err := command.call(store); err == nil {
					t.Fatalf("%s accepted impossible count %d for two keys", command.name, count)
				}
			})
		}
	}
}

func TestKeyValueLengthCommandsRejectNegativeResponses(t *testing.T) {
	for _, command := range []struct {
		name string
		call func(*KeyValueStore) error
	}{
		{name: "APPEND", call: func(store *KeyValueStore) error {
			_, err := store.Append(context.Background(), "key", "value")
			return err
		}},
		{name: "STRLEN", call: func(store *KeyValueStore) error {
			_, err := store.StrLen(context.Background(), "key")
			return err
		}},
		{name: "SETRANGE", call: func(store *KeyValueStore) error {
			_, err := store.SetRange(context.Background(), "key", 0, "value")
			return err
		}},
	} {
		t.Run(command.name, func(t *testing.T) {
			if err := command.call(NewClientWithExecutor(&fakeExecutor{value: int64(-1)}).KV()); err == nil {
				t.Fatalf("%s accepted a negative length", command.name)
			}
		})
	}
}

func TestKeyValueTTLCommandsRejectUnknownNegativeSentinels(t *testing.T) {
	for _, command := range []struct {
		name string
		call func(*KeyValueStore) error
	}{
		{name: "TTL", call: func(store *KeyValueStore) error { _, err := store.TTL(context.Background(), "key"); return err }},
		{name: "PTTL", call: func(store *KeyValueStore) error { _, err := store.PTTL(context.Background(), "key"); return err }},
		{name: "EXPIRETIME", call: func(store *KeyValueStore) error {
			_, err := store.ExpireTime(context.Background(), "key")
			return err
		}},
		{name: "PEXPIRETIME", call: func(store *KeyValueStore) error {
			_, err := store.PExpireTime(context.Background(), "key")
			return err
		}},
	} {
		t.Run(command.name, func(t *testing.T) {
			if err := command.call(NewClientWithExecutor(&fakeExecutor{value: int64(-3)}).KV()); err == nil {
				t.Fatalf("%s accepted an unknown negative sentinel", command.name)
			}
			for _, valid := range []int64{-2, -1, 0} {
				if err := command.call(NewClientWithExecutor(&fakeExecutor{value: valid}).KV()); err != nil {
					t.Fatalf("%s rejected valid TTL value %d: %v", command.name, valid, err)
				}
			}
		})
	}
}
