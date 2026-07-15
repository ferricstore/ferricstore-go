package ferricstore

import (
	"bytes"
	"context"
	"math"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
)

type countingKVCodec struct {
	encodes atomic.Int64
}

func (c *countingKVCodec) Encode(value any) (any, error) {
	c.encodes.Add(1)
	return value, nil
}

func (*countingKVCodec) Decode(value any) (any, error) { return value, nil }

func TestKeyValueSetOptionsRejectInvalidCombinationsLocally(t *testing.T) {
	expiry := int64(10)
	tests := []struct {
		name string
		opt  SetOptions
	}{
		{name: "nx and xx", opt: SetOptions{NX: true, XX: true}},
		{name: "multiple expiries", opt: SetOptions{EXSeconds: &expiry, PXMilliseconds: &expiry}},
		{name: "keep ttl and expiry", opt: SetOptions{EXSeconds: &expiry, KeepTTL: true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			client := NewClientWithExecutor(exec)
			if _, err := client.KV().SetWithOptions(context.Background(), "key", "value", tc.opt); err == nil {
				t.Fatal("expected invalid SET options to fail")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid SET options reached executor: %#v", exec.calls)
			}
		})
	}
}

func TestKeyValueSetOptionsValidatesNonGetAcknowledgement(t *testing.T) {
	for _, test := range []struct {
		name    string
		value   any
		opt     SetOptions
		wantErr bool
	}{
		{name: "OK bytes", value: []byte("OK")},
		{name: "unconditional nil", value: nil, wantErr: true},
		{name: "condition not applied", value: nil, opt: SetOptions{NX: true}},
		{name: "GET missing prior value", value: nil, opt: SetOptions{Get: true}},
		{name: "malformed integer", value: int64(1), wantErr: true},
		{name: "malformed status", value: []byte("QUEUED"), wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: test.value}
			value, err := NewClientWithExecutor(exec).KV().SetWithOptions(
				context.Background(), "key", "value", test.opt,
			)
			if (err != nil) != test.wantErr {
				t.Fatalf("SetWithOptions() = %#v, %v; wantErr=%v", value, err, test.wantErr)
			}
			if !test.wantErr && !reflect.DeepEqual(value, test.value) {
				t.Fatalf("SetWithOptions() value = %#v, want %#v", value, test.value)
			}
		})
	}
}

func TestKeyValueGetEXRejectsMultipleExpiryModesLocally(t *testing.T) {
	expiry := int64(10)
	tests := []GetEXOptions{
		{EXSeconds: &expiry, PXMilliseconds: &expiry},
		{EXSeconds: &expiry, Persist: true},
	}
	for _, opt := range tests {
		exec := &fakeExecutor{value: []byte("value")}
		client := NewClientWithExecutor(exec)
		if _, err := client.KV().GetEX(context.Background(), "key", opt); err == nil {
			t.Fatal("expected invalid GETEX options to fail")
		}
		if len(exec.calls) != 0 {
			t.Fatalf("invalid GETEX options reached executor: %#v", exec.calls)
		}
	}
}

func TestKeyValueExpiryCommandsRejectNonPositiveValuesBeforeCodecOrTransport(t *testing.T) {
	for _, value := range []int64{0, -1} {
		t.Run(strconv.FormatInt(value, 10), func(t *testing.T) {
			tests := []struct {
				name string
				call func(*KeyValueStore) error
			}{
				{name: "SET EX", call: func(store *KeyValueStore) error {
					_, err := store.SetWithOptions(context.Background(), "key", "value", SetOptions{EXSeconds: &value})
					return err
				}},
				{name: "SET PX", call: func(store *KeyValueStore) error {
					_, err := store.SetWithOptions(context.Background(), "key", "value", SetOptions{PXMilliseconds: &value})
					return err
				}},
				{name: "SET EXAT", call: func(store *KeyValueStore) error {
					_, err := store.SetWithOptions(context.Background(), "key", "value", SetOptions{EXATSeconds: &value})
					return err
				}},
				{name: "SET PXAT", call: func(store *KeyValueStore) error {
					_, err := store.SetWithOptions(context.Background(), "key", "value", SetOptions{PXATMillis: &value})
					return err
				}},
				{name: "GETEX EX", call: func(store *KeyValueStore) error {
					_, err := store.GetEX(context.Background(), "key", GetEXOptions{EXSeconds: &value})
					return err
				}},
				{name: "GETEX PX", call: func(store *KeyValueStore) error {
					_, err := store.GetEX(context.Background(), "key", GetEXOptions{PXMilliseconds: &value})
					return err
				}},
				{name: "GETEX EXAT", call: func(store *KeyValueStore) error {
					_, err := store.GetEX(context.Background(), "key", GetEXOptions{EXATSeconds: &value})
					return err
				}},
				{name: "GETEX PXAT", call: func(store *KeyValueStore) error {
					_, err := store.GetEX(context.Background(), "key", GetEXOptions{PXATMillis: &value})
					return err
				}},
				{name: "SETEX", call: func(store *KeyValueStore) error {
					return store.SetEX(context.Background(), "key", value, "value")
				}},
				{name: "PSETEX", call: func(store *KeyValueStore) error {
					return store.PSetEX(context.Background(), "key", value, "value")
				}},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					exec := &fakeExecutor{value: []byte("OK")}
					codec := &countingKVCodec{}
					store := NewClientWithExecutor(exec, WithConcurrentCodec(codec)).KV()
					if err := test.call(store); err == nil {
						t.Fatal("non-positive expiry succeeded")
					}
					if codec.encodes.Load() != 0 {
						t.Fatalf("invalid expiry invoked codec %d times", codec.encodes.Load())
					}
					if len(exec.calls) != 0 {
						t.Fatalf("invalid expiry reached executor: %#v", exec.calls)
					}
				})
			}
		})
	}
}

func TestKeyValueNumericMutationBoundsFailBeforeTransport(t *testing.T) {
	tests := []struct {
		name string
		call func(*KeyValueStore) error
	}{
		{name: "negative SETRANGE offset", call: func(store *KeyValueStore) error {
			_, err := store.SetRange(context.Background(), "key", -1, "value")
			return err
		}},
		{name: "oversized SETRANGE offset", call: func(store *KeyValueStore) error {
			_, err := store.SetRange(context.Background(), "key", 536_870_912, "value")
			return err
		}},
		{name: "NaN INCRBYFLOAT", call: func(store *KeyValueStore) error {
			_, err := store.IncrByFloat(context.Background(), "key", math.NaN())
			return err
		}},
		{name: "positive infinity INCRBYFLOAT", call: func(store *KeyValueStore) error {
			_, err := store.IncrByFloat(context.Background(), "key", math.Inf(1))
			return err
		}},
		{name: "negative infinity INCRBYFLOAT", call: func(store *KeyValueStore) error {
			_, err := store.IncrByFloat(context.Background(), "key", math.Inf(-1))
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: int64(1)}
			if err := test.call(NewClientWithExecutor(exec).KV()); err == nil {
				t.Fatal("invalid numeric mutation succeeded")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid numeric mutation reached executor: %#v", exec.calls)
			}
		})
	}
}

func TestJSONCodecEncodesNilAsJSONNull(t *testing.T) {
	encoded, err := (JSONCodec{}).Encode(nil)
	if err != nil {
		t.Fatal(err)
	}
	value, ok := encoded.([]byte)
	if !ok || !bytes.Equal(value, []byte("null")) {
		t.Fatalf("JSON nil encoded as %#v; want JSON null bytes", encoded)
	}
	decoded, err := (JSONCodec{}).Decode(value)
	if err != nil || decoded != nil {
		t.Fatalf("JSON null decoded as %#v, %v", decoded, err)
	}
}

func TestKeyValueByteMutationRejectsStructuredValuesBeforeTransport(t *testing.T) {
	for _, call := range []struct {
		name string
		do   func(*KeyValueStore) error
	}{
		{name: "append", do: func(store *KeyValueStore) error {
			_, err := store.Append(context.Background(), "key", map[string]any{"not": "raw"})
			return err
		}},
		{name: "setrange", do: func(store *KeyValueStore) error {
			_, err := store.SetRange(context.Background(), "key", 0, map[string]any{"not": "raw"})
			return err
		}},
	} {
		t.Run(call.name, func(t *testing.T) {
			exec := &fakeExecutor{value: int64(1)}
			if err := call.do(NewClientWithExecutor(exec).KV()); err == nil {
				t.Fatal("expected structured byte mutation value to fail")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid raw value reached executor: %#v", exec.calls)
			}
		})
	}
}

func TestKeyValueByteMutationsAcceptNamedRawTypes(t *testing.T) {
	type namedString string
	type namedBytes []byte

	for _, test := range []struct {
		name  string
		value any
		call  func(*KeyValueStore, any) error
	}{
		{name: "append string", value: namedString("tail"), call: func(store *KeyValueStore, value any) error {
			_, err := store.Append(context.Background(), "key", value)
			return err
		}},
		{name: "append bytes", value: namedBytes("tail"), call: func(store *KeyValueStore, value any) error {
			_, err := store.Append(context.Background(), "key", value)
			return err
		}},
		{name: "setrange string", value: namedString("part"), call: func(store *KeyValueStore, value any) error {
			_, err := store.SetRange(context.Background(), "key", 2, value)
			return err
		}},
		{name: "setrange bytes", value: namedBytes("part"), call: func(store *KeyValueStore, value any) error {
			_, err := store.SetRange(context.Background(), "key", 2, value)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: int64(4)}
			if err := test.call(NewClientWithExecutor(exec).KV(), test.value); err != nil {
				t.Fatalf("raw mutation failed: %v", err)
			}
			if len(exec.calls) != 1 {
				t.Fatalf("executor calls = %d, want 1", len(exec.calls))
			}
			if got := exec.calls[0][len(exec.calls[0])-1]; !reflect.DeepEqual(got, test.value) {
				t.Fatalf("wire value = %#v, want %#v", got, test.value)
			}
		})
	}
}

func TestKeyValueEmptyCollectionsAreHandledLocally(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	store := NewClientWithExecutor(exec).KV()
	ctx := context.Background()

	values, err := store.MGet(ctx)
	if err != nil {
		t.Errorf("empty MGET: %v", err)
	}
	if values == nil || len(values) != 0 {
		t.Errorf("empty MGET = %#v; want non-nil empty slice", values)
	}
	if count, err := store.Del(ctx); err != nil || count != 0 {
		t.Errorf("empty DEL = %d, %v; want 0, nil", count, err)
	}
	if count, err := store.Exists(ctx); err != nil || count != 0 {
		t.Errorf("empty EXISTS = %d, %v; want 0, nil", count, err)
	}
	if err := store.MSet(ctx, map[string]any{}); err != nil {
		t.Errorf("empty MSET: %v", err)
	}
	if _, err := store.MSetNX(ctx, map[string]any{}); err == nil {
		t.Error("empty MSETNX succeeded; want a local validation error")
	}
	if len(exec.calls) != 0 {
		t.Fatalf("empty collection calls reached executor: %#v", exec.calls)
	}
}
