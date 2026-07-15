package ferricstore

import (
	"math"
	"reflect"
	"testing"
)

type nativeNamedMapKey string

func TestNativeEncoderRejectsNonStringMapKeys(t *testing.T) {
	tests := []any{
		map[int]string{1: "value"},
		map[any]any{"valid": "value", int64(2): "invalid"},
	}
	for _, value := range tests {
		if _, err := encodeNativeValue(value); err == nil {
			t.Fatalf("encodeNativeValue(%#v) accepted non-string map keys", value)
		}
	}
}

func TestNativeEncoderAcceptsNamedStringMapKeys(t *testing.T) {
	encoded, err := encodeNativeValue(map[nativeNamedMapKey]string{"field": "value"})
	if err != nil {
		t.Fatal(err)
	}
	decoded, rest, err := decodeNativeValue(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 0 {
		t.Fatalf("unexpected trailing bytes: %d", len(rest))
	}
	want := map[string]any{"field": []byte("value")}
	if !reflect.DeepEqual(decoded, want) {
		t.Fatalf("decoded value = %#v, want %#v", decoded, want)
	}
}

func TestNativeEncoderPreservesNamedScalarWireTypes(t *testing.T) {
	type namedBool bool
	type namedInt int32
	type namedUint uint16
	type namedFloat float32
	type namedUintptr uintptr

	values := []any{
		namedBool(true),
		namedInt(-42),
		namedUint(42),
		namedFloat(1.25),
		namedUintptr(7),
	}
	want := []any{true, int64(-42), int64(42), float64(1.25), int64(7)}
	encoded, err := encodeNativeValue(values)
	if err != nil {
		t.Fatal(err)
	}
	decoded, rest, err := decodeNativeValue(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 0 {
		t.Fatalf("unexpected trailing bytes: %d", len(rest))
	}
	if !reflect.DeepEqual(decoded, want) {
		t.Fatalf("decoded named scalars = %#v, want %#v", decoded, want)
	}
}

func TestNativeEncoderRejectsNamedUnsignedIntegerOverflow(t *testing.T) {
	type namedUint64 uint64
	if _, err := encodeNativeValue(namedUint64(math.MaxUint64)); err == nil {
		t.Fatal("named uint64 overflow was encoded")
	}
}
