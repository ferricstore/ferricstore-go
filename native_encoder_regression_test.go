package ferricstore

import (
	"math"
	"reflect"
	"testing"
)

type nativeNamedMapKey string

type pointerNativeStringer struct{ value string }

func (value *pointerNativeStringer) String() string { return value.value }

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

func TestNativeEncoderRoundTripsNamedUnsignedInteger(t *testing.T) {
	type namedUint64 uint64
	encoded, err := encodeNativeValue(namedUint64(math.MaxUint64))
	if err != nil {
		t.Fatal(err)
	}
	decoded, rest, err := decodeNativeValue(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 0 || decoded != uint64(math.MaxUint64) {
		t.Fatalf("decoded unsigned integer = %#v with %d trailing bytes", decoded, len(rest))
	}
}

func TestNativeEncoderRejectsUnsupportedReflectKinds(t *testing.T) {
	type record struct{ Value string }
	for _, value := range []any{
		record{Value: "data"},
		func() {},
		make(chan int),
		complex(1, 2),
	} {
		if _, err := encodeNativeValue(map[string]any{"value": value}); err == nil {
			t.Fatalf("native encoder silently formatted unsupported %T", value)
		}
	}
}

func TestNativeEncoderMapWireOrderIsDeterministic(t *testing.T) {
	values := []any{
		map[string]any{"z": int64(1), "a": int64(2), "m": int64(3), "b": int64(4)},
		map[nativeNamedMapKey]int{"z": 1, "a": 2, "m": 3, "b": 4},
	}
	for _, value := range values {
		want, err := encodeNativeValue(value)
		if err != nil {
			t.Fatal(err)
		}
		for iteration := 0; iteration < 100; iteration++ {
			got, err := encodeNativeValue(value)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("identical %T map encoded differently on iteration %d", value, iteration)
			}
		}
	}
}

func TestNativeEncoderAcceptsPointerStringersAndNilPointers(t *testing.T) {
	var nilStringer *pointerNativeStringer
	encoded, err := encodeNativeValue([]any{&pointerNativeStringer{value: "wire"}, nilStringer})
	if err != nil {
		t.Fatalf("encode pointer Stringer: %v", err)
	}
	decoded, rest, err := decodeNativeValue(encoded)
	if err != nil || len(rest) != 0 {
		t.Fatalf("decode pointer Stringer: rest=%d err=%v", len(rest), err)
	}
	want := []any{[]byte("wire"), nil}
	if !reflect.DeepEqual(decoded, want) {
		t.Fatalf("decoded pointer Stringer = %#v, want %#v", decoded, want)
	}
}
