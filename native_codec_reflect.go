package ferricstore

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
)

func writeNativeReflect(buf *nativeEncodeBuffer, value any, state *nativeEncodeState, depth int) error {
	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return buf.writeByte(0)
	}
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface:
		if rv.IsNil() {
			return buf.writeByte(0)
		}
		if stringer, ok := value.(fmt.Stringer); ok {
			return writeNativeString(buf, stringer.String())
		}
		return writeNativeValue(buf, rv.Elem().Interface(), state, depth+1)
	case reflect.Bool:
		if rv.Bool() {
			return buf.writeByte(1)
		}
		return buf.writeByte(2)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return writeNativeInt(buf, rv.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return writeNativeUint(buf, rv.Uint())
	case reflect.Float32, reflect.Float64:
		return writeNativeFloat(buf, rv.Float())
	case reflect.String:
		return writeNativeString(buf, rv.String())
	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return writeNativeBytes(buf, rv.Bytes())
		}
		fallthrough
	case reflect.Array:
		if rv.Len() > math.MaxUint32 {
			return errors.New("ferricstore native array is too large")
		}
		if err := ensureNativeEncodeContainerBudget("array", rv.Len(), state.remaining); err != nil {
			return err
		}
		if err := writeNativeContainerHeader(buf, 5, rv.Len()); err != nil {
			return err
		}
		for index := 0; index < rv.Len(); index++ {
			if err := writeNativeValue(buf, rv.Index(index).Interface(), state, depth+1); err != nil {
				return err
			}
		}
		return nil
	case reflect.Map:
		if err := ensureNativeEncodeContainerBudget("map", rv.Len(), state.remaining); err != nil {
			return err
		}
		entries, err := sortedNativeReflectMapEntries(rv)
		if err != nil {
			return err
		}
		if err := writeNativeContainerHeader(buf, 6, rv.Len()); err != nil {
			return err
		}
		for _, entry := range entries {
			if err := writeNativeMapKey(buf, entry.key); err != nil {
				return err
			}
			if err := writeNativeValue(buf, entry.value.Interface(), state, depth+1); err != nil {
				return err
			}
		}
		return nil
	default:
		if stringer, ok := value.(fmt.Stringer); ok {
			return writeNativeString(buf, stringer.String())
		}
		return fmt.Errorf("ferricstore native value type %T is unsupported", value)
	}
}

type nativeReflectMapEntry struct {
	key   string
	value reflect.Value
}

func sortedNativeReflectMapEntries(value reflect.Value) ([]nativeReflectMapEntry, error) {
	entries := make([]nativeReflectMapEntry, 0, value.Len())
	iter := value.MapRange()
	for iter.Next() {
		key, err := nativeReflectMapKey(iter.Key())
		if err != nil {
			return nil, err
		}
		entries = append(entries, nativeReflectMapEntry{key: key, value: iter.Value()})
	}
	sort.Slice(entries, func(left, right int) bool { return entries[left].key < entries[right].key })
	for index := 1; index < len(entries); index++ {
		if entries[index-1].key == entries[index].key {
			return nil, fmt.Errorf("ferricstore native map contains duplicate textual key %q", entries[index].key)
		}
	}
	return entries, nil
}

func nativeReflectMapKey(value reflect.Value) (string, error) {
	for value.IsValid() && value.Kind() == reflect.Interface {
		if value.IsNil() {
			return "", errors.New("ferricstore native map key must be a string, got nil")
		}
		value = value.Elem()
	}
	if !value.IsValid() || value.Kind() != reflect.String {
		return "", fmt.Errorf("ferricstore native map key must be a string, got %s", value.Kind())
	}
	return value.String(), nil
}
