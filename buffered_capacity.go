package ferricstore

import "reflect"

// bufferedCommandRetainedSize conservatively estimates memory a command
// snapshot would retain before that snapshot allocates. The depth bound keeps
// cyclic inputs finite, and the snapshotter subsequently rejects those inputs.
// Conservative container overhead keeps the byte limit fail-closed.
func bufferedCommandRetainedSize(args []any, limit int) (int, bool) {
	if limit <= 0 {
		return 0, false
	}
	budget := bufferedSizeBudget{remaining: uint64(limit)}
	if len(args) > (int(^uint(0)>>1)-24)/16 {
		return 0, false
	}
	if !budget.consume(24 + 16*len(args)) {
		return 0, false
	}
	for _, arg := range args {
		if !budget.consumeValue(reflect.ValueOf(arg), 0) {
			return 0, false
		}
	}
	return limit - int(budget.remaining), true
}

type bufferedSizeBudget struct {
	remaining uint64
}

func (b *bufferedSizeBudget) consume(size int) bool {
	if size < 0 || uint64(size) > b.remaining {
		return false
	}
	b.remaining -= uint64(size)
	return true
}

func (b *bufferedSizeBudget) consumeValue(value reflect.Value, depth int) bool {
	if !value.IsValid() {
		return b.consume(1)
	}
	if depth > nativeMaxEncodeDepth {
		return false
	}
	switch value.Kind() {
	case reflect.Interface:
		if !b.consume(16) {
			return false
		}
		if value.IsNil() {
			return true
		}
		return b.consumeValue(value.Elem(), depth+1)
	case reflect.Pointer:
		if !b.consume(8) {
			return false
		}
		if value.IsNil() {
			return true
		}
		return b.consumeValue(value.Elem(), depth+1)
	case reflect.String:
		return b.consume(16) && b.consume(value.Len())
	case reflect.Slice:
		if !b.consume(24) {
			return false
		}
		if value.IsNil() {
			return true
		}
		if value.Type().Elem().Kind() == reflect.Uint8 {
			return b.consume(value.Len())
		}
		for index := 0; index < value.Len(); index++ {
			if !b.consumeValue(value.Index(index), depth+1) {
				return false
			}
		}
		return true
	case reflect.Array:
		if !b.consumeReflectSize(value.Type().Size()) {
			return false
		}
		for index := 0; index < value.Len(); index++ {
			if !b.consumeValue(value.Index(index), depth+1) {
				return false
			}
		}
		return true
	case reflect.Map:
		if !b.consume(8) {
			return false
		}
		if value.IsNil() {
			return true
		}
		if value.Len() > int(^uint(0)>>1)/64 || !b.consume(64*value.Len()) {
			return false
		}
		iterator := value.MapRange()
		for iterator.Next() {
			if !b.consumeValue(iterator.Key(), depth+1) || !b.consumeValue(iterator.Value(), depth+1) {
				return false
			}
		}
		return true
	case reflect.Struct:
		if !b.consumeReflectSize(value.Type().Size()) {
			return false
		}
		for index := 0; index < value.NumField(); index++ {
			if value.Type().Field(index).PkgPath == "" && !b.consumeValue(value.Field(index), depth+1) {
				return false
			}
		}
		return true
	default:
		return b.consumeReflectSize(value.Type().Size())
	}
}

func (b *bufferedSizeBudget) consumeReflectSize(size uintptr) bool {
	if uint64(size) > b.remaining {
		return false
	}
	b.remaining -= uint64(size)
	return true
}
