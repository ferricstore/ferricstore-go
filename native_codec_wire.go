package ferricstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
)

const nativeMaxEncodeDepth = 64

// nativePreencodedPayload carries an already validated native value body to
// the writer. It is internal so callers cannot bypass normal wire validation.
type nativePreencodedPayload struct{ body []byte }

type nativeEncodeLimitError struct{ limit int }

func (e nativeEncodeLimitError) Error() string {
	return fmt.Sprintf("ferricstore native request body exceeds %d-byte encoding limit", e.limit)
}

type nativeEncodeBuffer struct {
	bytes.Buffer
	limit int
}

func (b *nativeEncodeBuffer) ensure(size int) error {
	if size < 0 || size > b.limit-b.Len() {
		return nativeEncodeLimitError{limit: b.limit}
	}
	return nil
}

func (b *nativeEncodeBuffer) writeByte(value byte) error {
	if err := b.ensure(1); err != nil {
		return err
	}
	return b.WriteByte(value)
}

func (b *nativeEncodeBuffer) write(value []byte) error {
	if err := b.ensure(len(value)); err != nil {
		return err
	}
	_, err := b.Write(value)
	return err
}

func (b *nativeEncodeBuffer) writeString(value string) error {
	if err := b.ensure(len(value)); err != nil {
		return err
	}
	_, err := b.WriteString(value)
	return err
}

type nativeEncodeVisit struct {
	kind   reflect.Kind
	typeID reflect.Type
	ptr    uintptr
	len    int
	cap    int
}

type nativeEncodeState struct {
	remaining int
	visiting  map[nativeEncodeVisit]struct{}
}

func encodeNativeValue(value any) ([]byte, error) {
	return encodeNativeValueWithLimit(value, nativeMaxFrameBytes)
}

func encodeNativeValueWithLimit(value any, limit int) ([]byte, error) {
	if limit <= 0 {
		return nil, errors.New("ferricstore native request encoding limit must be positive")
	}
	buf := &nativeEncodeBuffer{limit: limit}
	state := &nativeEncodeState{remaining: nativeMaxContainerItems, visiting: make(map[nativeEncodeVisit]struct{})}
	if err := writeNativeValue(buf, value, state, 0); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *nativeEncodeState) enter(value any, depth int) (func(), error) {
	if depth > nativeMaxEncodeDepth {
		return nil, fmt.Errorf("ferricstore native request exceeds maximum nesting depth %d", nativeMaxEncodeDepth)
	}
	s.remaining--
	if s.remaining < 0 {
		return nil, fmt.Errorf("ferricstore native request exceeds maximum item count %d", nativeMaxContainerItems)
	}
	visit, ok := nativeEncodeIdentity(value)
	if !ok {
		return func() {}, nil
	}
	if _, exists := s.visiting[visit]; exists {
		return nil, errors.New("ferricstore native request contains a reference cycle")
	}
	s.visiting[visit] = struct{}{}
	return func() { delete(s.visiting, visit) }, nil
}

func nativeEncodeIdentity(value any) (nativeEncodeVisit, bool) {
	rv := reflect.ValueOf(value)
	for rv.IsValid() && rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			return nativeEncodeVisit{}, false
		}
		rv = rv.Elem()
	}
	if !rv.IsValid() {
		return nativeEncodeVisit{}, false
	}
	switch rv.Kind() {
	case reflect.Map, reflect.Pointer:
		if rv.IsNil() {
			return nativeEncodeVisit{}, false
		}
		return nativeEncodeVisit{kind: rv.Kind(), typeID: rv.Type(), ptr: rv.Pointer()}, true
	case reflect.Slice:
		if rv.IsNil() || rv.Type().Elem().Kind() == reflect.Uint8 {
			return nativeEncodeVisit{}, false
		}
		return nativeEncodeVisit{kind: rv.Kind(), typeID: rv.Type(), ptr: rv.Pointer(), len: rv.Len(), cap: rv.Cap()}, true
	default:
		return nativeEncodeVisit{}, false
	}
}

func writeNativeValue(buf *nativeEncodeBuffer, value any, state *nativeEncodeState, depth int) error {
	switch deferred := value.(type) {
	case nativeDeferredCodecValue:
		encoded, err := encodeNativeDeferredCodecValue(deferred)
		if err != nil {
			return err
		}
		return writeNativeValue(buf, encoded, state, depth)
	case nativeJSONCommandArg:
		encoded, err := deferred.encode()
		if err != nil {
			return err
		}
		return writeNativeValue(buf, encoded, state, depth)
	}
	leave, err := state.enter(value, depth)
	if err != nil {
		return err
	}
	defer leave()
	switch v := value.(type) {
	case nil:
		return buf.writeByte(0)
	case bool:
		if v {
			return buf.writeByte(1)
		}
		return buf.writeByte(2)
	case int:
		return writeNativeInt(buf, int64(v))
	case int8:
		return writeNativeInt(buf, int64(v))
	case int16:
		return writeNativeInt(buf, int64(v))
	case int32:
		return writeNativeInt(buf, int64(v))
	case int64:
		return writeNativeInt(buf, v)
	case uint:
		return writeNativeUint(buf, uint64(v))
	case uint8:
		return writeNativeUint(buf, uint64(v))
	case uint16:
		return writeNativeUint(buf, uint64(v))
	case uint32:
		return writeNativeUint(buf, uint64(v))
	case uint64:
		return writeNativeUint(buf, v)
	case float32:
		return writeNativeFloat(buf, float64(v))
	case float64:
		return writeNativeFloat(buf, v)
	case string:
		return writeNativeString(buf, v)
	case []byte:
		return writeNativeBytes(buf, v)
	case nativeMGetPayload:
		return writeNativeMGetPayload(buf, v, state, depth)
	case nativeMSetPayload:
		return writeNativeMSetPayload(buf, v, state, depth)
	case nativeKeyValueCommandPayload:
		return writeNativeKeyValueCommandPayload(buf, v, state, depth)
	case nativeKeyCommandPayload:
		return writeNativeKeyCommandPayload(buf, v, state, depth)
	case []any:
		return writeNativeArray(buf, v, state, depth)
	case map[string]any:
		return writeNativeMap(buf, v, state, depth)
	default:
		return writeNativeReflect(buf, value, state, depth)
	}
}

func writeNativeMGetPayload(buf *nativeEncodeBuffer, payload nativeMGetPayload, state *nativeEncodeState, depth int) error {
	if err := ensureNativeEncodeContainerBudget("map", 1, state.remaining); err != nil {
		return err
	}
	if err := writeNativeSingleFieldMap(buf, "keys"); err != nil {
		return err
	}
	var container any = payload.args
	count := len(payload.args)
	if payload.stringKeys != nil {
		container = payload.stringKeys
		count = len(payload.stringKeys)
	}
	leave, err := state.enter(container, depth+1)
	if err != nil {
		return err
	}
	defer leave()
	if err := ensureNativeEncodeContainerBudget("array", count, state.remaining); err != nil {
		return err
	}
	if err := writeNativeContainerHeader(buf, 5, count); err != nil {
		return err
	}
	if payload.stringKeys != nil {
		for _, key := range payload.stringKeys {
			if err := writeNativeValue(buf, key, state, depth+2); err != nil {
				return err
			}
		}
		return nil
	}
	for _, key := range payload.args {
		if err := writeNativeValue(buf, key, state, depth+2); err != nil {
			return err
		}
	}
	return nil
}

func writeNativeMSetPayload(buf *nativeEncodeBuffer, payload nativeMSetPayload, state *nativeEncodeState, depth int) error {
	if err := ensureNativeEncodeContainerBudget("map", 1, state.remaining); err != nil {
		return err
	}
	if err := writeNativeSingleFieldMap(buf, "pairs"); err != nil {
		return err
	}
	count := len(payload.args) / 2
	if payload.keys != nil {
		if len(payload.keys) != len(payload.values) {
			return fmt.Errorf("ferricstore native MSET payload has %d keys and %d values", len(payload.keys), len(payload.values))
		}
		count = len(payload.keys)
	} else if len(payload.args)%2 != 0 {
		return errors.New("ferricstore native MSET payload requires key/value pairs")
	}
	leave, err := state.enter(payload, depth+1)
	if err != nil {
		return err
	}
	defer leave()
	if err := ensureNativeEncodeContainerBudget("array", count, state.remaining); err != nil {
		return err
	}
	if err := writeNativeContainerHeader(buf, 5, count); err != nil {
		return err
	}
	for index := range count {
		pairLeave, err := state.enter(index, depth+2)
		if err != nil {
			return err
		}
		if err := ensureNativeEncodeContainerBudget("array", 2, state.remaining); err != nil {
			pairLeave()
			return err
		}
		if err := writeNativeContainerHeader(buf, 5, 2); err != nil {
			pairLeave()
			return err
		}
		var key, value any
		if payload.keys != nil {
			key, value = payload.keys[index], payload.values[index]
		} else {
			key, value = payload.args[2*index], payload.args[2*index+1]
		}
		if err := writeNativeValue(buf, key, state, depth+3); err != nil {
			pairLeave()
			return err
		}
		if err := writeNativeValue(buf, value, state, depth+3); err != nil {
			pairLeave()
			return err
		}
		pairLeave()
	}
	return nil
}

func writeNativeKeyValueCommandPayload(
	buf *nativeEncodeBuffer,
	payload nativeKeyValueCommandPayload,
	state *nativeEncodeState,
	depth int,
) error {
	if len(payload.keys) != len(payload.values) {
		return fmt.Errorf("ferricstore native %s payload has %d keys and %d values", payload.command, len(payload.keys), len(payload.values))
	}
	if err := ensureNativeEncodeContainerBudget("map", 2, state.remaining); err != nil {
		return err
	}
	if err := writeNativeContainerHeader(buf, 6, 2); err != nil {
		return err
	}
	if err := writeNativeMapKey(buf, "command"); err != nil {
		return err
	}
	if err := writeNativeValue(buf, payload.command, state, depth+1); err != nil {
		return err
	}
	if err := writeNativeMapKey(buf, "args"); err != nil {
		return err
	}
	count := 2 * len(payload.keys)
	leave, err := state.enter(payload, depth+1)
	if err != nil {
		return err
	}
	defer leave()
	if err := ensureNativeEncodeContainerBudget("array", count, state.remaining); err != nil {
		return err
	}
	if err := writeNativeContainerHeader(buf, 5, count); err != nil {
		return err
	}
	for index, key := range payload.keys {
		if err := writeNativeValue(buf, key, state, depth+2); err != nil {
			return err
		}
		if err := writeNativeValue(buf, payload.values[index], state, depth+2); err != nil {
			return err
		}
	}
	return nil
}

func writeNativeMapKey(buf *nativeEncodeBuffer, key string) error {
	if uint64(len(key)) > math.MaxUint32 {
		return errors.New("ferricstore native map key is too large")
	}
	var raw [4]byte
	binary.BigEndian.PutUint32(raw[:], uint32(len(key)))
	if err := buf.write(raw[:]); err != nil {
		return err
	}
	return buf.writeString(key)
}

func writeNativeSingleFieldMap(buf *nativeEncodeBuffer, key string) error {
	if err := writeNativeContainerHeader(buf, 6, 1); err != nil {
		return err
	}
	return writeNativeMapKey(buf, key)
}

func writeNativeInt(buf *nativeEncodeBuffer, value int64) error {
	if err := buf.writeByte(3); err != nil {
		return err
	}
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], uint64(value))
	return buf.write(raw[:])
}

func writeNativeUint(buf *nativeEncodeBuffer, value uint64) error {
	if value > math.MaxInt64 {
		return fmt.Errorf("ferricstore native integer overflows int64: %d", value)
	}
	return writeNativeInt(buf, int64(value))
}

func writeNativeFloat(buf *nativeEncodeBuffer, value float64) error {
	if err := buf.writeByte(7); err != nil {
		return err
	}
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], math.Float64bits(value))
	return buf.write(raw[:])
}

func writeNativeBytes(buf *nativeEncodeBuffer, value []byte) error {
	if uint64(len(value)) > math.MaxUint32 {
		return errors.New("ferricstore native binary is too large")
	}
	if err := writeNativeContainerHeader(buf, 4, len(value)); err != nil {
		return err
	}
	return buf.write(value)
}

func writeNativeString(buf *nativeEncodeBuffer, value string) error {
	if uint64(len(value)) > math.MaxUint32 {
		return errors.New("ferricstore native binary is too large")
	}
	if err := writeNativeContainerHeader(buf, 4, len(value)); err != nil {
		return err
	}
	return buf.writeString(value)
}

func writeNativeContainerHeader(buf *nativeEncodeBuffer, tag byte, count int) error {
	if err := buf.writeByte(tag); err != nil {
		return err
	}
	var raw [4]byte
	binary.BigEndian.PutUint32(raw[:], uint32(count))
	return buf.write(raw[:])
}

func writeNativeArray(buf *nativeEncodeBuffer, values []any, state *nativeEncodeState, depth int) error {
	if uint64(len(values)) > math.MaxUint32 {
		return errors.New("ferricstore native array is too large")
	}
	if err := ensureNativeEncodeContainerBudget("array", len(values), state.remaining); err != nil {
		return err
	}
	if err := writeNativeContainerHeader(buf, 5, len(values)); err != nil {
		return err
	}
	for _, value := range values {
		if err := writeNativeValue(buf, value, state, depth+1); err != nil {
			return err
		}
	}
	return nil
}

func writeNativeMap(buf *nativeEncodeBuffer, values map[string]any, state *nativeEncodeState, depth int) error {
	if uint64(len(values)) > math.MaxUint32 {
		return errors.New("ferricstore native map is too large")
	}
	if err := ensureNativeEncodeContainerBudget("map", len(values), state.remaining); err != nil {
		return err
	}
	if err := writeNativeContainerHeader(buf, 6, len(values)); err != nil {
		return err
	}
	var localKeys [16]string
	keys := localKeys[:0]
	if len(values) > len(localKeys) {
		keys = make([]string, 0, len(values))
	}
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var raw [4]byte
	for _, key := range keys {
		if uint64(len(key)) > math.MaxUint32 {
			return errors.New("ferricstore native map key is too large")
		}
		binary.BigEndian.PutUint32(raw[:], uint32(len(key)))
		if err := buf.write(raw[:]); err != nil {
			return err
		}
		if err := buf.writeString(key); err != nil {
			return err
		}
		if err := writeNativeValue(buf, values[key], state, depth+1); err != nil {
			return err
		}
	}
	return nil
}

func ensureNativeEncodeContainerBudget(kind string, count, remaining int) error {
	if count > remaining {
		return fmt.Errorf("ferricstore native %s exceeds aggregate item limit", kind)
	}
	return nil
}
