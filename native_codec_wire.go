package ferricstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
)

func encodeNativeValue(value any) ([]byte, error) {
	var buf bytes.Buffer
	if err := writeNativeValue(&buf, value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeNativeValue(buf *bytes.Buffer, value any) error {
	switch v := value.(type) {
	case nil:
		buf.WriteByte(0)
	case bool:
		if v {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(2)
		}
	case int:
		writeNativeInt(buf, int64(v))
	case int8:
		writeNativeInt(buf, int64(v))
	case int16:
		writeNativeInt(buf, int64(v))
	case int32:
		writeNativeInt(buf, int64(v))
	case int64:
		writeNativeInt(buf, v)
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
		writeNativeFloat(buf, float64(v))
	case float64:
		writeNativeFloat(buf, v)
	case string:
		writeNativeBytes(buf, []byte(v))
	case []byte:
		writeNativeBytes(buf, v)
	case []any:
		return writeNativeArray(buf, v)
	case map[string]any:
		return writeNativeMap(buf, v)
	default:
		return writeNativeReflect(buf, value)
	}
	return nil
}

func writeNativeReflect(buf *bytes.Buffer, value any) error {
	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		buf.WriteByte(0)
		return nil
	}
	for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			buf.WriteByte(0)
			return nil
		}
		rv = rv.Elem()
	}
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		if rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
			writeNativeBytes(buf, rv.Bytes())
			return nil
		}
		items := make([]any, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			items = append(items, rv.Index(i).Interface())
		}
		return writeNativeArray(buf, items)
	case reflect.Map:
		mapping := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			mapping[fmt.Sprint(iter.Key().Interface())] = iter.Value().Interface()
		}
		return writeNativeMap(buf, mapping)
	default:
		writeNativeBytes(buf, []byte(fmt.Sprint(value)))
		return nil
	}
}

func writeNativeInt(buf *bytes.Buffer, value int64) {
	buf.WriteByte(3)
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], uint64(value))
	buf.Write(raw[:])
}

func writeNativeUint(buf *bytes.Buffer, value uint64) error {
	if value > math.MaxInt64 {
		return fmt.Errorf("ferricstore native integer overflows int64: %d", value)
	}
	writeNativeInt(buf, int64(value))
	return nil
}

func writeNativeFloat(buf *bytes.Buffer, value float64) {
	buf.WriteByte(7)
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], math.Float64bits(value))
	buf.Write(raw[:])
}

func writeNativeBytes(buf *bytes.Buffer, value []byte) {
	buf.WriteByte(4)
	var raw [4]byte
	binary.BigEndian.PutUint32(raw[:], uint32(len(value)))
	buf.Write(raw[:])
	buf.Write(value)
}

func writeNativeArray(buf *bytes.Buffer, values []any) error {
	buf.WriteByte(5)
	var raw [4]byte
	binary.BigEndian.PutUint32(raw[:], uint32(len(values)))
	buf.Write(raw[:])
	for _, value := range values {
		if err := writeNativeValue(buf, value); err != nil {
			return err
		}
	}
	return nil
}

func writeNativeMap(buf *bytes.Buffer, values map[string]any) error {
	buf.WriteByte(6)
	var raw [4]byte
	binary.BigEndian.PutUint32(raw[:], uint32(len(values)))
	buf.Write(raw[:])
	for key, value := range values {
		binary.BigEndian.PutUint32(raw[:], uint32(len(key)))
		buf.Write(raw[:])
		buf.WriteString(key)
		if err := writeNativeValue(buf, value); err != nil {
			return err
		}
	}
	return nil
}

func decodeNativeValue(data []byte) (any, []byte, error) {
	if len(data) == 0 {
		return nil, nil, errors.New("ferricstore native value is empty")
	}
	tag := data[0]
	rest := data[1:]
	switch tag {
	case 0:
		return nil, rest, nil
	case 1:
		return true, rest, nil
	case 2:
		return false, rest, nil
	case 3:
		if len(rest) < 8 {
			return nil, nil, errors.New("ferricstore native integer is truncated")
		}
		return int64(binary.BigEndian.Uint64(rest[:8])), rest[8:], nil
	case 4:
		if len(rest) < 4 {
			return nil, nil, errors.New("ferricstore native binary length is truncated")
		}
		size := int(binary.BigEndian.Uint32(rest[:4]))
		rest = rest[4:]
		if len(rest) < size {
			return nil, nil, errors.New("ferricstore native binary is truncated")
		}
		return append([]byte(nil), rest[:size]...), rest[size:], nil
	case 5:
		if len(rest) < 4 {
			return nil, nil, errors.New("ferricstore native array length is truncated")
		}
		count, err := nativeBoundedItemCount("array", binary.BigEndian.Uint32(rest[:4]), len(rest)-4, 1)
		if err != nil {
			return nil, nil, err
		}
		rest = rest[4:]
		items := make([]any, 0, count)
		for i := 0; i < count; i++ {
			value, next, err := decodeNativeValue(rest)
			if err != nil {
				return nil, nil, err
			}
			items = append(items, value)
			rest = next
		}
		return items, rest, nil
	case 6:
		if len(rest) < 4 {
			return nil, nil, errors.New("ferricstore native map length is truncated")
		}
		count, err := nativeBoundedItemCount("map", binary.BigEndian.Uint32(rest[:4]), len(rest)-4, 5)
		if err != nil {
			return nil, nil, err
		}
		rest = rest[4:]
		mapping := make(map[string]any, count)
		for i := 0; i < count; i++ {
			if len(rest) < 4 {
				return nil, nil, errors.New("ferricstore native map key length is truncated")
			}
			keySize := int(binary.BigEndian.Uint32(rest[:4]))
			rest = rest[4:]
			if len(rest) < keySize {
				return nil, nil, errors.New("ferricstore native map key is truncated")
			}
			key := string(rest[:keySize])
			rest = rest[keySize:]
			value, next, err := decodeNativeValue(rest)
			if err != nil {
				return nil, nil, err
			}
			mapping[key] = value
			rest = next
		}
		return mapping, rest, nil
	case 7:
		if len(rest) < 8 {
			return nil, nil, errors.New("ferricstore native float is truncated")
		}
		return math.Float64frombits(binary.BigEndian.Uint64(rest[:8])), rest[8:], nil
	default:
		return nil, nil, fmt.Errorf("ferricstore native value has unknown tag %d", tag)
	}
}

func nativeErrorMessage(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	case map[string]any:
		if message := asString(v["message"]); message != "" {
			return message
		}
		if code := asString(v["code"]); code != "" {
			return code
		}
	}
	return fmt.Sprint(value)
}
