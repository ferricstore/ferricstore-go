package ferricstore

import (
	"fmt"
	"reflect"
	"time"
)

var commandSnapshotTimeType = reflect.TypeOf(time.Time{})

type commandCloneVisit struct {
	kind   reflect.Kind
	typeID reflect.Type
	ptr    uintptr
	len    int
	cap    int
}

func snapshotCommandArgs(args []any) ([]any, error) {
	out := make([]any, len(args))
	var visiting map[commandCloneVisit]struct{}
	for index, arg := range args {
		cloned, err := snapshotCommandArg(arg, &visiting)
		if err != nil {
			return nil, fmt.Errorf("snapshot command argument %d: %w", index, err)
		}
		out[index] = cloned
	}
	return out, nil
}

func snapshotCommandArg(arg any, visiting *map[commandCloneVisit]struct{}) (any, error) {
	if arg == nil {
		return nil, nil
	}
	value := reflect.ValueOf(arg)
	switch value.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64,
		reflect.Complex64, reflect.Complex128,
		reflect.String:
		return arg, nil
	}
	if bytesValue, ok := arg.([]byte); ok {
		return append([]byte(nil), bytesValue...), nil
	}
	if *visiting == nil {
		*visiting = make(map[commandCloneVisit]struct{})
	}
	cloned, err := cloneCommandReflect(value, *visiting, 0)
	if err != nil || !cloned.IsValid() {
		return nil, err
	}
	return cloned.Interface(), nil
}

func cloneCommandReflect(value reflect.Value, visiting map[commandCloneVisit]struct{}, depth int) (reflect.Value, error) {
	if !value.IsValid() {
		return reflect.Value{}, nil
	}
	if depth > nativeMaxEncodeDepth {
		return reflect.Value{}, fmt.Errorf("mutable value exceeds maximum nesting depth %d", nativeMaxEncodeDepth)
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		cloned, err := cloneCommandReflect(value.Elem(), visiting, depth+1)
		if err != nil {
			return reflect.Value{}, err
		}
		out := reflect.New(value.Type()).Elem()
		out.Set(cloned)
		return out, nil
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		leave, err := enterCommandClone(value, visiting)
		if err != nil {
			return reflect.Value{}, err
		}
		defer leave()
		cloned, err := cloneCommandReflect(value.Elem(), visiting, depth+1)
		if err != nil {
			return reflect.Value{}, err
		}
		out := reflect.New(value.Type().Elem())
		out.Elem().Set(cloned)
		return out, nil
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		leave, err := enterCommandClone(value, visiting)
		if err != nil {
			return reflect.Value{}, err
		}
		defer leave()
		out := reflect.MakeMapWithSize(value.Type(), value.Len())
		cloneKeys := commandMapKeyNeedsClone(value.Type().Key())
		iter := value.MapRange()
		for iter.Next() {
			clonedKey := iter.Key()
			if cloneKeys {
				clonedKey, err = cloneCommandReflect(clonedKey, visiting, depth+1)
				if err != nil {
					return reflect.Value{}, err
				}
			}
			clonedValue, err := cloneCommandReflect(iter.Value(), visiting, depth+1)
			if err != nil {
				return reflect.Value{}, err
			}
			out.SetMapIndex(clonedKey, clonedValue)
		}
		return out, nil
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		leave, err := enterCommandClone(value, visiting)
		if err != nil {
			return reflect.Value{}, err
		}
		defer leave()
		out := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for index := 0; index < value.Len(); index++ {
			cloned, err := cloneCommandReflect(value.Index(index), visiting, depth+1)
			if err != nil {
				return reflect.Value{}, err
			}
			out.Index(index).Set(cloned)
		}
		return out, nil
	case reflect.Array:
		out := reflect.New(value.Type()).Elem()
		for index := 0; index < value.Len(); index++ {
			cloned, err := cloneCommandReflect(value.Index(index), visiting, depth+1)
			if err != nil {
				return reflect.Value{}, err
			}
			out.Index(index).Set(cloned)
		}
		return out, nil
	case reflect.Struct:
		out := reflect.New(value.Type()).Elem()
		out.Set(value)
		if value.Type() == commandSnapshotTimeType {
			return out, nil
		}
		for index := 0; index < value.NumField(); index++ {
			if value.Type().Field(index).PkgPath != "" {
				if commandValueRetainsMutableState(value.Field(index), depth+1) {
					return reflect.Value{}, fmt.Errorf(
						"struct %s contains unexported mutable field %s",
						value.Type(), value.Type().Field(index).Name,
					)
				}
				continue
			}
			cloned, err := cloneCommandReflect(value.Field(index), visiting, depth+1)
			if err != nil {
				return reflect.Value{}, err
			}
			out.Field(index).Set(cloned)
		}
		return out, nil
	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return reflect.Value{}, fmt.Errorf("command argument kind %s cannot be snapshotted safely", value.Kind())
	default:
		return value, nil
	}
}

func commandValueRetainsMutableState(value reflect.Value, depth int) bool {
	if !value.IsValid() || depth > nativeMaxEncodeDepth {
		return true
	}
	if value.Type() == commandSnapshotTimeType {
		return false
	}
	switch value.Kind() {
	case reflect.Interface:
		return !value.IsNil() && commandValueRetainsMutableState(value.Elem(), depth+1)
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return !value.IsNil()
	case reflect.Array:
		for index := 0; index < value.Len(); index++ {
			if commandValueRetainsMutableState(value.Index(index), depth+1) {
				return true
			}
		}
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			if commandValueRetainsMutableState(value.Field(index), depth+1) {
				return true
			}
		}
	}
	return false
}

func commandMapKeyNeedsClone(valueType reflect.Type) bool {
	if valueType == commandSnapshotTimeType {
		return false
	}
	switch valueType.Kind() {
	case reflect.Interface, reflect.Pointer:
		return true
	case reflect.Array:
		return commandMapKeyNeedsClone(valueType.Elem())
	case reflect.Struct:
		for index := 0; index < valueType.NumField(); index++ {
			if commandMapKeyNeedsClone(valueType.Field(index).Type) {
				return true
			}
		}
	}
	return false
}

func enterCommandClone(value reflect.Value, visiting map[commandCloneVisit]struct{}) (func(), error) {
	visit := commandCloneVisit{kind: value.Kind(), typeID: value.Type(), ptr: value.Pointer()}
	if value.Kind() == reflect.Slice {
		visit.len = value.Len()
		visit.cap = value.Cap()
	}
	if _, exists := visiting[visit]; exists {
		return nil, fmt.Errorf("mutable command argument contains a reference cycle")
	}
	visiting[visit] = struct{}{}
	return func() { delete(visiting, visit) }, nil
}
