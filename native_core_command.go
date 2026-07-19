package ferricstore

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

func buildV080CoreNativeCommand(name string, args []any) (nativeCommand, bool, error) {
	switch name {
	case "CAS":
		return buildNativeCAS(args)
	case "LOCK":
		return buildCheckedNativeMap(name, nativeOpLock, args, []int{0, 1}, []int{2}, "key", "owner", "ttl_ms")
	case "UNLOCK":
		return buildCheckedNativeMap(name, nativeOpUnlock, args, []int{0, 1}, nil, "key", "owner")
	case "EXTEND":
		return buildCheckedNativeMap(name, nativeOpExtend, args, []int{0, 1}, []int{2}, "key", "owner", "ttl_ms")
	case "RATELIMIT.ADD":
		if len(args) == 3 {
			return buildCheckedNativeMap(name, nativeOpRateLimitAdd, args, []int{0}, []int{1, 2}, "key", "window_ms", "max")
		}
		return buildCheckedNativeMap(name, nativeOpRateLimitAdd, args, []int{0}, []int{1, 2, 3}, "key", "window_ms", "max", "count")
	case "FETCH_OR_COMPUTE":
		if len(args) == 2 {
			return buildCheckedNativeMap(name, nativeOpFetchOrCompute, args, []int{0}, []int{1}, "key", "ttl_ms")
		}
		return buildCheckedNativeMap(name, nativeOpFetchOrCompute, args, []int{0, 2}, []int{1}, "key", "ttl_ms", "hint")
	case "FETCH_OR_COMPUTE_RESULT":
		return buildCheckedNativeMap(name, nativeOpFetchOrComputeResult, args, []int{0, 1}, []int{3}, "key", "token", "value", "ttl_ms")
	case "FETCH_OR_COMPUTE_ERROR":
		return buildCheckedNativeMap(name, nativeOpFetchOrComputeError, args, []int{0, 1, 2}, nil, "key", "token", "message")
	case "HSET":
		return buildNativeHSet(args)
	case "HGET":
		return buildCheckedNativeMap(name, nativeOpHGet, args, []int{0, 1}, nil, "key", "field")
	case "HMGET":
		return buildNativeKeyList(name, nativeOpHMGet, args, "fields")
	case "HGETALL":
		return buildCheckedNativeMap(name, nativeOpHGetAll, args, []int{0}, nil, "key")
	case "LPUSH":
		return buildNativeKeyList(name, nativeOpLPush, args, "values")
	case "RPUSH":
		return buildNativeKeyList(name, nativeOpRPush, args, "values")
	case "LPOP":
		return buildNativePop(name, nativeOpLPop, args)
	case "RPOP":
		return buildNativePop(name, nativeOpRPop, args)
	case "LRANGE":
		return buildCheckedNativeMap(name, nativeOpLRange, args, []int{0}, []int{1, 2}, "key", "start", "stop")
	case "SADD":
		return buildNativeKeyList(name, nativeOpSAdd, args, "members")
	case "SREM":
		return buildNativeKeyList(name, nativeOpSRem, args, "members")
	case "SMEMBERS":
		return buildCheckedNativeMap(name, nativeOpSMembers, args, []int{0}, nil, "key")
	case "SISMEMBER":
		return buildNativeBinaryMember(name, nativeOpSIsMember, args)
	case "ZADD":
		return buildNativeZAdd(args)
	case "ZREM":
		return buildNativeKeyList(name, nativeOpZRem, args, "members")
	case "ZRANGE":
		return buildNativeZRange(args)
	case "ZSCORE":
		return buildNativeBinaryMember(name, nativeOpZScore, args)
	default:
		return nativeCommand{}, false, nil
	}
}

func buildCheckedNativeMap(
	name string,
	opcode uint16,
	args []any,
	binaryFields []int,
	integerFields []int,
	fields ...string,
) (nativeCommand, bool, error) {
	if len(args) != len(fields) {
		return nativeCommand{}, true, fmt.Errorf("%s requires %d arguments", name, len(fields))
	}
	for _, index := range binaryFields {
		if !nativeBinaryCandidate(args[index]) {
			return nativeCommand{}, false, nil
		}
	}
	for _, index := range integerFields {
		if !nativeIntegerCandidate(args[index]) {
			return nativeCommand{}, false, nil
		}
	}
	return buildFixedNativeMap(name, opcode, args, fields...)
}

func buildFixedNativeMap(name string, opcode uint16, args []any, fields ...string) (nativeCommand, bool, error) {
	if len(args) != len(fields) {
		return nativeCommand{}, true, fmt.Errorf("%s requires %d arguments", name, len(fields))
	}
	payload := make(map[string]any, len(fields))
	for index, field := range fields {
		payload[field] = args[index]
	}
	return nativeCommand{name: name, opcode: opcode, laneID: 1, payload: payload}, true, nil
}

func buildNativeCAS(args []any) (nativeCommand, bool, error) {
	if len(args) != 3 && len(args) != 5 {
		return nativeCommand{}, true, errors.New("CAS requires key, expected, value, and optional EX seconds")
	}
	if !nativeBinaryCandidate(args[0]) {
		return nativeCommand{}, false, nil
	}
	payload := map[string]any{"key": args[0], "expected": args[1], "value": args[2]}
	if len(args) == 5 {
		if !strings.EqualFold(asString(args[3]), "EX") {
			return nativeCommand{}, false, nil
		}
		seconds, err := responseInt64(args[4], nil)
		if err != nil {
			return nativeCommand{}, true, fmt.Errorf("CAS EX: %w", err)
		}
		if err := validateNativeTTLSecondsV080("CAS", seconds); err != nil {
			return nativeCommand{}, true, err
		}
		payload["ttl"] = seconds * 1000
	}
	return nativeCommand{name: "CAS", opcode: nativeOpCAS, laneID: 1, payload: payload}, true, nil
}

func buildNativeHSet(args []any) (nativeCommand, bool, error) {
	if len(args) < 3 || len(args)%2 == 0 {
		return nativeCommand{}, true, errors.New("HSET requires key and field/value pairs")
	}
	if !nativeBinaryCandidate(args[0]) {
		return nativeCommand{}, false, nil
	}
	fields := make(map[string]any, (len(args)-1)/2)
	for index := 1; index < len(args); index += 2 {
		field, ok := commandText(args[index])
		if !ok {
			return nativeCommand{}, false, nil
		}
		fields[field] = args[index+1]
	}
	return nativeCommand{
		name: "HSET", opcode: nativeOpHSet, laneID: 1,
		payload: map[string]any{"key": args[0], "fields": fields},
	}, true, nil
}

func buildNativeKeyList(name string, opcode uint16, args []any, field string) (nativeCommand, bool, error) {
	if len(args) < 2 {
		return nativeCommand{}, true, fmt.Errorf("%s requires a key and values", name)
	}
	if !nativeBinaryCandidate(args[0]) {
		return nativeCommand{}, false, nil
	}
	values := append([]any(nil), args[1:]...)
	for _, value := range values {
		if !nativeStoredBinaryCandidate(value) {
			return nativeCommand{}, false, nil
		}
	}
	if field == "fields" {
		fields := make([]string, len(values))
		for index, value := range values {
			fields[index] = asString(value)
		}
		return nativeCommand{
			name: name, opcode: opcode, laneID: 1,
			payload: map[string]any{"key": args[0], field: fields},
		}, true, nil
	}
	return nativeCommand{
		name: name, opcode: opcode, laneID: 1,
		payload: map[string]any{"key": args[0], field: values},
	}, true, nil
}

func buildNativePop(name string, opcode uint16, args []any) (nativeCommand, bool, error) {
	if len(args) == 1 {
		return buildCheckedNativeMap(name, opcode, args, []int{0}, nil, "key")
	}
	return buildCheckedNativeMap(name, opcode, args, []int{0}, []int{1}, "key", "count")
}

func buildNativeZAdd(args []any) (nativeCommand, bool, error) {
	if len(args) < 3 || len(args)%2 == 0 {
		return nativeCommand{}, true, errors.New("ZADD requires key and score/member pairs")
	}
	if !nativeBinaryCandidate(args[0]) {
		return nativeCommand{}, false, nil
	}
	items := make([]any, 0, (len(args)-1)/2)
	for index := 1; index < len(args); index += 2 {
		if !nativeNumericValue(args[index]) || !nativeStoredBinaryCandidate(args[index+1]) {
			return nativeCommand{}, false, nil
		}
		items = append(items, []any{args[index], args[index+1]})
	}
	return nativeCommand{
		name: "ZADD", opcode: nativeOpZAdd, laneID: 1,
		payload: map[string]any{"key": args[0], "items": items},
	}, true, nil
}

func buildNativeBinaryMember(name string, opcode uint16, args []any) (nativeCommand, bool, error) {
	if len(args) != 2 {
		return nativeCommand{}, true, fmt.Errorf("%s requires key and member", name)
	}
	if !nativeBinaryCandidate(args[0]) || !nativeStoredBinaryCandidate(args[1]) {
		return nativeCommand{}, false, nil
	}
	return buildFixedNativeMap(name, opcode, args, "key", "member")
}

func nativeStoredBinaryCandidate(value any) bool {
	if nativeBinaryCandidate(value) {
		return true
	}
	deferred, ok := value.(nativeDeferredCodecValue)
	if !ok {
		return false
	}
	switch originalCodec(deferred.codec).(type) {
	case StringCodec, *StringCodec, JSONCodec, *JSONCodec:
		return true
	default:
		return false
	}
}

func nativeBinaryCandidate(value any) bool {
	_, ok := commandText(value)
	return ok
}

func nativeIntegerCandidate(value any) bool {
	if value == nil {
		return false
	}
	switch reflect.ValueOf(value).Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	default:
		return false
	}
}

func nativeNumericValue(value any) bool {
	if value == nil {
		return false
	}
	switch reflect.ValueOf(value).Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

func buildNativeZRange(args []any) (nativeCommand, bool, error) {
	if len(args) != 3 && len(args) != 4 {
		return nativeCommand{}, false, nil
	}
	command, exact, err := buildCheckedNativeMap("ZRANGE", nativeOpZRange, args[:3], []int{0}, []int{1, 2}, "key", "start", "stop")
	if err != nil {
		return nativeCommand{}, true, err
	}
	if !exact {
		return nativeCommand{}, false, nil
	}
	if len(args) == 4 {
		if !strings.EqualFold(asString(args[3]), "WITHSCORES") {
			return nativeCommand{}, false, nil
		}
		command.payload.(map[string]any)["withscores"] = true
	}
	return command, true, nil
}
