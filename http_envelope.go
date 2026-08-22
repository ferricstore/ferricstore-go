package ferricstore

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
)

const (
	httpBinaryEncoding = "ferricstore-json-v1"
	httpBytesTag       = "$ferricstore_bytes"
	httpMapTag         = "$ferricstore_map"
)

var httpConnectionAffineCommands = map[string]struct{}{
	"AUTH": {}, "BLMOVE": {}, "BLMPOP": {}, "BLPOP": {}, "BRPOP": {}, "CLIENT": {},
	"DISCARD": {}, "EXEC": {}, "HELLO": {}, "MULTI": {}, "PSUBSCRIBE": {},
	"PUNSUBSCRIBE": {}, "QUIT": {}, "SELECT": {}, "SUBSCRIBE": {}, "UNSUBSCRIBE": {},
	"UNWATCH": {}, "WATCH": {}, "XREAD": {}, "XREADGROUP": {},
}

// HTTPCommandDisposition reports whether a command can run through the
// stateless HTTP gateway or requires a persistent native TCP session.
func HTTPCommandDisposition(name string) string {
	if _, nativeOnly := httpConnectionAffineCommands[strings.ToUpper(name)]; nativeOnly {
		return "native_only"
	}
	return "supported"
}

var httpStructuredFlowCommands = map[string]struct{}{
	"FLOW.VALUE.PUT": {}, "FLOW.VALUE.MGET": {}, "FLOW.STEP_CONTINUE": {}, "FLOW.START_AND_CLAIM": {},
	"FLOW.RUN_STEPS_MANY": {}, "FLOW.SCHEDULE.CREATE": {}, "FLOW.SCHEDULE.GET": {},
	"FLOW.SCHEDULE.DELETE": {}, "FLOW.SCHEDULE.FIRE_DUE": {}, "FLOW.SCHEDULE.LIST": {},
	"FLOW.SCHEDULE.FIRE": {}, "FLOW.SCHEDULE.PAUSE": {}, "FLOW.SCHEDULE.RESUME": {},
	"FLOW.EFFECT.RESERVE": {}, "FLOW.EFFECT.CONFIRM": {}, "FLOW.EFFECT.FAIL": {},
	"FLOW.EFFECT.COMPENSATE": {}, "FLOW.EFFECT.GET": {}, "FLOW.GOVERNANCE.LEDGER": {},
	"FLOW.GOVERNANCE.OVERVIEW": {}, "FLOW.APPROVAL.REQUEST": {},
	"FLOW.APPROVAL.APPROVE": {}, "FLOW.APPROVAL.REJECT": {}, "FLOW.APPROVAL.GET": {},
	"FLOW.APPROVAL.LIST": {}, "FLOW.CIRCUIT.OPEN": {}, "FLOW.CIRCUIT.CLOSE": {},
	"FLOW.CIRCUIT.GET": {}, "FLOW.BUDGET.RESERVE": {}, "FLOW.BUDGET.COMMIT": {},
	"FLOW.BUDGET.RELEASE": {}, "FLOW.BUDGET.GET": {}, "FLOW.BUDGET.LIST": {},
	"FLOW.LIMIT.LEASE": {}, "FLOW.LIMIT.SPEND": {}, "FLOW.LIMIT.RELEASE": {},
	"FLOW.LIMIT.GET": {}, "FLOW.LIMIT.LIST": {},
}

func encodeHTTPCommand(args []any) (any, error) {
	if err := validateCommandArgs(args); err != nil {
		return nil, err
	}
	name := strings.ToUpper(commandPart(args[0]))
	if HTTPCommandDisposition(name) == "native_only" {
		return nil, fmt.Errorf("%w: %s", ErrHTTPConnectionAffineCommand, name)
	}
	if _, structured := httpStructuredFlowCommands[name]; structured {
		command, err := buildNativeCommand(args)
		if err != nil {
			return nil, err
		}
		if provider, ok := command.payload.(nativePipelineBodyProvider); ok {
			command.payload, err = provider.nativePipelineBody()
			if err != nil {
				return nil, err
			}
		}
		payload, err := encodeHTTPValue(command.payload)
		if err != nil {
			return nil, fmt.Errorf("%s HTTP payload: %w", name, err)
		}
		return map[string]any{"command": name, "opcode": command.opcode, "payload": payload}, nil
	}
	encoded := make([]any, len(args))
	encoded[0] = name
	for index := 1; index < len(args); index++ {
		value, err := encodeHTTPValue(args[index])
		if err != nil {
			return nil, fmt.Errorf("%s argument %d: %w", name, index, err)
		}
		encoded[index] = value
	}
	return encoded, nil
}

func encodeHTTPValue(value any) (any, error) {
	if wrapped, ok := value.(nativeJSONCommandArg); ok {
		value = wrapped.value
	}
	return encodeHTTPReflect(reflect.ValueOf(value), 0)
}

func encodeHTTPReflect(value reflect.Value, depth int) (any, error) {
	if depth > nativeMaxEncodeDepth {
		return nil, errors.New("HTTP command value exceeds maximum nesting depth")
	}
	if !value.IsValid() {
		return nil, nil
	}
	if value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil, nil
		}
		return encodeHTTPReflect(value.Elem(), depth)
	}
	if value.Type() == reflect.TypeFor[[]byte]() {
		return map[string]any{httpBytesTag: base64.StdEncoding.EncodeToString(value.Bytes())}, nil
	}
	switch value.Kind() {
	case reflect.Bool:
		return value.Bool(), nil
	case reflect.String:
		return value.String(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return value.Uint(), nil
	case reflect.Float32, reflect.Float64:
		number := value.Float()
		if math.IsNaN(number) || math.IsInf(number, 0) {
			return nil, errors.New("HTTP command floats must be finite")
		}
		return number, nil
	case reflect.Slice, reflect.Array:
		items := make([]any, value.Len())
		for index := range value.Len() {
			item, err := encodeHTTPReflect(value.Index(index), depth+1)
			if err != nil {
				return nil, err
			}
			items[index] = item
		}
		return items, nil
	case reflect.Map:
		pairs := make([]any, 0, value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			key, err := encodeHTTPReflect(iterator.Key(), depth+1)
			if err != nil {
				return nil, err
			}
			item, err := encodeHTTPReflect(iterator.Value(), depth+1)
			if err != nil {
				return nil, err
			}
			pairs = append(pairs, []any{key, item})
		}
		return map[string]any{httpMapTag: pairs}, nil
	default:
		return nil, fmt.Errorf("HTTP command value type %s is unsupported", value.Type())
	}
}

func decodeHTTPValue(value any) (any, error) {
	return decodeHTTPValueDepth(value, 0)
}

func decodeHTTPValueDepth(value any, depth int) (any, error) {
	if depth > nativeMaxDecodeDepth {
		return nil, errors.New("HTTP response value exceeds maximum nesting depth")
	}
	switch typed := value.(type) {
	case nil, bool:
		return typed, nil
	case string:
		return []byte(typed), nil
	case json.Number:
		return decodeHTTPNumber(typed)
	case float64:
		return typed, nil
	case []any:
		result := make([]any, len(typed))
		for index, item := range typed {
			decoded, err := decodeHTTPValueDepth(item, depth+1)
			if err != nil {
				return nil, err
			}
			result[index] = decoded
		}
		return result, nil
	case map[string]any:
		if len(typed) == 1 {
			if encoded, exists := typed[httpBytesTag]; exists {
				text, ok := encoded.(string)
				if !ok {
					return nil, errors.New("HTTP binary marker payload must be text")
				}
				decoded, err := base64.StdEncoding.Strict().DecodeString(text)
				if err != nil {
					return nil, errors.New("HTTP binary marker contains invalid base64")
				}
				return decoded, nil
			}
			if rawPairs, exists := typed[httpMapTag]; exists {
				return decodeHTTPMapPairs(rawPairs, depth+1)
			}
		}
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			decoded, err := decodeHTTPValueDepth(item, depth+1)
			if err != nil {
				return nil, err
			}
			result[key] = decoded
		}
		return result, nil
	default:
		return nil, fmt.Errorf("HTTP response value type %T is unsupported", value)
	}
}

func decodeHTTPMapPairs(value any, depth int) (any, error) {
	pairs, ok := value.([]any)
	if !ok {
		return nil, errors.New("HTTP map marker payload must be an array")
	}
	result := make(map[any]any, len(pairs))
	stringResult := make(map[string]any, len(pairs))
	allStringKeys := true
	for _, rawPair := range pairs {
		pair, ok := rawPair.([]any)
		if !ok || len(pair) != 2 {
			return nil, errors.New("HTTP map marker entry must be a key/value pair")
		}
		key, err := decodeHTTPValueDepth(pair[0], depth+1)
		if err != nil {
			return nil, err
		}
		if bytes, ok := key.([]byte); ok {
			key = string(bytes)
		}
		if key == nil || !reflect.TypeOf(key).Comparable() {
			return nil, errors.New("HTTP map marker key is not comparable in Go")
		}
		item, err := decodeHTTPValueDepth(pair[1], depth+1)
		if err != nil {
			return nil, err
		}
		if _, duplicate := result[key]; duplicate {
			return nil, errors.New("HTTP map marker contains a duplicate key")
		}
		result[key] = item
		if text, ok := key.(string); ok {
			stringResult[text] = item
		} else {
			allStringKeys = false
		}
	}
	if allStringKeys {
		return stringResult, nil
	}
	return result, nil
}

func decodeHTTPNumber(number json.Number) (any, error) {
	text := number.String()
	if !strings.ContainsAny(text, ".eE") {
		if signed, err := strconv.ParseInt(text, 10, 64); err == nil {
			return signed, nil
		}
		if unsigned, err := strconv.ParseUint(text, 10, 64); err == nil {
			return unsigned, nil
		}
	}
	value, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsInf(value, 0) || math.IsNaN(value) {
		return nil, errors.New("HTTP response contains an invalid number")
	}
	return value, nil
}
