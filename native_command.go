package ferricstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type nativeCommand struct {
	name    string
	opcode  uint16
	laneID  uint32
	payload any
	flags   byte
	budget  nativeRequestBudget
}

type nativeJSONCommandArg struct{ value any }

// nativeRequestContextMarker is deliberately a distinct dynamic type. Raw
// command arguments named REQUEST_CONTEXT must remain ordinary command data;
// only SDK helpers can attach transport metadata with this marker.
type nativeRequestContextMarker string

const requestContextMarker nativeRequestContextMarker = "REQUEST_CONTEXT"

type nativeRequestContextArgumentExecutor interface {
	supportsNativeRequestContextArguments()
}

func appendNativeRequestContext(args []any, requestContext any) []any {
	return append(args, requestContextMarker, requestContext)
}

func splitNativeRequestContext(args []any) (payload []any, requestContext any, ok bool) {
	if len(args) < 2 {
		return args, nil, false
	}
	if marker, typed := args[len(args)-2].(nativeRequestContextMarker); !typed || marker != requestContextMarker {
		return args, nil, false
	}
	return args[:len(args)-2], args[len(args)-1], true
}

func commandArgsForExecutor(exec Executor, args []any) []any {
	_, _, hasRequestContext := splitNativeRequestContext(args)
	if !hasRequestContext {
		return args
	}
	if _, supported := exec.(nativeRequestContextArgumentExecutor); supported {
		return args
	}
	out := append([]any(nil), args...)
	out[len(out)-2] = string(requestContextMarker)
	return out
}

func commandBatchArgsForExecutor(exec Executor, commands [][]any) [][]any {
	if _, supported := exec.(nativeRequestContextArgumentExecutor); supported {
		return commands
	}
	out := commands
	copied := false
	for index, command := range commands {
		_, _, hasRequestContext := splitNativeRequestContext(command)
		if !hasRequestContext {
			continue
		}
		if !copied {
			out = append([][]any(nil), commands...)
			copied = true
		}
		out[index] = commandArgsForExecutor(exec, command)
	}
	return out
}

func (a nativeJSONCommandArg) encode() ([]byte, error) {
	encoded, err := json.Marshal(a.value)
	if err != nil {
		return nil, fmt.Errorf("encode native command argument %T: %w", a.value, err)
	}
	return encoded, nil
}

// These payloads keep bulk keys typed until wire encoding. Converting a large
// []string to []any allocates once per key, while the wire representation is
// still the same {"keys": [...]} or {"pairs": [[key, value], ...]} map.
type nativeMGetPayload struct {
	stringKeys []string
	args       []any
}

type nativeMSetPayload struct {
	keys   []string
	values []any
	args   []any
}

type nativeKeyValueCommandPayload struct {
	command string
	keys    []string
	values  []any
}

type nativeKeyCommandPayload struct {
	command string
	keys    []string
}

func newNativeMGetCommand(keys []string) nativeCommand {
	return nativeCommand{
		name:    "MGET",
		opcode:  nativeOpMGet,
		laneID:  1,
		payload: nativeMGetPayload{stringKeys: keys},
	}
}

func newNativeMSetCommand(keys []string, values []any) (nativeCommand, error) {
	if len(keys) != len(values) {
		return nativeCommand{}, fmt.Errorf("MSET received %d keys and %d values", len(keys), len(values))
	}
	return nativeCommand{
		name:    "MSET",
		opcode:  nativeOpMSet,
		laneID:  1,
		payload: nativeMSetPayload{keys: keys, values: values},
	}, nil
}

func newNativeMSetNXCommand(keys []string, values []any) (nativeCommand, error) {
	if len(keys) != len(values) {
		return nativeCommand{}, fmt.Errorf("MSETNX received %d keys and %d values", len(keys), len(values))
	}
	return nativeCommand{
		name:   "MSETNX",
		opcode: nativeOpCommandExec,
		laneID: 1,
		payload: nativeKeyValueCommandPayload{
			command: "MSETNX",
			keys:    keys,
			values:  values,
		},
	}, nil
}

func newNativeDelCommand(keys []string) nativeCommand {
	return nativeCommand{
		name:    "DEL",
		opcode:  nativeOpDel,
		laneID:  1,
		payload: nativeMGetPayload{stringKeys: keys},
	}
}

func newNativeExistsCommand(keys []string) nativeCommand {
	return nativeCommand{
		name:   "EXISTS",
		opcode: nativeOpCommandExec,
		laneID: 1,
		payload: nativeKeyCommandPayload{
			command: "EXISTS",
			keys:    keys,
		},
	}
}

func buildNativeCommand(args []any) (nativeCommand, error) {
	if err := validateCommandArgs(args); err != nil {
		return nativeCommand{}, err
	}
	commandName, _ := validatedCommandName(args[0])
	command := strings.ToUpper(commandName)
	if built, ok, err := buildBasicNativeCommand(command, args[1:]); ok || err != nil {
		return built, err
	}
	if built, ok, err := buildFlowNativeCommand(command, args[1:]); ok || err != nil {
		return built, err
	}
	if command == "COMMAND_EXEC" {
		if len(args) < 2 {
			return nativeCommand{}, errors.New("COMMAND_EXEC requires command name")
		}
		nested, err := validatedCommandName(args[1])
		if err != nil {
			return nativeCommand{}, fmt.Errorf("COMMAND_EXEC: %w", err)
		}
		return commandExecNativeCommand(strings.ToUpper(nested), args[2:])
	}
	return commandExecNativeCommand(command, args[1:])
}

type nativeRequestBudget struct {
	extension      time.Duration
	disableDefault bool
}

func blockingCommandBudget(args []any) nativeRequestBudget {
	if len(args) == 0 {
		return nativeRequestBudget{}
	}
	name := strings.ToUpper(asString(args[0]))
	values := args[1:]
	var value any
	var unit time.Duration
	switch name {
	case "BLPOP", "BRPOP", "BLMOVE", "BRPOPLPUSH", "BZPOPMIN", "BZPOPMAX":
		if len(values) > 0 {
			value, unit = values[len(values)-1], time.Second
		}
	case "BLMPOP", "BZMPOP":
		if len(values) > 0 {
			value, unit = values[0], time.Second
		}
	case "XREAD", "XREADGROUP":
		if timeout, ok := streamBlockingTimeout(name, values); ok {
			value, unit = timeout, time.Millisecond
		}
	case "WAIT", "WAITAOF":
		if len(values) > 0 {
			value, unit = values[len(values)-1], time.Millisecond
		}
	case "FLOW.CLAIM_DUE":
		for i := 0; i+1 < len(values); i++ {
			if strings.EqualFold(asString(values[i]), "BLOCK") {
				value, unit = values[i+1], time.Millisecond
				break
			}
		}
	}
	if unit == 0 || value == nil {
		return nativeRequestBudget{}
	}
	n, err := strconv.ParseFloat(asString(value), 64)
	if err != nil || math.IsNaN(n) || math.IsInf(n, 0) || n < 0 {
		return nativeRequestBudget{}
	}
	if n == 0 {
		if name == "FLOW.CLAIM_DUE" {
			return nativeRequestBudget{}
		}
		return nativeRequestBudget{disableDefault: true}
	}
	seconds := n * float64(unit)
	if seconds >= float64(time.Duration(1<<63-1)) {
		return nativeRequestBudget{disableDefault: true}
	}
	return nativeRequestBudget{extension: time.Duration(seconds)}
}

func streamBlockingTimeout(name string, values []any) (any, bool) {
	index := 0
	if name == "XREADGROUP" {
		if len(values) < 3 || !strings.EqualFold(asString(values[0]), "GROUP") {
			return nil, false
		}
		index = 3
	}
	for index < len(values) {
		switch strings.ToUpper(asString(values[index])) {
		case "STREAMS":
			return nil, false
		case "COUNT":
			if index+1 >= len(values) {
				return nil, false
			}
			index += 2
		case "BLOCK":
			if index+1 >= len(values) {
				return nil, false
			}
			return values[index+1], true
		case "NOACK":
			if name != "XREADGROUP" {
				return nil, false
			}
			index++
		default:
			return nil, false
		}
	}
	return nil, false
}

func commandExecNativeCommand(command string, args []any) (nativeCommand, error) {
	payloadArgs, requestContextValue, hasRequestContext := splitNativeRequestContext(args)
	var requestContext map[string]any
	var err error
	if hasRequestContext {
		requestContext, err = normalizeRequestContext(requestContextValue)
		if err != nil {
			return nativeCommand{}, err
		}
	}
	encodedArgs, err := nativeCommandArgs(payloadArgs)
	if err != nil {
		return nativeCommand{}, err
	}
	payload := map[string]any{"command": command, "args": encodedArgs}
	if hasRequestContext {
		payload["request_context"] = requestContext
	}
	return nativeCommand{
		name:    command,
		opcode:  nativeOpCommandExec,
		laneID:  1,
		payload: payload,
	}, nil
}

func nativeCommandArgs(args []any) ([]any, error) {
	out := make([]any, 0, len(args))
	for _, arg := range args {
		encoded, err := nativeCommandArg(arg)
		if err != nil {
			return nil, err
		}
		out = append(out, encoded)
	}
	return out, nil
}

func nativeCommandArg(arg any) (any, error) {
	switch arg.(type) {
	case nil, string, []byte, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, bool, float32, float64, nativeDeferredCodecValue:
		return arg, nil
	case nativeRequestContextMarker:
		return nil, errors.New("request context marker must be the final command metadata pair")
	}
	value := reflect.ValueOf(arg)
	if value.IsValid() {
		switch value.Kind() {
		case reflect.String:
			return value.String(), nil
		case reflect.Slice:
			if value.Type().Elem().Kind() == reflect.Uint8 {
				return value.Bytes(), nil
			}
		}
	}
	return nativeJSONCommandArg{value: arg}, nil
}

func buildBasicNativeCommand(name string, args []any) (nativeCommand, bool, error) {
	switch name {
	case "SHARDS":
		if len(args) != 0 {
			return nativeCommand{}, true, errors.New("SHARDS does not accept arguments")
		}
		return nativeCommand{name: name, opcode: nativeOpShards, laneID: 0, payload: map[string]any{}}, true, nil
	case "PING":
		if len(args) > 1 {
			return nativeCommand{}, false, nil
		}
		payload := map[string]any{}
		if len(args) > 0 {
			payload["message"] = args[0]
		}
		return nativeCommand{name: name, opcode: nativeOpPing, laneID: 0, payload: payload}, true, nil
	case "WINDOW_UPDATE":
		payload, err := nativeOptionMap(name, args)
		if err != nil {
			return nativeCommand{}, true, err
		}
		return nativeCommand{name: name, opcode: nativeOpWindowUpdate, laneID: 0, payload: payload}, true, nil
	case "GET":
		if len(args) < 1 {
			return nativeCommand{}, true, errors.New("GET requires key")
		}
		if len(args) > 1 {
			return nativeCommand{}, false, nil
		}
		return nativeCommand{name: name, opcode: nativeOpGet, laneID: 1, payload: map[string]any{"key": args[0]}}, true, nil
	case "SET":
		if len(args) < 2 {
			return nativeCommand{}, true, errors.New("SET requires key and value")
		}
		if len(args) > 2 {
			return nativeCommand{}, false, nil
		}
		return nativeCommand{name: name, opcode: nativeOpSet, laneID: 1, payload: map[string]any{"key": args[0], "value": args[1]}}, true, nil
	case "DEL":
		if len(args) == 0 {
			return nativeCommand{}, true, errors.New("DEL requires at least one key")
		}
		return nativeCommand{name: name, opcode: nativeOpDel, laneID: 1, payload: map[string]any{"keys": append([]any(nil), args...)}}, true, nil
	case "MGET":
		if len(args) == 0 {
			return nativeCommand{}, true, errors.New("MGET requires at least one key")
		}
		return nativeCommand{name: name, opcode: nativeOpMGet, laneID: 1, payload: nativeMGetPayload{args: args}}, true, nil
	case "MSET":
		if len(args) == 0 || len(args)%2 != 0 {
			return nativeCommand{}, true, errors.New("MSET requires key/value pairs")
		}
		return nativeCommand{name: name, opcode: nativeOpMSet, laneID: 1, payload: nativeMSetPayload{args: args}}, true, nil
	default:
		return nativeCommand{}, false, nil
	}
}

func nativeOptionMap(command string, args []any) (map[string]any, error) {
	if len(args)%2 != 0 {
		return nil, fmt.Errorf("%s option %q requires a value", command, asString(args[len(args)-1]))
	}
	payload := make(map[string]any, len(args)/2)
	for index := 0; index < len(args); index += 2 {
		name := strings.ToLower(strings.TrimSpace(asString(args[index])))
		if name == "" {
			return nil, fmt.Errorf("%s option name is empty", command)
		}
		if _, exists := payload[name]; exists {
			return nil, fmt.Errorf("%s option %q is duplicated", command, name)
		}
		payload[name] = args[index+1]
	}
	return payload, nil
}
