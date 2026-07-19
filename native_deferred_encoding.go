package ferricstore

import (
	"fmt"
	"sync"
)

type nativeDeferredCodecValue struct {
	codec             Codec
	value             any
	maxEncodedBytes   int
	encodedValueLabel string
	command           string
}

type deferredCodecExecutor interface {
	supportsDeferredCodec(Codec) bool
}

func executorSupportsDeferredCodec(exec Executor, codec Codec) bool {
	switch codec.(type) {
	case RawCodec, *RawCodec:
		// RawCodec already returns the caller-owned value without materializing
		// another representation, so deferring it only adds wrapper overhead.
		return false
	}
	capability, ok := exec.(deferredCodecExecutor)
	return ok && capability.supportsDeferredCodec(codec)
}

func (*NativeExecutor) supportsDeferredCodec(Codec) bool         { return true }
func (*TopologyNativeExecutor) supportsDeferredCodec(Codec) bool { return true }
func (*AutoBatchExecutor) supportsDeferredCodec(Codec) bool      { return true }

func (*NativeExecutor) supportsNativeRequestContextArguments()         {}
func (*TopologyNativeExecutor) supportsNativeRequestContextArguments() {}
func (*AutoBatchExecutor) supportsNativeRequestContextArguments()      {}
func (*BufferedExecutor) supportsNativeRequestContextArguments()       {}

func encodeNativeDeferredCodecValue(value nativeDeferredCodecValue) (any, error) {
	encoded, err := value.codec.Encode(value.value)
	if err != nil {
		return nil, err
	}
	if _, nested := encoded.(nativeDeferredCodecValue); nested {
		return nil, fmt.Errorf("ferricstore codec returned an internal deferred value")
	}
	if value.maxEncodedBytes > 0 {
		if err := validateEncodedByteLimit(value.command, value.encodedValueLabel, encoded, value.maxEncodedBytes); err != nil {
			return nil, err
		}
	}
	return encoded, nil
}

func materializeDeferredCodecValues(values []any, serial *sync.Mutex) ([]any, error) {
	needsSerial := deferredValuesNeedSerialEncoding(values)
	if needsSerial && serial != nil {
		serial.Lock()
		defer serial.Unlock()
	}
	materialized := values
	copied := false
	for index, value := range values {
		deferred, ok := value.(nativeDeferredCodecValue)
		if !ok {
			continue
		}
		encoded, err := encodeNativeDeferredCodecValue(deferred)
		if err != nil {
			return nil, err
		}
		if !copied {
			materialized = append([]any(nil), values...)
			copied = true
		}
		materialized[index] = encoded
	}
	return materialized, nil
}

func materializeDeferredCodecValuesForExecutor(
	values []any,
	exec Executor,
	serial *sync.Mutex,
) ([]any, error) {
	for _, value := range values {
		deferred, ok := value.(nativeDeferredCodecValue)
		if ok && !executorSupportsDeferredCodec(exec, deferred.codec) {
			return materializeDeferredCodecValues(values, serial)
		}
	}
	return values, nil
}

// snapshotDeferredCodecInputs transfers ownership of mutable inputs while
// retaining deferred wrappers so routing can reject an invalid command before
// invoking its codec.
func snapshotDeferredCodecInputs(values []any) ([]any, error) {
	if len(values) == 0 {
		return values, nil
	}
	out := make([]any, len(values))
	var visiting map[commandCloneVisit]struct{}
	for index, value := range values {
		deferred, isDeferred := value.(nativeDeferredCodecValue)
		if isDeferred {
			value = deferred.value
		}
		cloned, err := snapshotCommandArg(value, &visiting)
		if err != nil {
			return nil, fmt.Errorf("snapshot deferred value %d: %w", index, err)
		}
		if isDeferred {
			deferred.value = cloned
			out[index] = deferred
			continue
		}
		out[index] = cloned
	}
	return out, nil
}

func snapshotDeferredCodecCommands(
	commands [][]any,
	serial *sync.Mutex,
) (owned [][]any, accepted int, err error) {
	totalValues := 0
	for _, command := range commands {
		totalValues += len(command)
	}
	owned = make([][]any, len(commands))
	values := make([]any, totalValues)
	valueIndex := 0
	var visiting map[commandCloneVisit]struct{}
	for commandIndex, command := range commands {
		owned[commandIndex] = values[valueIndex : valueIndex+len(command)]
		needsSerial := deferredValuesNeedSerialEncoding(command)
		if needsSerial && serial != nil {
			serial.Lock()
		}
		for argumentIndex, value := range command {
			if deferred, ok := value.(nativeDeferredCodecValue); ok {
				value, err = encodeNativeDeferredCodecValue(deferred)
				if err != nil {
					err = fmt.Errorf("resolve command argument %d: %w", argumentIndex, err)
					break
				}
			}
			value, err = snapshotCommandArg(value, &visiting)
			if err != nil {
				err = fmt.Errorf("snapshot command argument %d: %w", argumentIndex, err)
				break
			}
			owned[commandIndex][argumentIndex] = value
		}
		if needsSerial && serial != nil {
			serial.Unlock()
		}
		if err != nil {
			return owned, commandIndex, err
		}
		valueIndex += len(command)
	}
	return owned, len(commands), nil
}

func deferredValuesNeedSerialEncoding(values []any) bool {
	for _, value := range values {
		deferred, ok := value.(nativeDeferredCodecValue)
		if ok && codecNeedsSerialEncoding(deferred.codec) {
			return true
		}
	}
	return false
}

func codecNeedsSerialEncoding(codec Codec) bool {
	switch codec.(type) {
	case JSONCodec, *JSONCodec, StringCodec, *StringCodec, RawCodec, *RawCodec, *serializedCodec, *concurrentCodec:
		return false
	default:
		return true
	}
}
