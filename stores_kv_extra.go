package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
)

const maxSetRangeOffset int64 = 536_870_911

type SetOptions struct {
	EXSeconds      *int64
	PXMilliseconds *int64
	EXATSeconds    *int64
	PXATMillis     *int64
	NX             bool
	XX             bool
	Get            bool
	KeepTTL        bool
}

func (s *KeyValueStore) SetWithOptions(ctx context.Context, key string, value any, opt SetOptions) (any, error) {
	if err := validateSetOptions(opt); err != nil {
		return nil, err
	}
	if err := validatePublicV080StringKey(key); err != nil {
		return nil, err
	}
	encoded, err := s.client.encode(value)
	if err != nil {
		return nil, err
	}
	args := []any{"SET", key, encoded}
	appendInt64Ptr(&args, "EX", opt.EXSeconds)
	appendInt64Ptr(&args, "PX", opt.PXMilliseconds)
	appendInt64Ptr(&args, "EXAT", opt.EXATSeconds)
	appendInt64Ptr(&args, "PXAT", opt.PXATMillis)
	if opt.NX {
		args = append(args, "NX")
	}
	if opt.XX {
		args = append(args, "XX")
	}
	if opt.Get {
		args = append(args, "GET")
	}
	if opt.KeepTTL {
		args = append(args, "KEEPTTL")
	}
	response, err := s.commandReply(ctx, args...)
	if err != nil {
		return response, err
	}
	if !opt.Get {
		// The dedicated native v1 SET schema returns a boolean for conditional
		// writes, while COMMAND_EXEC-compatible transports return OK/nil.
		// Normalize the native false result to the existing nil contract and
		// preserve true as the successful native acknowledgement.
		if applied, ok := response.(bool); ok && (opt.NX || opt.XX) {
			if !applied {
				return nil, nil
			}
			return response, nil
		}
		if response == nil && (opt.NX || opt.XX) {
			return nil, nil
		}
		_, err := responseOK(response, nil)
		return response, err
	}
	if response == nil {
		return nil, nil
	}
	return s.client.codec.Decode(response)
}

func validateSetOptions(opt SetOptions) error {
	if opt.NX && opt.XX {
		return errors.New("SET NX and XX options are mutually exclusive")
	}
	expiryModes := countPresentInt64(opt.EXSeconds, opt.PXMilliseconds, opt.EXATSeconds, opt.PXATMillis)
	if expiryModes > 1 {
		return errors.New("SET accepts only one expiration option")
	}
	if opt.KeepTTL && expiryModes != 0 {
		return errors.New("SET KEEPTTL and expiration options are mutually exclusive")
	}
	if err := validatePositiveExpiryOptions("SET", opt.EXSeconds, opt.PXMilliseconds, opt.EXATSeconds, opt.PXATMillis); err != nil {
		return err
	}
	return validateExpiryOptionBounds("SET", opt.EXSeconds, opt.PXMilliseconds, opt.EXATSeconds, opt.PXATMillis)
}

func (s *KeyValueStore) MSet(ctx context.Context, values map[string]any) error {
	if len(values) == 0 {
		return nil
	}
	keys := mapKeysForCodec(values, s.client.codec)
	if err := validatePublicV080StringKeys(keys); err != nil {
		return err
	}
	if err := validateSameSlotStringKeys("MSET", keys); err != nil {
		return err
	}
	if bulk, ok := s.client.exec.(keyValueBulkExecutor); ok {
		encodedValues := make([]any, 0, len(values))
		for _, key := range keys {
			encoded, err := s.client.encode(values[key])
			if err != nil {
				return err
			}
			encodedValues = append(encodedValues, encoded)
		}
		return s.commandStatusDirect(ctx, func() (any, error) {
			return bulk.keyValueMSet(ctx, keys, encodedValues)
		}, func() []any {
			args := make([]any, 1, 1+2*len(keys))
			args[0] = "MSET"
			for index, key := range keys {
				args = append(args, key, encodedValues[index])
			}
			return args
		})
	}
	sort.Strings(keys)
	args := make([]any, 1, 1+2*len(values))
	args[0] = "MSET"
	for _, key := range keys {
		encoded, err := s.client.encode(values[key])
		if err != nil {
			return err
		}
		args = append(args, key, encoded)
	}
	return s.commandStatus(ctx, args...)
}

func (s *KeyValueStore) MSetNX(ctx context.Context, values map[string]any) (bool, error) {
	if len(values) == 0 {
		return false, errors.New("MSETNX requires at least one key/value pair")
	}
	keys := mapKeysForCodec(values, s.client.codec)
	if err := validatePublicV080StringKeys(keys); err != nil {
		return false, err
	}
	if err := validateSameSlotStringKeys("MSETNX", keys); err != nil {
		return false, err
	}
	if bulk, ok := s.client.exec.(keyValueMSetNXExecutor); ok {
		encodedValues := make([]any, 0, len(values))
		for _, key := range keys {
			encoded, err := s.client.encode(values[key])
			if err != nil {
				return false, err
			}
			encodedValues = append(encodedValues, encoded)
		}
		response, err := s.commandReplyDirect(ctx, func() (any, error) {
			return bulk.keyValueMSetNX(ctx, keys, encodedValues)
		}, func() []any {
			args := make([]any, 1, 1+2*len(keys))
			args[0] = "MSETNX"
			for index, key := range keys {
				args = append(args, key, encodedValues[index])
			}
			return args
		})
		return responseBool(response, err)
	}
	args := make([]any, 1, 1+2*len(values))
	args[0] = "MSETNX"
	for _, key := range keys {
		encoded, err := s.client.encode(values[key])
		if err != nil {
			return false, err
		}
		args = append(args, key, encoded)
	}
	response, err := s.commandReply(ctx, args...)
	return responseBool(response, err)
}

func (s *KeyValueStore) IncrBy(ctx context.Context, key string, increment int64) (int64, error) {
	value, err := s.commandReply(ctx, "INCRBY", key, increment)
	return responseInt64(value, err)
}

func (s *KeyValueStore) DecrBy(ctx context.Context, key string, decrement int64) (int64, error) {
	value, err := s.commandReply(ctx, "DECRBY", key, decrement)
	return responseInt64(value, err)
}

func (s *KeyValueStore) IncrByFloat(ctx context.Context, key string, increment float64) (float64, error) {
	if math.IsNaN(increment) || math.IsInf(increment, 0) {
		return 0, errors.New("INCRBYFLOAT increment must be finite")
	}
	value, err := s.commandReply(ctx, "INCRBYFLOAT", key, increment)
	return responseFloat64(value, err)
}

func (s *KeyValueStore) Append(ctx context.Context, key string, value any) (int64, error) {
	if err := validateRawMutationValue("APPEND", value); err != nil {
		return 0, err
	}
	response, err := s.commandReply(ctx, "APPEND", key, value)
	return keyValueLengthResponse("APPEND", response, err)
}

func (s *KeyValueStore) StrLen(ctx context.Context, key string) (int64, error) {
	value, err := s.commandReply(ctx, "STRLEN", key)
	return keyValueLengthResponse("STRLEN", value, err)
}

func (s *KeyValueStore) GetSet(ctx context.Context, key string, value any) (any, error) {
	if err := validatePublicStringKey(key); err != nil {
		return nil, err
	}
	encoded, err := s.client.encode(value)
	if err != nil {
		return nil, err
	}
	response, err := s.commandReply(ctx, "GETSET", key, encoded)
	if err != nil || response == nil {
		return response, err
	}
	return s.client.codec.Decode(response)
}

func (s *KeyValueStore) GetDel(ctx context.Context, key string) (any, error) {
	response, err := s.commandReply(ctx, "GETDEL", key)
	if err != nil || response == nil {
		return response, err
	}
	return s.client.codec.Decode(response)
}

type GetEXOptions struct {
	EXSeconds      *int64
	PXMilliseconds *int64
	EXATSeconds    *int64
	PXATMillis     *int64
	Persist        bool
}

func (s *KeyValueStore) GetEX(ctx context.Context, key string, opt GetEXOptions) (any, error) {
	if countPresentInt64(opt.EXSeconds, opt.PXMilliseconds, opt.EXATSeconds, opt.PXATMillis)+boolInt(opt.Persist) > 1 {
		return nil, errors.New("GETEX accepts only one expiration or PERSIST option")
	}
	if err := validatePositiveExpiryOptions("GETEX", opt.EXSeconds, opt.PXMilliseconds, opt.EXATSeconds, opt.PXATMillis); err != nil {
		return nil, err
	}
	if err := validateExpiryOptionBounds("GETEX", opt.EXSeconds, opt.PXMilliseconds, opt.EXATSeconds, opt.PXATMillis); err != nil {
		return nil, err
	}
	args := []any{"GETEX", key}
	appendInt64Ptr(&args, "EX", opt.EXSeconds)
	appendInt64Ptr(&args, "PX", opt.PXMilliseconds)
	appendInt64Ptr(&args, "EXAT", opt.EXATSeconds)
	appendInt64Ptr(&args, "PXAT", opt.PXATMillis)
	if opt.Persist {
		args = append(args, "PERSIST")
	}
	response, err := s.commandReply(ctx, args...)
	if err != nil || response == nil {
		return response, err
	}
	return s.client.codec.Decode(response)
}

func (s *KeyValueStore) SetNX(ctx context.Context, key string, value any) (bool, error) {
	if err := validatePublicStringKey(key); err != nil {
		return false, err
	}
	encoded, err := s.client.encode(value)
	if err != nil {
		return false, err
	}
	response, err := s.commandReply(ctx, "SETNX", key, encoded)
	return responseBool(response, err)
}

func (s *KeyValueStore) SetEX(ctx context.Context, key string, seconds int64, value any) error {
	if seconds <= 0 {
		return errors.New("SETEX expiration must be positive")
	}
	if err := validateRelativeExpiryValue("SETEX", seconds, maxRelativeExpirySecsV080); err != nil {
		return err
	}
	if err := validatePublicStringKey(key); err != nil {
		return err
	}
	encoded, err := s.client.encode(value)
	if err != nil {
		return err
	}
	return s.commandStatus(ctx, "SETEX", key, seconds, encoded)
}

func (s *KeyValueStore) PSetEX(ctx context.Context, key string, milliseconds int64, value any) error {
	if milliseconds <= 0 {
		return errors.New("PSETEX expiration must be positive")
	}
	if err := validateRelativeExpiryValue("PSETEX", milliseconds, maxRelativeExpiryMillisV080); err != nil {
		return err
	}
	if err := validatePublicStringKey(key); err != nil {
		return err
	}
	encoded, err := s.client.encode(value)
	if err != nil {
		return err
	}
	return s.commandStatus(ctx, "PSETEX", key, milliseconds, encoded)
}

func (s *KeyValueStore) GetRange(ctx context.Context, key string, start, end int64) (any, error) {
	return s.commandReply(ctx, "GETRANGE", key, start, end)
}

func (s *KeyValueStore) SetRange(ctx context.Context, key string, offset int64, value any) (int64, error) {
	if offset < 0 || offset > maxSetRangeOffset {
		return 0, fmt.Errorf("SETRANGE offset must be between 0 and %d", maxSetRangeOffset)
	}
	if err := validateRawMutationValue("SETRANGE", value); err != nil {
		return 0, err
	}
	response, err := s.commandReply(ctx, "SETRANGE", key, offset, value)
	return keyValueLengthResponse("SETRANGE", response, err)
}

func countPresentInt64(values ...*int64) int {
	count := 0
	for _, value := range values {
		if value != nil {
			count++
		}
	}
	return count
}

func validatePositiveExpiryOptions(command string, values ...*int64) error {
	for _, value := range values {
		if value != nil && *value <= 0 {
			return fmt.Errorf("%s expiration must be positive", command)
		}
	}
	return nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func validateRawMutationValue(command string, value any) error {
	switch value.(type) {
	case string, []byte:
		return nil
	}
	valueType := reflect.TypeOf(value)
	if valueType != nil && (valueType.Kind() == reflect.String ||
		(valueType.Kind() == reflect.Slice && valueType.Elem().Kind() == reflect.Uint8)) {
		return nil
	}
	return fmt.Errorf("%s value must be string or []byte, got %T", command, value)
}

func (s *KeyValueStore) PTTL(ctx context.Context, key string) (int64, error) {
	value, err := s.commandReply(ctx, "PTTL", key)
	return keyValueTTLResponse("PTTL", value, err)
}

func (s *KeyValueStore) PExpire(ctx context.Context, key string, milliseconds int64) (bool, error) {
	value, err := s.commandReply(ctx, "PEXPIRE", key, milliseconds)
	return responseBool(value, err)
}

func (s *KeyValueStore) ExpireAt(ctx context.Context, key string, unixSeconds int64) (bool, error) {
	value, err := s.commandReply(ctx, "EXPIREAT", key, unixSeconds)
	return responseBool(value, err)
}

func (s *KeyValueStore) PExpireAt(ctx context.Context, key string, unixMilliseconds int64) (bool, error) {
	value, err := s.commandReply(ctx, "PEXPIREAT", key, unixMilliseconds)
	return responseBool(value, err)
}

func (s *KeyValueStore) Persist(ctx context.Context, key string) (bool, error) {
	value, err := s.commandReply(ctx, "PERSIST", key)
	return responseBool(value, err)
}

func (s *KeyValueStore) ExpireTime(ctx context.Context, key string) (int64, error) {
	value, err := s.commandReply(ctx, "EXPIRETIME", key)
	return keyValueTTLResponse("EXPIRETIME", value, err)
}

func (s *KeyValueStore) PExpireTime(ctx context.Context, key string) (int64, error) {
	value, err := s.commandReply(ctx, "PEXPIRETIME", key)
	return keyValueTTLResponse("PEXPIRETIME", value, err)
}
