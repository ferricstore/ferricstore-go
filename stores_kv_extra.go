package ferricstore

import "context"

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
	response, err := s.client.Command(ctx, args...)
	if err != nil || !opt.Get || response == nil {
		return response, err
	}
	return s.client.codec.Decode(response)
}

func (s *KeyValueStore) MSet(ctx context.Context, values map[string]any) error {
	args := []any{"MSET"}
	for _, key := range sortedKeys(values) {
		encoded, err := s.client.encode(values[key])
		if err != nil {
			return err
		}
		args = append(args, key, encoded)
	}
	_, err := s.client.Command(ctx, args...)
	return err
}

func (s *KeyValueStore) MSetNX(ctx context.Context, values map[string]any) (bool, error) {
	args := []any{"MSETNX"}
	for _, key := range sortedKeys(values) {
		encoded, err := s.client.encode(values[key])
		if err != nil {
			return false, err
		}
		args = append(args, key, encoded)
	}
	response, err := s.client.Command(ctx, args...)
	return asBool(response), err
}

func (s *KeyValueStore) IncrBy(ctx context.Context, key string, increment int64) (int64, error) {
	value, err := s.client.Command(ctx, "INCRBY", key, increment)
	return asInt64(value), err
}

func (s *KeyValueStore) DecrBy(ctx context.Context, key string, decrement int64) (int64, error) {
	value, err := s.client.Command(ctx, "DECRBY", key, decrement)
	return asInt64(value), err
}

func (s *KeyValueStore) IncrByFloat(ctx context.Context, key string, increment float64) (float64, error) {
	value, err := s.client.Command(ctx, "INCRBYFLOAT", key, increment)
	return asFloat64(value), err
}

func (s *KeyValueStore) Append(ctx context.Context, key string, value any) (int64, error) {
	encoded, err := s.client.encode(value)
	if err != nil {
		return 0, err
	}
	response, err := s.client.Command(ctx, "APPEND", key, encoded)
	return asInt64(response), err
}

func (s *KeyValueStore) StrLen(ctx context.Context, key string) (int64, error) {
	value, err := s.client.Command(ctx, "STRLEN", key)
	return asInt64(value), err
}

func (s *KeyValueStore) GetSet(ctx context.Context, key string, value any) (any, error) {
	encoded, err := s.client.encode(value)
	if err != nil {
		return nil, err
	}
	response, err := s.client.Command(ctx, "GETSET", key, encoded)
	if err != nil || response == nil {
		return response, err
	}
	return s.client.codec.Decode(response)
}

func (s *KeyValueStore) GetDel(ctx context.Context, key string) (any, error) {
	response, err := s.client.Command(ctx, "GETDEL", key)
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
	args := []any{"GETEX", key}
	appendInt64Ptr(&args, "EX", opt.EXSeconds)
	appendInt64Ptr(&args, "PX", opt.PXMilliseconds)
	appendInt64Ptr(&args, "EXAT", opt.EXATSeconds)
	appendInt64Ptr(&args, "PXAT", opt.PXATMillis)
	if opt.Persist {
		args = append(args, "PERSIST")
	}
	response, err := s.client.Command(ctx, args...)
	if err != nil || response == nil {
		return response, err
	}
	return s.client.codec.Decode(response)
}

func (s *KeyValueStore) SetNX(ctx context.Context, key string, value any) (bool, error) {
	encoded, err := s.client.encode(value)
	if err != nil {
		return false, err
	}
	response, err := s.client.Command(ctx, "SETNX", key, encoded)
	return asBool(response), err
}

func (s *KeyValueStore) SetEX(ctx context.Context, key string, seconds int64, value any) error {
	encoded, err := s.client.encode(value)
	if err != nil {
		return err
	}
	_, err = s.client.Command(ctx, "SETEX", key, seconds, encoded)
	return err
}

func (s *KeyValueStore) PSetEX(ctx context.Context, key string, milliseconds int64, value any) error {
	encoded, err := s.client.encode(value)
	if err != nil {
		return err
	}
	_, err = s.client.Command(ctx, "PSETEX", key, milliseconds, encoded)
	return err
}

func (s *KeyValueStore) GetRange(ctx context.Context, key string, start, end int64) (any, error) {
	response, err := s.client.Command(ctx, "GETRANGE", key, start, end)
	if err != nil || response == nil {
		return response, err
	}
	return s.client.codec.Decode(response)
}

func (s *KeyValueStore) SetRange(ctx context.Context, key string, offset int64, value any) (int64, error) {
	encoded, err := s.client.encode(value)
	if err != nil {
		return 0, err
	}
	response, err := s.client.Command(ctx, "SETRANGE", key, offset, encoded)
	return asInt64(response), err
}

func (s *KeyValueStore) PTTL(ctx context.Context, key string) (int64, error) {
	value, err := s.client.Command(ctx, "PTTL", key)
	return asInt64(value), err
}

func (s *KeyValueStore) PExpire(ctx context.Context, key string, milliseconds int64) (bool, error) {
	value, err := s.client.Command(ctx, "PEXPIRE", key, milliseconds)
	return asBool(value), err
}

func (s *KeyValueStore) ExpireAt(ctx context.Context, key string, unixSeconds int64) (bool, error) {
	value, err := s.client.Command(ctx, "EXPIREAT", key, unixSeconds)
	return asBool(value), err
}

func (s *KeyValueStore) PExpireAt(ctx context.Context, key string, unixMilliseconds int64) (bool, error) {
	value, err := s.client.Command(ctx, "PEXPIREAT", key, unixMilliseconds)
	return asBool(value), err
}

func (s *KeyValueStore) Persist(ctx context.Context, key string) (bool, error) {
	value, err := s.client.Command(ctx, "PERSIST", key)
	return asBool(value), err
}

func (s *KeyValueStore) ExpireTime(ctx context.Context, key string) (int64, error) {
	value, err := s.client.Command(ctx, "EXPIRETIME", key)
	return asInt64(value), err
}

func (s *KeyValueStore) PExpireTime(ctx context.Context, key string) (int64, error) {
	value, err := s.client.Command(ctx, "PEXPIRETIME", key)
	return asInt64(value), err
}
