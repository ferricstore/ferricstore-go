package ferricstore

import (
	"context"
	"errors"
)

func (s *HashStore) MGet(ctx context.Context, key string, fields ...string) ([]any, error) {
	args := []any{"HMGET", key}
	for _, field := range fields {
		args = append(args, field)
	}
	value, err := s.client.Command(ctx, args...)
	return decodeArray(s.client.codec, value, err)
}

func (s *HashStore) Exists(ctx context.Context, key, field string) (bool, error) {
	value, err := s.client.Command(ctx, "HEXISTS", key, field)
	return asBool(value), err
}

func (s *HashStore) Keys(ctx context.Context, key string) ([]string, error) {
	value, err := s.client.Command(ctx, "HKEYS", key)
	return stringArray(value, err)
}

func (s *HashStore) Values(ctx context.Context, key string) ([]any, error) {
	value, err := s.client.Command(ctx, "HVALS", key)
	return decodeArray(s.client.codec, value, err)
}

func (s *HashStore) Len(ctx context.Context, key string) (int64, error) {
	value, err := s.client.Command(ctx, "HLEN", key)
	return asInt64(value), err
}

func (s *HashStore) IncrBy(ctx context.Context, key, field string, increment int64) (int64, error) {
	value, err := s.client.Command(ctx, "HINCRBY", key, field, increment)
	return asInt64(value), err
}

func (s *HashStore) IncrByFloat(ctx context.Context, key, field string, increment float64) (float64, error) {
	value, err := s.client.Command(ctx, "HINCRBYFLOAT", key, field, increment)
	return asFloat64(value), err
}

func (s *HashStore) SetNX(ctx context.Context, key, field string, value any) (bool, error) {
	encoded, err := s.client.encode(value)
	if err != nil {
		return false, err
	}
	response, err := s.client.Command(ctx, "HSETNX", key, field, encoded)
	return asBool(response), err
}

func (s *HashStore) StrLen(ctx context.Context, key, field string) (int64, error) {
	value, err := s.client.Command(ctx, "HSTRLEN", key, field)
	return asInt64(value), err
}

func (s *HashStore) RandField(ctx context.Context, key string, count *int, withValues bool) (any, error) {
	args := []any{"HRANDFIELD", key}
	if count != nil {
		args = append(args, *count)
	}
	if withValues {
		args = append(args, "WITHVALUES")
	}
	return s.client.Command(ctx, args...)
}

func (s *HashStore) Scan(ctx context.Context, key string, cursor int64, match string, count *int) (any, error) {
	args := []any{"HSCAN", key, cursor}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	appendScanCount(&args, count)
	return s.client.Command(ctx, args...)
}

func (s *HashStore) GetDel(ctx context.Context, key string, fields ...string) ([]any, error) {
	args := []any{"HGETDEL", key, "FIELDS", len(fields)}
	for _, field := range fields {
		args = append(args, field)
	}
	value, err := s.client.Command(ctx, args...)
	return decodeArray(s.client.codec, value, err)
}

type HashGetEXOptions struct {
	EXSeconds      *int64
	PXMilliseconds *int64
	EXATSeconds    *int64
	PXATMillis     *int64
	Persist        bool
}

func (s *HashStore) GetEX(ctx context.Context, key string, fields []string, opt HashGetEXOptions) ([]any, error) {
	args := []any{"HGETEX", key}
	appendInt64Ptr(&args, "EX", opt.EXSeconds)
	appendInt64Ptr(&args, "PX", opt.PXMilliseconds)
	appendInt64Ptr(&args, "EXAT", opt.EXATSeconds)
	appendInt64Ptr(&args, "PXAT", opt.PXATMillis)
	if opt.Persist {
		args = append(args, "PERSIST")
	}
	args = append(args, "FIELDS", len(fields))
	for _, field := range fields {
		args = append(args, field)
	}
	value, err := s.client.Command(ctx, args...)
	return decodeArray(s.client.codec, value, err)
}

type HashSetEXOptions struct {
	EXSeconds      *int64
	PXMilliseconds *int64
	EXATSeconds    *int64
	PXATMillis     *int64
	KeepTTL        bool
	FNXX           bool
	FXX            bool
}

func (s *HashStore) SetEX(ctx context.Context, key string, values map[string]any, opt HashSetEXOptions) (bool, error) {
	if opt.EXSeconds == nil {
		return false, errors.New("HSETEX requires EXSeconds")
	}
	if opt.PXMilliseconds != nil || opt.EXATSeconds != nil || opt.PXATMillis != nil || opt.KeepTTL || opt.FNXX || opt.FXX {
		return false, errors.New("HSETEX only supports EXSeconds")
	}
	args := []any{"HSETEX", key, *opt.EXSeconds}
	for _, field := range sortedKeys(values) {
		encoded, err := s.client.encode(values[field])
		if err != nil {
			return false, err
		}
		args = append(args, field, encoded)
	}
	response, err := s.client.Command(ctx, args...)
	return asInt64(response) >= 0 || asBool(response), err
}

func (s *HashStore) Expire(ctx context.Context, key string, seconds int64, fields ...string) (any, error) {
	args := []any{"HEXPIRE", key, seconds, "FIELDS", len(fields)}
	for _, field := range fields {
		args = append(args, field)
	}
	return s.client.Command(ctx, args...)
}

func (s *HashStore) PExpire(ctx context.Context, key string, milliseconds int64, fields ...string) (any, error) {
	args := []any{"HPEXPIRE", key, milliseconds, "FIELDS", len(fields)}
	for _, field := range fields {
		args = append(args, field)
	}
	return s.client.Command(ctx, args...)
}

func (s *HashStore) TTL(ctx context.Context, key string, fields ...string) (any, error) {
	args := []any{"HTTL", key, "FIELDS", len(fields)}
	for _, field := range fields {
		args = append(args, field)
	}
	return s.client.Command(ctx, args...)
}

func (s *HashStore) PTTL(ctx context.Context, key string, fields ...string) (any, error) {
	args := []any{"HPTTL", key, "FIELDS", len(fields)}
	for _, field := range fields {
		args = append(args, field)
	}
	return s.client.Command(ctx, args...)
}

func (s *HashStore) ExpireTime(ctx context.Context, key string, fields ...string) (any, error) {
	args := []any{"HEXPIRETIME", key, "FIELDS", len(fields)}
	for _, field := range fields {
		args = append(args, field)
	}
	return s.client.Command(ctx, args...)
}

func (s *HashStore) PExpireTime(ctx context.Context, key string, fields ...string) (any, error) {
	args := []any{"HPEXPIRETIME", key, "FIELDS", len(fields)}
	for _, field := range fields {
		args = append(args, field)
	}
	return s.client.Command(ctx, args...)
}

func (s *HashStore) Persist(ctx context.Context, key string, fields ...string) (any, error) {
	args := []any{"HPERSIST", key, "FIELDS", len(fields)}
	for _, field := range fields {
		args = append(args, field)
	}
	return s.client.Command(ctx, args...)
}
