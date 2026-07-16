package ferricstore

import (
	"context"
	"errors"
)

func (s *HashStore) MGet(ctx context.Context, key string, fields ...string) ([]any, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	args := make([]any, 2, len(fields)+2)
	args[0], args[1] = "HMGET", key
	for _, field := range fields {
		args = append(args, field)
	}
	value, err := s.client.typedReply(ctx, args...)
	return decodeArrayExact(s.client.codec, value, err, len(fields), "HMGET")
}

func (s *HashStore) Exists(ctx context.Context, key, field string) (bool, error) {
	value, err := s.client.typedReply(ctx, "HEXISTS", key, field)
	return responseBool(value, err)
}

func (s *HashStore) Keys(ctx context.Context, key string) ([]string, error) {
	value, err := s.client.typedReply(ctx, "HKEYS", key)
	return stringArray(value, err)
}

func (s *HashStore) Values(ctx context.Context, key string) ([]any, error) {
	value, err := s.client.typedReply(ctx, "HVALS", key)
	return decodeArray(s.client.codec, value, err)
}

func (s *HashStore) Len(ctx context.Context, key string) (int64, error) {
	value, err := s.client.typedReply(ctx, "HLEN", key)
	return nonNegativeInt64Response("HLEN", value, err)
}

func (s *HashStore) IncrBy(ctx context.Context, key, field string, increment int64) (int64, error) {
	value, err := s.client.typedReply(ctx, "HINCRBY", key, field, increment)
	return responseInt64(value, err)
}

func (s *HashStore) IncrByFloat(ctx context.Context, key, field string, increment float64) (float64, error) {
	if err := validateFiniteFloat("HINCRBYFLOAT", increment); err != nil {
		return 0, err
	}
	value, err := s.client.typedReply(ctx, "HINCRBYFLOAT", key, field, increment)
	return responseFloat64(value, err)
}

func (s *HashStore) SetNX(ctx context.Context, key, field string, value any) (bool, error) {
	encoded, err := s.client.encode(value)
	if err != nil {
		return false, err
	}
	response, err := s.client.typedReply(ctx, "HSETNX", key, field, encoded)
	return responseBool(response, err)
}

func (s *HashStore) StrLen(ctx context.Context, key, field string) (int64, error) {
	value, err := s.client.typedReply(ctx, "HSTRLEN", key, field)
	return nonNegativeInt64Response("HSTRLEN", value, err)
}

func (s *HashStore) RandField(ctx context.Context, key string, count *int, withValues bool) (any, error) {
	if withValues && count == nil {
		return nil, errors.New("HRANDFIELD WITHVALUES requires count")
	}
	args := []any{"HRANDFIELD", key}
	if count != nil {
		args = append(args, *count)
	}
	if withValues {
		args = append(args, "WITHVALUES")
	}
	value, err := s.client.typedReply(ctx, args...)
	return decodeHashRandomField(s.client.codec, value, err, count, withValues)
}

func (s *HashStore) Scan(ctx context.Context, key string, cursor int64, match string, count *int) (any, error) {
	return s.scanCursor(ctx, key, cursor, match, count)
}

// ScanCursor continues HSCAN using an opaque cursor returned by FerricStore.
func (s *HashStore) ScanCursor(ctx context.Context, key string, cursor any, match string, count *int) (any, error) {
	return s.scanCursor(ctx, key, cursor, match, count)
}

func (s *HashStore) scanCursor(ctx context.Context, key string, cursor any, match string, count *int) (any, error) {
	normalizedCursor, err := normalizeScanCursor(cursor, true)
	if err != nil {
		return nil, err
	}
	if err := validateScanCount(count); err != nil {
		return nil, err
	}
	args := []any{"HSCAN", key, normalizedCursor}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	appendScanCount(&args, count)
	value, err := s.client.typedReply(ctx, args...)
	return decodeCollectionScan(s.client.codec, value, err, hashCollectionScan, "HSCAN")
}

func (s *HashStore) GetDel(ctx context.Context, key string, fields ...string) ([]any, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	args := []any{"HGETDEL", key, "FIELDS", len(fields)}
	for _, field := range fields {
		args = append(args, field)
	}
	value, err := s.client.typedReply(ctx, args...)
	return decodeArrayExact(s.client.codec, value, err, len(fields), "HGETDEL")
}

type HashGetEXOptions struct {
	EXSeconds      *int64
	PXMilliseconds *int64
	EXATSeconds    *int64
	PXATMillis     *int64
	Persist        bool
}

func (s *HashStore) GetEX(ctx context.Context, key string, fields []string, opt HashGetEXOptions) ([]any, error) {
	if countPresentInt64(opt.EXSeconds, opt.PXMilliseconds, opt.EXATSeconds, opt.PXATMillis)+boolInt(opt.Persist) > 1 {
		return nil, errors.New("HGETEX accepts only one expiration or PERSIST option")
	}
	if err := validatePositiveExpiryOptions("HGETEX", opt.EXSeconds, opt.PXMilliseconds, opt.EXATSeconds, opt.PXATMillis); err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, nil
	}
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
	value, err := s.client.typedReply(ctx, args...)
	return decodeArrayExact(s.client.codec, value, err, len(fields), "HGETEX")
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
	if err := validatePositiveExpiryOptions("HSETEX", opt.EXSeconds); err != nil {
		return false, err
	}
	if len(values) == 0 {
		return false, errors.New("HSETEX requires at least one field/value pair")
	}
	args := []any{"HSETEX", key, *opt.EXSeconds}
	for _, field := range sortedKeys(values) {
		encoded, err := s.client.encode(values[field])
		if err != nil {
			return false, err
		}
		args = append(args, field, encoded)
	}
	response, err := s.client.typedReply(ctx, args...)
	if err != nil {
		return false, err
	}
	if _, intErr := responseInt64(response, nil); intErr == nil {
		_, err := boundedCountResponse("HSETEX", len(values), response, nil)
		return err == nil, err
	}
	return responseBool(response, nil)
}

func (s *HashStore) Expire(ctx context.Context, key string, seconds int64, fields ...string) (any, error) {
	if seconds <= 0 {
		return nil, errors.New("HEXPIRE expiration must be positive")
	}
	if err := validateHashFieldArgs("HEXPIRE", fields); err != nil {
		return nil, err
	}
	args := []any{"HEXPIRE", key, seconds, "FIELDS", len(fields)}
	for _, field := range fields {
		args = append(args, field)
	}
	value, err := s.client.typedReply(ctx, args...)
	return hashFieldIntegerResponse("HEXPIRE", value, err, len(fields), hashFieldExpiryResult)
}

func (s *HashStore) PExpire(ctx context.Context, key string, milliseconds int64, fields ...string) (any, error) {
	if milliseconds <= 0 {
		return nil, errors.New("HPEXPIRE expiration must be positive")
	}
	if err := validateHashFieldArgs("HPEXPIRE", fields); err != nil {
		return nil, err
	}
	args := []any{"HPEXPIRE", key, milliseconds, "FIELDS", len(fields)}
	for _, field := range fields {
		args = append(args, field)
	}
	value, err := s.client.typedReply(ctx, args...)
	return hashFieldIntegerResponse("HPEXPIRE", value, err, len(fields), hashFieldExpiryResult)
}

func (s *HashStore) TTL(ctx context.Context, key string, fields ...string) (any, error) {
	if err := validateHashFieldArgs("HTTL", fields); err != nil {
		return nil, err
	}
	args := []any{"HTTL", key, "FIELDS", len(fields)}
	for _, field := range fields {
		args = append(args, field)
	}
	value, err := s.client.typedReply(ctx, args...)
	return hashFieldIntegerResponse("HTTL", value, err, len(fields), hashFieldTTLResult)
}

func (s *HashStore) PTTL(ctx context.Context, key string, fields ...string) (any, error) {
	if err := validateHashFieldArgs("HPTTL", fields); err != nil {
		return nil, err
	}
	args := []any{"HPTTL", key, "FIELDS", len(fields)}
	for _, field := range fields {
		args = append(args, field)
	}
	value, err := s.client.typedReply(ctx, args...)
	return hashFieldIntegerResponse("HPTTL", value, err, len(fields), hashFieldTTLResult)
}

func (s *HashStore) ExpireTime(ctx context.Context, key string, fields ...string) (any, error) {
	if err := validateHashFieldArgs("HEXPIRETIME", fields); err != nil {
		return nil, err
	}
	args := []any{"HEXPIRETIME", key, "FIELDS", len(fields)}
	for _, field := range fields {
		args = append(args, field)
	}
	value, err := s.client.typedReply(ctx, args...)
	return hashFieldIntegerResponse("HEXPIRETIME", value, err, len(fields), hashFieldTTLResult)
}

func (s *HashStore) PExpireTime(ctx context.Context, key string, fields ...string) (any, error) {
	if err := validateHashFieldArgs("HPEXPIRETIME", fields); err != nil {
		return nil, err
	}
	args := []any{"HPEXPIRETIME", key, "FIELDS", len(fields)}
	for _, field := range fields {
		args = append(args, field)
	}
	value, err := s.client.typedReply(ctx, args...)
	return hashFieldIntegerResponse("HPEXPIRETIME", value, err, len(fields), hashFieldTTLResult)
}

func (s *HashStore) Persist(ctx context.Context, key string, fields ...string) (any, error) {
	if err := validateHashFieldArgs("HPERSIST", fields); err != nil {
		return nil, err
	}
	args := []any{"HPERSIST", key, "FIELDS", len(fields)}
	for _, field := range fields {
		args = append(args, field)
	}
	value, err := s.client.typedReply(ctx, args...)
	return hashFieldIntegerResponse("HPERSIST", value, err, len(fields), hashFieldPersistResult)
}
