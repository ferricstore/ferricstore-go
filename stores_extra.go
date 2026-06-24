package ferricstore

import (
	"context"
	"errors"
	"sort"
	"strconv"
)

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

func (s *ListStore) Len(ctx context.Context, key string) (int64, error) {
	value, err := s.client.Command(ctx, "LLEN", key)
	return asInt64(value), err
}

func (s *ListStore) Index(ctx context.Context, key string, index int64) (any, error) {
	value, err := s.client.Command(ctx, "LINDEX", key, index)
	if err != nil || value == nil {
		return value, err
	}
	return s.client.codec.Decode(value)
}

func (s *ListStore) Set(ctx context.Context, key string, index int64, value any) error {
	encoded, err := s.client.encode(value)
	if err != nil {
		return err
	}
	_, err = s.client.Command(ctx, "LSET", key, index, encoded)
	return err
}

func (s *ListStore) Rem(ctx context.Context, key string, count int64, value any) (int64, error) {
	encoded, err := s.client.encode(value)
	if err != nil {
		return 0, err
	}
	response, err := s.client.Command(ctx, "LREM", key, count, encoded)
	return asInt64(response), err
}

func (s *ListStore) Trim(ctx context.Context, key string, start, stop int64) error {
	_, err := s.client.Command(ctx, "LTRIM", key, start, stop)
	return err
}

func (s *ListStore) Pos(ctx context.Context, key string, value any, rank, count, maxLen *int64) (any, error) {
	encoded, err := s.client.encode(value)
	if err != nil {
		return nil, err
	}
	args := []any{"LPOS", key, encoded}
	appendInt64Ptr(&args, "RANK", rank)
	appendInt64Ptr(&args, "COUNT", count)
	appendInt64Ptr(&args, "MAXLEN", maxLen)
	return s.client.Command(ctx, args...)
}

func (s *ListStore) Insert(ctx context.Context, key string, before bool, pivot, value any) (int64, error) {
	encodedPivot, err := s.client.encode(pivot)
	if err != nil {
		return 0, err
	}
	encodedValue, err := s.client.encode(value)
	if err != nil {
		return 0, err
	}
	where := "AFTER"
	if before {
		where = "BEFORE"
	}
	response, err := s.client.Command(ctx, "LINSERT", key, where, encodedPivot, encodedValue)
	return asInt64(response), err
}

func (s *ListStore) Move(ctx context.Context, source, destination, whereFrom, whereTo string) (any, error) {
	value, err := s.client.Command(ctx, "LMOVE", source, destination, whereFrom, whereTo)
	if err != nil || value == nil {
		return value, err
	}
	return s.client.codec.Decode(value)
}

func (s *ListStore) RPopLPush(ctx context.Context, source, destination string) (any, error) {
	value, err := s.client.Command(ctx, "RPOPLPUSH", source, destination)
	if err != nil || value == nil {
		return value, err
	}
	return s.client.codec.Decode(value)
}

func (s *ListStore) LPushX(ctx context.Context, key string, values ...any) (int64, error) {
	return s.push(ctx, "LPUSHX", key, values...)
}

func (s *ListStore) RPushX(ctx context.Context, key string, values ...any) (int64, error) {
	return s.push(ctx, "RPUSHX", key, values...)
}

func (s *ListStore) BLPop(ctx context.Context, timeoutSeconds int64, keys ...string) (any, error) {
	args := []any{"BLPOP"}
	for _, key := range keys {
		args = append(args, key)
	}
	args = append(args, timeoutSeconds)
	return s.client.Command(ctx, args...)
}

func (s *ListStore) BRPop(ctx context.Context, timeoutSeconds int64, keys ...string) (any, error) {
	args := []any{"BRPOP"}
	for _, key := range keys {
		args = append(args, key)
	}
	args = append(args, timeoutSeconds)
	return s.client.Command(ctx, args...)
}

func (s *ListStore) BLMove(ctx context.Context, source, destination, whereFrom, whereTo string, timeoutSeconds int64) (any, error) {
	value, err := s.client.Command(ctx, "BLMOVE", source, destination, whereFrom, whereTo, timeoutSeconds)
	if err != nil || value == nil {
		return value, err
	}
	return s.client.codec.Decode(value)
}

func (s *ListStore) BLMPop(ctx context.Context, timeoutSeconds float64, keys []string, where string, count *int) (any, error) {
	args := []any{"BLMPOP", floatArg(timeoutSeconds), len(keys)}
	for _, key := range keys {
		args = append(args, key)
	}
	args = append(args, where)
	appendIntPtr(&args, "COUNT", count)
	return s.client.Command(ctx, args...)
}

func (s *SetStore) IsMember(ctx context.Context, key string, member any) (bool, error) {
	encoded, err := s.client.encode(member)
	if err != nil {
		return false, err
	}
	value, err := s.client.Command(ctx, "SISMEMBER", key, encoded)
	return asBool(value), err
}

func (s *SetStore) MIsMember(ctx context.Context, key string, members ...any) ([]bool, error) {
	args := []any{"SMISMEMBER", key}
	for _, member := range members {
		encoded, err := s.client.encode(member)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.Command(ctx, args...)
	return boolArray(value, err)
}

func (s *SetStore) Card(ctx context.Context, key string) (int64, error) {
	value, err := s.client.Command(ctx, "SCARD", key)
	return asInt64(value), err
}

func (s *SetStore) RandMember(ctx context.Context, key string, count *int) ([]any, error) {
	args := []any{"SRANDMEMBER", key}
	if count != nil {
		args = append(args, *count)
	}
	value, err := s.client.Command(ctx, args...)
	if count == nil && err == nil && value != nil {
		decoded, err := s.client.codec.Decode(value)
		return []any{decoded}, err
	}
	return decodeArray(s.client.codec, value, err)
}

func (s *SetStore) Pop(ctx context.Context, key string, count *int) ([]any, error) {
	args := []any{"SPOP", key}
	if count != nil {
		args = append(args, *count)
	}
	value, err := s.client.Command(ctx, args...)
	if count == nil && err == nil && value != nil {
		decoded, err := s.client.codec.Decode(value)
		return []any{decoded}, err
	}
	return decodeArray(s.client.codec, value, err)
}

func (s *SetStore) Diff(ctx context.Context, keys ...string) ([]any, error) {
	return s.setOp(ctx, "SDIFF", keys...)
}

func (s *SetStore) Inter(ctx context.Context, keys ...string) ([]any, error) {
	return s.setOp(ctx, "SINTER", keys...)
}

func (s *SetStore) Union(ctx context.Context, keys ...string) ([]any, error) {
	return s.setOp(ctx, "SUNION", keys...)
}

func (s *SetStore) setOp(ctx context.Context, command string, keys ...string) ([]any, error) {
	args := []any{command}
	for _, key := range keys {
		args = append(args, key)
	}
	value, err := s.client.Command(ctx, args...)
	return decodeArray(s.client.codec, value, err)
}

func (s *SetStore) DiffStore(ctx context.Context, destination string, keys ...string) (int64, error) {
	return s.setStoreOp(ctx, "SDIFFSTORE", destination, keys...)
}

func (s *SetStore) InterStore(ctx context.Context, destination string, keys ...string) (int64, error) {
	return s.setStoreOp(ctx, "SINTERSTORE", destination, keys...)
}

func (s *SetStore) UnionStore(ctx context.Context, destination string, keys ...string) (int64, error) {
	return s.setStoreOp(ctx, "SUNIONSTORE", destination, keys...)
}

func (s *SetStore) setStoreOp(ctx context.Context, command, destination string, keys ...string) (int64, error) {
	args := []any{command, destination}
	for _, key := range keys {
		args = append(args, key)
	}
	value, err := s.client.Command(ctx, args...)
	return asInt64(value), err
}

func (s *SetStore) InterCard(ctx context.Context, keys []string, limit *int64) (int64, error) {
	args := []any{"SINTERCARD", len(keys)}
	for _, key := range keys {
		args = append(args, key)
	}
	appendInt64Ptr(&args, "LIMIT", limit)
	value, err := s.client.Command(ctx, args...)
	return asInt64(value), err
}

func (s *SetStore) Move(ctx context.Context, source, destination string, member any) (bool, error) {
	encoded, err := s.client.encode(member)
	if err != nil {
		return false, err
	}
	value, err := s.client.Command(ctx, "SMOVE", source, destination, encoded)
	return asBool(value), err
}

func (s *SetStore) Scan(ctx context.Context, key string, cursor int64, match string, count *int) (any, error) {
	args := []any{"SSCAN", key, cursor}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	appendScanCount(&args, count)
	return s.client.Command(ctx, args...)
}

type ZAddOptions struct {
	NX bool
	XX bool
	GT bool
	LT bool
	CH bool
}

func (s *SortedSetStore) AddWithOptions(ctx context.Context, key string, opt ZAddOptions, members ...ZAddMember) (int64, error) {
	args := []any{"ZADD", key}
	if opt.NX {
		args = append(args, "NX")
	}
	if opt.XX {
		args = append(args, "XX")
	}
	if opt.GT {
		args = append(args, "GT")
	}
	if opt.LT {
		args = append(args, "LT")
	}
	if opt.CH {
		args = append(args, "CH")
	}
	for _, member := range members {
		encoded, err := s.client.encode(member.Member)
		if err != nil {
			return 0, err
		}
		args = append(args, member.Score, encoded)
	}
	value, err := s.client.Command(ctx, args...)
	return asInt64(value), err
}

func (s *SortedSetStore) Score(ctx context.Context, key string, member any) (float64, error) {
	encoded, err := s.client.encode(member)
	if err != nil {
		return 0, err
	}
	value, err := s.client.Command(ctx, "ZSCORE", key, encoded)
	return asFloat64(value), err
}

func (s *SortedSetStore) Rank(ctx context.Context, key string, member any) (int64, error) {
	return s.rank(ctx, "ZRANK", key, member)
}

func (s *SortedSetStore) RevRank(ctx context.Context, key string, member any) (int64, error) {
	return s.rank(ctx, "ZREVRANK", key, member)
}

func (s *SortedSetStore) rank(ctx context.Context, command, key string, member any) (int64, error) {
	encoded, err := s.client.encode(member)
	if err != nil {
		return 0, err
	}
	value, err := s.client.Command(ctx, command, key, encoded)
	return asInt64(value), err
}

func (s *SortedSetStore) RevRange(ctx context.Context, key string, start, stop int64) ([]any, error) {
	value, err := s.client.Command(ctx, "ZREVRANGE", key, start, stop)
	return decodeArray(s.client.codec, value, err)
}

func (s *SortedSetStore) Card(ctx context.Context, key string) (int64, error) {
	value, err := s.client.Command(ctx, "ZCARD", key)
	return asInt64(value), err
}

func (s *SortedSetStore) Rem(ctx context.Context, key string, members ...any) (int64, error) {
	args := []any{"ZREM", key}
	for _, member := range members {
		encoded, err := s.client.encode(member)
		if err != nil {
			return 0, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.Command(ctx, args...)
	return asInt64(value), err
}

func (s *SortedSetStore) IncrBy(ctx context.Context, key string, increment float64, member any) (float64, error) {
	encoded, err := s.client.encode(member)
	if err != nil {
		return 0, err
	}
	value, err := s.client.Command(ctx, "ZINCRBY", key, increment, encoded)
	return asFloat64(value), err
}

func (s *SortedSetStore) Count(ctx context.Context, key, min, max string) (int64, error) {
	value, err := s.client.Command(ctx, "ZCOUNT", key, min, max)
	return asInt64(value), err
}

func (s *SortedSetStore) PopMin(ctx context.Context, key string, count *int) (any, error) {
	return s.zpop(ctx, "ZPOPMIN", key, count)
}

func (s *SortedSetStore) PopMax(ctx context.Context, key string, count *int) (any, error) {
	return s.zpop(ctx, "ZPOPMAX", key, count)
}

func (s *SortedSetStore) zpop(ctx context.Context, command, key string, count *int) (any, error) {
	args := []any{command, key}
	if count != nil {
		args = append(args, *count)
	}
	return s.client.Command(ctx, args...)
}

func (s *SortedSetStore) RandMember(ctx context.Context, key string, count *int, withScores bool) (any, error) {
	args := []any{"ZRANDMEMBER", key}
	if count != nil {
		args = append(args, *count)
	}
	if withScores {
		args = append(args, "WITHSCORES")
	}
	return s.client.Command(ctx, args...)
}

func (s *SortedSetStore) MScore(ctx context.Context, key string, members ...any) ([]float64, error) {
	args := []any{"ZMSCORE", key}
	for _, member := range members {
		encoded, err := s.client.encode(member)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.Command(ctx, args...)
	return floatArray(value, err)
}

func (s *SortedSetStore) RangeByScore(ctx context.Context, key, min, max string, withScores bool, limitOffset, limitCount *int64) (any, error) {
	args := []any{"ZRANGEBYSCORE", key, min, max}
	if withScores {
		args = append(args, "WITHSCORES")
	}
	if limitOffset != nil && limitCount != nil {
		args = append(args, "LIMIT", *limitOffset, *limitCount)
	}
	return s.client.Command(ctx, args...)
}

func (s *SortedSetStore) RevRangeByScore(ctx context.Context, key, max, min string, withScores bool, limitOffset, limitCount *int64) (any, error) {
	args := []any{"ZREVRANGEBYSCORE", key, max, min}
	if withScores {
		args = append(args, "WITHSCORES")
	}
	if limitOffset != nil && limitCount != nil {
		args = append(args, "LIMIT", *limitOffset, *limitCount)
	}
	return s.client.Command(ctx, args...)
}

func (s *SortedSetStore) Scan(ctx context.Context, key string, cursor int64, match string, count *int) (any, error) {
	args := []any{"ZSCAN", key, cursor}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	appendScanCount(&args, count)
	return s.client.Command(ctx, args...)
}

func (s *StreamStore) Len(ctx context.Context, key string) (int64, error) {
	value, err := s.client.Command(ctx, "XLEN", key)
	return asInt64(value), err
}

func (s *StreamStore) RevRange(ctx context.Context, key, end, start string, count *int) (any, error) {
	args := []any{"XREVRANGE", key, end, start}
	appendIntPtr(&args, "COUNT", count)
	return s.client.Command(ctx, args...)
}

type StreamReadOptions struct {
	Count   *int
	BlockMS *int64
	Streams []StreamRef
}

type StreamRef struct {
	Key string
	ID  string
}

func (s *StreamStore) Read(ctx context.Context, opt StreamReadOptions) (any, error) {
	args := []any{"XREAD"}
	appendIntPtr(&args, "COUNT", opt.Count)
	appendInt64Ptr(&args, "BLOCK", opt.BlockMS)
	args = append(args, "STREAMS")
	for _, stream := range opt.Streams {
		args = append(args, stream.Key)
	}
	for _, stream := range opt.Streams {
		args = append(args, stream.ID)
	}
	return s.client.Command(ctx, args...)
}

func (s *StreamStore) Trim(ctx context.Context, key string, approximate bool, threshold string, limit *int) (int64, error) {
	args := []any{"XTRIM", key, "MAXLEN"}
	if approximate {
		args = append(args, "~")
	}
	args = append(args, threshold)
	appendIntPtr(&args, "LIMIT", limit)
	value, err := s.client.Command(ctx, args...)
	return asInt64(value), err
}

func (s *StreamStore) Del(ctx context.Context, key string, ids ...string) (int64, error) {
	args := []any{"XDEL", key}
	for _, id := range ids {
		args = append(args, id)
	}
	value, err := s.client.Command(ctx, args...)
	return asInt64(value), err
}

func (s *StreamStore) Info(ctx context.Context, key string) (any, error) {
	return s.client.Command(ctx, "XINFO", "STREAM", key)
}

func (s *StreamStore) GroupCreate(ctx context.Context, key, group, id string, mkStream bool) error {
	args := []any{"XGROUP", "CREATE", key, group, id}
	if mkStream {
		args = append(args, "MKSTREAM")
	}
	_, err := s.client.Command(ctx, args...)
	return err
}

type StreamReadGroupOptions struct {
	Group    string
	Consumer string
	Count    *int
	BlockMS  *int64
	Streams  []StreamRef
}

func (s *StreamStore) ReadGroup(ctx context.Context, opt StreamReadGroupOptions) (any, error) {
	args := []any{"XREADGROUP", "GROUP", opt.Group, opt.Consumer}
	appendIntPtr(&args, "COUNT", opt.Count)
	appendInt64Ptr(&args, "BLOCK", opt.BlockMS)
	args = append(args, "STREAMS")
	for _, stream := range opt.Streams {
		args = append(args, stream.Key)
	}
	for _, stream := range opt.Streams {
		args = append(args, stream.ID)
	}
	return s.client.Command(ctx, args...)
}

func (s *StreamStore) Ack(ctx context.Context, key, group string, ids ...string) (int64, error) {
	args := []any{"XACK", key, group}
	for _, id := range ids {
		args = append(args, id)
	}
	value, err := s.client.Command(ctx, args...)
	return asInt64(value), err
}

func (s *BitmapStore) Pos(ctx context.Context, key string, bit int, start, end *int64) (int64, error) {
	args := []any{"BITPOS", key, bit}
	if start != nil {
		args = append(args, *start)
	}
	if end != nil {
		args = append(args, *end)
	}
	value, err := s.client.Command(ctx, args...)
	return asInt64(value), err
}

func (s *BitmapStore) Op(ctx context.Context, operation, destination string, keys ...string) (int64, error) {
	args := []any{"BITOP", operation, destination}
	for _, key := range keys {
		args = append(args, key)
	}
	value, err := s.client.Command(ctx, args...)
	return asInt64(value), err
}

func (s *HyperLogLogStore) Merge(ctx context.Context, destination string, sources ...string) error {
	args := []any{"PFMERGE", destination}
	for _, source := range sources {
		args = append(args, source)
	}
	_, err := s.client.Command(ctx, args...)
	return err
}

func (s *GeoStore) Pos(ctx context.Context, key string, members ...any) (any, error) {
	args := []any{"GEOPOS", key}
	for _, member := range members {
		encoded, err := s.client.encode(member)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	return s.client.Command(ctx, args...)
}

func (s *GeoStore) Hash(ctx context.Context, key string, members ...any) ([]string, error) {
	args := []any{"GEOHASH", key}
	for _, member := range members {
		encoded, err := s.client.encode(member)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.Command(ctx, args...)
	return stringArray(value, err)
}

type GeoSearchOptions struct {
	FromMember any
	FromLonLat *GeoCoordinate
	ByRadius   *GeoRadius
	ByBox      *GeoBox
	Asc        bool
	Desc       bool
	Count      *int
	Any        bool
	WithCoord  bool
	WithDist   bool
	WithHash   bool
}

type GeoCoordinate struct {
	Longitude float64
	Latitude  float64
}

type GeoRadius struct {
	Radius float64
	Unit   string
}

type GeoBox struct {
	Width  float64
	Height float64
	Unit   string
}

func (s *GeoStore) Search(ctx context.Context, key string, opt GeoSearchOptions) (any, error) {
	args, err := s.geoSearchArgs("GEOSEARCH", key, opt)
	if err != nil {
		return nil, err
	}
	return s.client.Command(ctx, args...)
}

func (s *GeoStore) SearchStore(ctx context.Context, destination, source string, opt GeoSearchOptions, storeDist bool) (int64, error) {
	args, err := s.geoSearchArgs("GEOSEARCHSTORE", destination, opt)
	if err != nil {
		return 0, err
	}
	args = append(args[:2], append([]any{source}, args[2:]...)...)
	if storeDist {
		args = append(args, "STOREDIST")
	}
	value, err := s.client.Command(ctx, args...)
	return asInt64(value), err
}

func (s *GeoStore) geoSearchArgs(command, key string, opt GeoSearchOptions) ([]any, error) {
	args := []any{command, key}
	if opt.FromMember != nil {
		encoded, err := s.client.encode(opt.FromMember)
		if err != nil {
			return nil, err
		}
		args = append(args, "FROMMEMBER", encoded)
	}
	if opt.FromLonLat != nil {
		args = append(args, "FROMLONLAT", opt.FromLonLat.Longitude, opt.FromLonLat.Latitude)
	}
	if opt.ByRadius != nil {
		args = append(args, "BYRADIUS", opt.ByRadius.Radius, opt.ByRadius.Unit)
	}
	if opt.ByBox != nil {
		args = append(args, "BYBOX", opt.ByBox.Width, opt.ByBox.Height, opt.ByBox.Unit)
	}
	if opt.Asc {
		args = append(args, "ASC")
	}
	if opt.Desc {
		args = append(args, "DESC")
	}
	if opt.Count != nil {
		args = append(args, "COUNT", *opt.Count)
		if opt.Any {
			args = append(args, "ANY")
		}
	}
	if opt.WithCoord {
		args = append(args, "WITHCOORD")
	}
	if opt.WithDist {
		args = append(args, "WITHDIST")
	}
	if opt.WithHash {
		args = append(args, "WITHHASH")
	}
	return args, nil
}

func boolArray(value any, err error) ([]bool, error) {
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		return nil, nil
	}
	out := make([]bool, 0, len(items))
	for _, item := range items {
		out = append(out, asBool(item))
	}
	return out, nil
}

func stringArray(value any, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		if value == nil {
			return nil, nil
		}
		return []string{asString(value)}, nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, asString(item))
	}
	return out, nil
}

func intArray(value any, err error) ([]int64, error) {
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		if value == nil {
			return nil, nil
		}
		return []int64{asInt64(value)}, nil
	}
	out := make([]int64, 0, len(items))
	for _, item := range items {
		out = append(out, asInt64(item))
	}
	return out, nil
}

func floatArray(value any, err error) ([]float64, error) {
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		if value == nil {
			return nil, nil
		}
		return []float64{asFloat64(value)}, nil
	}
	out := make([]float64, 0, len(items))
	for _, item := range items {
		out = append(out, asFloat64(item))
	}
	return out, nil
}

func floatArg(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
