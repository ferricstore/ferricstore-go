package ferricstore

import (
	"context"
	"fmt"
)

type KeyValueStore struct{ client *Client }

// keyValueBulkExecutor lets the built-in transports consume typed key slices
// directly. The generic Executor path remains available for custom transports.
type keyValueBulkExecutor interface {
	keyValueMGet(ctx context.Context, keys []string) (any, error)
	keyValueMSet(ctx context.Context, keys []string, values []any) (any, error)
}

// keyValueMSetNXExecutor avoids boxing every string key into an interface on
// transports that can retain typed key slices through wire encoding.
type keyValueMSetNXExecutor interface {
	keyValueMSetNX(ctx context.Context, keys []string, values []any) (any, error)
}

type keyValueDelExecutor interface {
	keyValueDel(ctx context.Context, keys []string) (any, error)
}

type keyValueExistsExecutor interface {
	keyValueExists(ctx context.Context, keys []string) (any, error)
}

func (s *KeyValueStore) Set(ctx context.Context, key string, value any) error {
	encoded, err := s.client.encode(value)
	if err != nil {
		return err
	}
	return s.commandStatus(ctx, "SET", key, encoded)
}

func (s *KeyValueStore) Get(ctx context.Context, key string) (any, error) {
	value, err := s.commandReply(ctx, "GET", key)
	if err != nil || value == nil {
		return value, err
	}
	return s.client.codec.Decode(value)
}

func (s *KeyValueStore) MGet(ctx context.Context, keys ...string) ([]any, error) {
	if len(keys) == 0 {
		return []any{}, nil
	}
	var value any
	var err error
	if bulk, ok := s.client.exec.(keyValueBulkExecutor); ok {
		value, err = s.commandReplyDirect(ctx, func() (any, error) {
			return bulk.keyValueMGet(ctx, keys)
		}, func() []any {
			args := make([]any, 1, len(keys)+1)
			args[0] = "MGET"
			for _, key := range keys {
				args = append(args, key)
			}
			return args
		})
	} else {
		args := make([]any, 1, len(keys)+1)
		args[0] = "MGET"
		for _, key := range keys {
			args = append(args, key)
		}
		value, err = s.commandReply(ctx, args...)
	}
	if err != nil {
		return nil, err
	}
	items, err := exactArrayItems(value, nil, len(keys), "MGET")
	if err != nil {
		return nil, err
	}
	out := make([]any, 0, len(items))
	for _, item := range items {
		decoded, err := decodeValue(s.client.codec, item)
		if err != nil {
			return nil, err
		}
		out = append(out, decoded)
	}
	return out, nil
}

func (s *KeyValueStore) Del(ctx context.Context, keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	if direct, ok := s.client.exec.(keyValueDelExecutor); ok {
		value, err := s.commandReplyDirect(ctx, func() (any, error) {
			return direct.keyValueDel(ctx, keys)
		}, func() []any {
			return keyListCommandArgs("DEL", keys)
		})
		return keyValueCountResponse("DEL", len(keys), value, err)
	}
	args := keyListCommandArgs("DEL", keys)
	value, err := s.commandReply(ctx, args...)
	return keyValueCountResponse("DEL", len(keys), value, err)
}

func (s *KeyValueStore) Exists(ctx context.Context, keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	if direct, ok := s.client.exec.(keyValueExistsExecutor); ok {
		value, err := s.commandReplyDirect(ctx, func() (any, error) {
			return direct.keyValueExists(ctx, keys)
		}, func() []any {
			return keyListCommandArgs("EXISTS", keys)
		})
		return keyValueCountResponse("EXISTS", len(keys), value, err)
	}
	args := keyListCommandArgs("EXISTS", keys)
	value, err := s.commandReply(ctx, args...)
	return keyValueCountResponse("EXISTS", len(keys), value, err)
}

func keyListCommandArgs(command string, keys []string) []any {
	args := make([]any, 1, len(keys)+1)
	args[0] = command
	for _, key := range keys {
		args = append(args, key)
	}
	return args
}

func (s *KeyValueStore) Incr(ctx context.Context, key string) (int64, error) {
	value, err := s.commandReply(ctx, "INCR", key)
	return responseInt64(value, err)
}

func (s *KeyValueStore) Decr(ctx context.Context, key string) (int64, error) {
	value, err := s.commandReply(ctx, "DECR", key)
	return responseInt64(value, err)
}

func (s *KeyValueStore) Expire(ctx context.Context, key string, seconds int64) (bool, error) {
	value, err := s.commandReply(ctx, "EXPIRE", key, seconds)
	return responseBool(value, err)
}

func (s *KeyValueStore) TTL(ctx context.Context, key string) (int64, error) {
	value, err := s.commandReply(ctx, "TTL", key)
	return keyValueTTLResponse("TTL", value, err)
}

type HashStore struct{ client *Client }

func (s *HashStore) Set(ctx context.Context, key, field string, value any) (int64, error) {
	encoded, err := s.client.encode(value)
	if err != nil {
		return 0, err
	}
	response, err := s.client.typedReply(ctx, "HSET", key, field, encoded)
	return boundedCountResponse("HSET", 1, response, err)
}

func (s *HashStore) Get(ctx context.Context, key, field string) (any, error) {
	value, err := s.client.typedReply(ctx, "HGET", key, field)
	if err != nil || value == nil {
		return value, err
	}
	return s.client.codec.Decode(value)
}

func (s *HashStore) GetAll(ctx context.Context, key string) (map[string]any, error) {
	value, err := s.client.typedReply(ctx, "HGETALL", key)
	if err != nil {
		return nil, err
	}
	raw, err := nativeMap(value)
	if err != nil {
		return nil, err
	}
	out := make(map[string]any, len(raw))
	for field, item := range raw {
		decoded, err := s.client.codec.Decode(item)
		if err != nil {
			return nil, err
		}
		out[field] = decoded
	}
	return out, nil
}

func (s *HashStore) Del(ctx context.Context, key string, fields ...string) (int64, error) {
	if len(fields) == 0 {
		return 0, nil
	}
	args := make([]any, 2, len(fields)+2)
	args[0], args[1] = "HDEL", key
	for _, field := range fields {
		args = append(args, field)
	}
	value, err := s.client.typedReply(ctx, args...)
	return boundedCountResponse("HDEL", len(fields), value, err)
}

type ListStore struct{ client *Client }

func (s *ListStore) LPush(ctx context.Context, key string, values ...any) (int64, error) {
	return s.push(ctx, "LPUSH", key, values...)
}

func (s *ListStore) RPush(ctx context.Context, key string, values ...any) (int64, error) {
	return s.push(ctx, "RPUSH", key, values...)
}

func (s *ListStore) push(ctx context.Context, command, key string, values ...any) (int64, error) {
	if len(values) == 0 {
		return 0, nil
	}
	args := []any{command, key}
	for _, value := range values {
		encoded, err := s.client.encode(value)
		if err != nil {
			return 0, err
		}
		args = append(args, encoded)
	}
	response, err := s.client.typedReply(ctx, args...)
	return nonNegativeInt64Response(command, response, err)
}

func (s *ListStore) LPop(ctx context.Context, key string) (any, error) {
	return s.pop(ctx, "LPOP", key)
}

func (s *ListStore) RPop(ctx context.Context, key string) (any, error) {
	return s.pop(ctx, "RPOP", key)
}

func (s *ListStore) pop(ctx context.Context, command, key string) (any, error) {
	value, err := s.client.typedReply(ctx, command, key)
	if err != nil || value == nil {
		return value, err
	}
	return s.client.codec.Decode(value)
}

func (s *ListStore) Range(ctx context.Context, key string, start, stop int64) ([]any, error) {
	value, err := s.client.typedReply(ctx, "LRANGE", key, start, stop)
	return decodeArray(s.client.codec, value, err)
}

type SetStore struct{ client *Client }

func (s *SetStore) Add(ctx context.Context, key string, members ...any) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	args := make([]any, 2, len(members)+2)
	args[0], args[1] = "SADD", key
	for _, member := range members {
		encoded, err := s.client.encode(member)
		if err != nil {
			return 0, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.typedReply(ctx, args...)
	return boundedCountResponse("SADD", len(members), value, err)
}

func (s *SetStore) Members(ctx context.Context, key string) ([]any, error) {
	value, err := s.client.typedReply(ctx, "SMEMBERS", key)
	return decodeArray(s.client.codec, value, err)
}

func (s *SetStore) Remove(ctx context.Context, key string, members ...any) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	args := make([]any, 2, len(members)+2)
	args[0], args[1] = "SREM", key
	for _, member := range members {
		encoded, err := s.client.encode(member)
		if err != nil {
			return 0, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.typedReply(ctx, args...)
	return boundedCountResponse("SREM", len(members), value, err)
}

type ZAddMember struct {
	Score  float64
	Member any
}

type SortedSetStore struct{ client *Client }

func (s *SortedSetStore) Add(ctx context.Context, key string, members ...ZAddMember) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	if err := validateZAddMembers(members); err != nil {
		return 0, err
	}
	args := make([]any, 2, len(members)*2+2)
	args[0], args[1] = "ZADD", key
	for _, member := range members {
		encoded, err := s.client.encode(member.Member)
		if err != nil {
			return 0, err
		}
		args = append(args, member.Score, encoded)
	}
	value, err := s.client.typedReply(ctx, args...)
	return boundedCountResponse("ZADD", len(members), value, err)
}

func (s *SortedSetStore) Range(ctx context.Context, key string, start, stop int64) ([]any, error) {
	value, err := s.client.typedReply(ctx, "ZRANGE", key, start, stop)
	return decodeArray(s.client.codec, value, err)
}

type StreamStore struct{ client *Client }

func (s *StreamStore) Add(ctx context.Context, key, id string, fields map[string]any) (string, error) {
	if len(fields) == 0 {
		return "", fmt.Errorf("XADD requires at least one field/value pair")
	}
	if id == "" {
		id = "*"
	}
	args := []any{"XADD", key, id}
	for _, field := range mapKeysForCodec(fields, s.client.codec) {
		value := fields[field]
		encoded, err := s.client.encode(value)
		if err != nil {
			return "", err
		}
		args = append(args, field, encoded)
	}
	response, err := s.client.typedReply(ctx, args...)
	return responseString(response, err)
}

func (s *StreamStore) Range(ctx context.Context, key, start, stop string, count *int) (any, error) {
	if err := validateNonNegativeCount("XRANGE", count); err != nil {
		return nil, err
	}
	args := []any{"XRANGE", key, start, stop}
	appendIntPtr(&args, "COUNT", count)
	value, err := s.client.typedReply(ctx, args...)
	return decodeStreamEntries(s.client.codec, value, err)
}

type BitmapStore struct{ client *Client }

func (s *BitmapStore) SetBit(ctx context.Context, key string, offset int64, value int) (int64, error) {
	if err := validateBitmapOffset(offset); err != nil {
		return 0, err
	}
	if err := validateBitValue(value); err != nil {
		return 0, err
	}
	response, err := s.client.typedReply(ctx, "SETBIT", key, offset, value)
	return boundedCountResponse("SETBIT", 1, response, err)
}

func (s *BitmapStore) GetBit(ctx context.Context, key string, offset int64) (int64, error) {
	if err := validateBitmapOffset(offset); err != nil {
		return 0, err
	}
	response, err := s.client.typedReply(ctx, "GETBIT", key, offset)
	return boundedCountResponse("GETBIT", 1, response, err)
}

func (s *BitmapStore) Count(ctx context.Context, key string) (int64, error) {
	response, err := s.client.typedReply(ctx, "BITCOUNT", key)
	return nonNegativeInt64Response("BITCOUNT", response, err)
}

type HyperLogLogStore struct{ client *Client }

func (s *HyperLogLogStore) Add(ctx context.Context, key string, elements ...any) (int64, error) {
	if len(elements) == 0 {
		return 0, fmt.Errorf("PFADD requires at least one element")
	}
	args := []any{"PFADD", key}
	for _, element := range elements {
		encoded, err := s.client.encode(element)
		if err != nil {
			return 0, err
		}
		args = append(args, encoded)
	}
	response, err := s.client.typedReply(ctx, args...)
	return boundedCountResponse("PFADD", 1, response, err)
}

func (s *HyperLogLogStore) Count(ctx context.Context, keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, fmt.Errorf("PFCOUNT requires at least one key")
	}
	args := []any{"PFCOUNT"}
	for _, key := range keys {
		args = append(args, key)
	}
	response, err := s.client.typedReply(ctx, args...)
	return nonNegativeInt64Response("PFCOUNT", response, err)
}

type GeoStore struct{ client *Client }

func (s *GeoStore) Add(ctx context.Context, key string, longitude, latitude float64, member any) (int64, error) {
	if err := validateGeoCoordinate(longitude, latitude); err != nil {
		return 0, err
	}
	encoded, err := s.client.encode(member)
	if err != nil {
		return 0, err
	}
	response, err := s.client.typedReply(ctx, "GEOADD", key, longitude, latitude, encoded)
	return boundedCountResponse("GEOADD", 1, response, err)
}

func (s *GeoStore) Distance(ctx context.Context, key string, member1, member2 any, unit string) (any, error) {
	if err := validateGeoUnit(unit, true); err != nil {
		return nil, err
	}
	encoded1, err := s.client.encode(member1)
	if err != nil {
		return nil, err
	}
	encoded2, err := s.client.encode(member2)
	if err != nil {
		return nil, err
	}
	args := []any{"GEODIST", key, encoded1, encoded2}
	if unit != "" {
		args = append(args, unit)
	}
	value, err := s.client.typedReply(ctx, args...)
	return geoDistanceResponse(value, err)
}

type BloomFilterStore struct{ client *Client }

func (s *BloomFilterStore) Reserve(ctx context.Context, key string, errorRate float64, capacity int64) (bool, error) {
	if err := validateUnitInterval("BF.RESERVE", "error rate", errorRate, true); err != nil {
		return false, err
	}
	if err := validatePositiveInt64("BF.RESERVE", "capacity", capacity); err != nil {
		return false, err
	}
	response, err := s.client.typedReply(ctx, "BF.RESERVE", key, errorRate, capacity)
	return responseOK(response, err)
}

func (s *BloomFilterStore) Add(ctx context.Context, key string, element any) (bool, error) {
	encoded, err := s.client.encode(element)
	if err != nil {
		return false, err
	}
	response, err := s.client.typedReply(ctx, "BF.ADD", key, encoded)
	return responseBool(response, err)
}

func (s *BloomFilterStore) Exists(ctx context.Context, key string, element any) (bool, error) {
	encoded, err := s.client.encode(element)
	if err != nil {
		return false, err
	}
	response, err := s.client.typedReply(ctx, "BF.EXISTS", key, encoded)
	return responseBool(response, err)
}

type CuckooFilterStore struct{ client *Client }

func (s *CuckooFilterStore) Reserve(ctx context.Context, key string, capacity int64) (bool, error) {
	if err := validatePositiveInt64("CF.RESERVE", "capacity", capacity); err != nil {
		return false, err
	}
	response, err := s.client.typedReply(ctx, "CF.RESERVE", key, capacity)
	return responseOK(response, err)
}

func (s *CuckooFilterStore) Add(ctx context.Context, key string, element any) (bool, error) {
	encoded, err := s.client.encode(element)
	if err != nil {
		return false, err
	}
	response, err := s.client.typedReply(ctx, "CF.ADD", key, encoded)
	return responseBool(response, err)
}

func (s *CuckooFilterStore) Exists(ctx context.Context, key string, element any) (bool, error) {
	encoded, err := s.client.encode(element)
	if err != nil {
		return false, err
	}
	response, err := s.client.typedReply(ctx, "CF.EXISTS", key, encoded)
	return responseBool(response, err)
}

type CountMinSketchStore struct{ client *Client }

func (s *CountMinSketchStore) InitByDim(ctx context.Context, key string, width, depth int64) (bool, error) {
	if err := validatePositiveInt64("CMS.INITBYDIM", "width", width); err != nil {
		return false, err
	}
	if err := validatePositiveInt64("CMS.INITBYDIM", "depth", depth); err != nil {
		return false, err
	}
	response, err := s.client.typedReply(ctx, "CMS.INITBYDIM", key, width, depth)
	return responseOK(response, err)
}

func (s *CountMinSketchStore) IncrBy(ctx context.Context, key string, item any, count int64) (any, error) {
	if err := validatePositiveInt64("CMS.INCRBY", "count", count); err != nil {
		return nil, err
	}
	encoded, err := s.client.encode(item)
	if err != nil {
		return nil, err
	}
	return s.client.typedReply(ctx, "CMS.INCRBY", key, encoded, count)
}

type TopKStore struct{ client *Client }

func (s *TopKStore) Reserve(ctx context.Context, key string, k int64) (bool, error) {
	if err := validatePositiveInt64("TOPK.RESERVE", "k", k); err != nil {
		return false, err
	}
	response, err := s.client.typedReply(ctx, "TOPK.RESERVE", key, k)
	return responseOK(response, err)
}

func (s *TopKStore) Add(ctx context.Context, key string, items ...any) ([]any, error) {
	if len(items) == 0 {
		return nil, nil
	}
	args := []any{"TOPK.ADD", key}
	for _, item := range items {
		encoded, err := s.client.encode(item)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.typedReply(ctx, args...)
	return decodeArrayExact(s.client.codec, value, err, len(items), "TOPK.ADD")
}

type TDigestStore struct{ client *Client }

func (s *TDigestStore) Create(ctx context.Context, key string, compression *int64) (bool, error) {
	if compression != nil {
		if err := validatePositiveInt64("TDIGEST.CREATE", "compression", *compression); err != nil {
			return false, err
		}
	}
	args := []any{"TDIGEST.CREATE", key}
	appendInt64Ptr(&args, "COMPRESSION", compression)
	response, err := s.client.typedReply(ctx, args...)
	return responseOK(response, err)
}

func (s *TDigestStore) Add(ctx context.Context, key string, values ...float64) (bool, error) {
	if len(values) == 0 {
		return false, fmt.Errorf("TDIGEST.ADD requires at least one value")
	}
	if err := validateFiniteValues("TDIGEST.ADD", values); err != nil {
		return false, err
	}
	args := []any{"TDIGEST.ADD", key}
	for _, value := range values {
		args = append(args, value)
	}
	response, err := s.client.typedReply(ctx, args...)
	return responseOK(response, err)
}

func decodeArray(codec Codec, value any, err error) ([]any, error) {
	return decodeArrayExact(codec, value, err, -1, "array response")
}

func decodeArrayExact(codec Codec, value any, err error, expected int, command string) ([]any, error) {
	if streamCodecIsRaw(codec) {
		return exactArrayItems(value, err, expected, command)
	}
	return decodeArrayExactWithCodec(codec, value, err, expected, command)
}

func decodeArrayExactWithCodec(codec Codec, value any, err error, expected int, command string) ([]any, error) {
	items, err := exactArrayItems(value, err, expected, command)
	if err != nil {
		return nil, err
	}
	out := make([]any, 0, len(items))
	for _, item := range items {
		decoded, err := decodeValue(codec, item)
		if err != nil {
			return nil, err
		}
		out = append(out, decoded)
	}
	return out, nil
}

func exactArrayItems(value any, err error, expected int, command string) ([]any, error) {
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%s returned %T, expected array", command, value)
	}
	if expected >= 0 && len(items) != expected {
		return nil, fmt.Errorf("%s returned %d values, expected %d", command, len(items), expected)
	}
	return items, nil
}
