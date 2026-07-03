package ferricstore

import "context"

type KeyValueStore struct{ client *Client }

func (s *KeyValueStore) Set(ctx context.Context, key string, value any) error {
	encoded, err := s.client.encode(value)
	if err != nil {
		return err
	}
	_, err = s.client.Command(ctx, "SET", key, encoded)
	return err
}

func (s *KeyValueStore) Get(ctx context.Context, key string) (any, error) {
	value, err := s.client.Command(ctx, "GET", key)
	if err != nil || value == nil {
		return value, err
	}
	return s.client.codec.Decode(value)
}

func (s *KeyValueStore) MGet(ctx context.Context, keys ...string) ([]any, error) {
	args := []any{"MGET"}
	for _, key := range keys {
		args = append(args, key)
	}
	value, err := s.client.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		return nil, nil
	}
	out := make([]any, 0, len(items))
	for _, item := range items {
		decoded, err := s.client.codec.Decode(item)
		if err != nil {
			return nil, err
		}
		out = append(out, decoded)
	}
	return out, nil
}

func (s *KeyValueStore) Del(ctx context.Context, keys ...string) (int64, error) {
	args := []any{"DEL"}
	for _, key := range keys {
		args = append(args, key)
	}
	value, err := s.client.Command(ctx, args...)
	return asInt64(value), err
}

func (s *KeyValueStore) Exists(ctx context.Context, keys ...string) (int64, error) {
	args := []any{"EXISTS"}
	for _, key := range keys {
		args = append(args, key)
	}
	value, err := s.client.Command(ctx, args...)
	return asInt64(value), err
}

func (s *KeyValueStore) Incr(ctx context.Context, key string) (int64, error) {
	value, err := s.client.Command(ctx, "INCR", key)
	return asInt64(value), err
}

func (s *KeyValueStore) Decr(ctx context.Context, key string) (int64, error) {
	value, err := s.client.Command(ctx, "DECR", key)
	return asInt64(value), err
}

func (s *KeyValueStore) Expire(ctx context.Context, key string, seconds int64) (bool, error) {
	value, err := s.client.Command(ctx, "EXPIRE", key, seconds)
	return asBool(value), err
}

func (s *KeyValueStore) TTL(ctx context.Context, key string) (int64, error) {
	value, err := s.client.Command(ctx, "TTL", key)
	return asInt64(value), err
}

type HashStore struct{ client *Client }

func (s *HashStore) Set(ctx context.Context, key, field string, value any) (int64, error) {
	encoded, err := s.client.encode(value)
	if err != nil {
		return 0, err
	}
	response, err := s.client.Command(ctx, "HSET", key, field, encoded)
	return asInt64(response), err
}

func (s *HashStore) Get(ctx context.Context, key, field string) (any, error) {
	value, err := s.client.Command(ctx, "HGET", key, field)
	if err != nil || value == nil {
		return value, err
	}
	return s.client.codec.Decode(value)
}

func (s *HashStore) GetAll(ctx context.Context, key string) (map[string]any, error) {
	value, err := s.client.Command(ctx, "HGETALL", key)
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
	args := []any{"HDEL", key}
	for _, field := range fields {
		args = append(args, field)
	}
	value, err := s.client.Command(ctx, args...)
	return asInt64(value), err
}

type ListStore struct{ client *Client }

func (s *ListStore) LPush(ctx context.Context, key string, values ...any) (int64, error) {
	return s.push(ctx, "LPUSH", key, values...)
}

func (s *ListStore) RPush(ctx context.Context, key string, values ...any) (int64, error) {
	return s.push(ctx, "RPUSH", key, values...)
}

func (s *ListStore) push(ctx context.Context, command, key string, values ...any) (int64, error) {
	args := []any{command, key}
	for _, value := range values {
		encoded, err := s.client.encode(value)
		if err != nil {
			return 0, err
		}
		args = append(args, encoded)
	}
	response, err := s.client.Command(ctx, args...)
	return asInt64(response), err
}

func (s *ListStore) LPop(ctx context.Context, key string) (any, error) {
	return s.pop(ctx, "LPOP", key)
}

func (s *ListStore) RPop(ctx context.Context, key string) (any, error) {
	return s.pop(ctx, "RPOP", key)
}

func (s *ListStore) pop(ctx context.Context, command, key string) (any, error) {
	value, err := s.client.Command(ctx, command, key)
	if err != nil || value == nil {
		return value, err
	}
	return s.client.codec.Decode(value)
}

func (s *ListStore) Range(ctx context.Context, key string, start, stop int64) ([]any, error) {
	value, err := s.client.Command(ctx, "LRANGE", key, start, stop)
	return decodeArray(s.client.codec, value, err)
}

type SetStore struct{ client *Client }

func (s *SetStore) Add(ctx context.Context, key string, members ...any) (int64, error) {
	args := []any{"SADD", key}
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

func (s *SetStore) Members(ctx context.Context, key string) ([]any, error) {
	value, err := s.client.Command(ctx, "SMEMBERS", key)
	return decodeArray(s.client.codec, value, err)
}

func (s *SetStore) Remove(ctx context.Context, key string, members ...any) (int64, error) {
	args := []any{"SREM", key}
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

type ZAddMember struct {
	Score  float64
	Member any
}

type SortedSetStore struct{ client *Client }

func (s *SortedSetStore) Add(ctx context.Context, key string, members ...ZAddMember) (int64, error) {
	args := []any{"ZADD", key}
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

func (s *SortedSetStore) Range(ctx context.Context, key string, start, stop int64) ([]any, error) {
	value, err := s.client.Command(ctx, "ZRANGE", key, start, stop)
	return decodeArray(s.client.codec, value, err)
}

type StreamStore struct{ client *Client }

func (s *StreamStore) Add(ctx context.Context, key, id string, fields map[string]any) (string, error) {
	if id == "" {
		id = "*"
	}
	args := []any{"XADD", key, id}
	for field, value := range fields {
		encoded, err := s.client.encode(value)
		if err != nil {
			return "", err
		}
		args = append(args, field, encoded)
	}
	response, err := s.client.Command(ctx, args...)
	return asString(response), err
}

func (s *StreamStore) Range(ctx context.Context, key, start, stop string, count *int) (any, error) {
	args := []any{"XRANGE", key, start, stop}
	appendIntPtr(&args, "COUNT", count)
	return s.client.Command(ctx, args...)
}

type BitmapStore struct{ client *Client }

func (s *BitmapStore) SetBit(ctx context.Context, key string, offset int64, value int) (int64, error) {
	response, err := s.client.Command(ctx, "SETBIT", key, offset, value)
	return asInt64(response), err
}

func (s *BitmapStore) GetBit(ctx context.Context, key string, offset int64) (int64, error) {
	response, err := s.client.Command(ctx, "GETBIT", key, offset)
	return asInt64(response), err
}

func (s *BitmapStore) Count(ctx context.Context, key string) (int64, error) {
	response, err := s.client.Command(ctx, "BITCOUNT", key)
	return asInt64(response), err
}

type HyperLogLogStore struct{ client *Client }

func (s *HyperLogLogStore) Add(ctx context.Context, key string, elements ...any) (int64, error) {
	args := []any{"PFADD", key}
	for _, element := range elements {
		encoded, err := s.client.encode(element)
		if err != nil {
			return 0, err
		}
		args = append(args, encoded)
	}
	response, err := s.client.Command(ctx, args...)
	return asInt64(response), err
}

func (s *HyperLogLogStore) Count(ctx context.Context, keys ...string) (int64, error) {
	args := []any{"PFCOUNT"}
	for _, key := range keys {
		args = append(args, key)
	}
	response, err := s.client.Command(ctx, args...)
	return asInt64(response), err
}

type GeoStore struct{ client *Client }

func (s *GeoStore) Add(ctx context.Context, key string, longitude, latitude float64, member any) (int64, error) {
	encoded, err := s.client.encode(member)
	if err != nil {
		return 0, err
	}
	response, err := s.client.Command(ctx, "GEOADD", key, longitude, latitude, encoded)
	return asInt64(response), err
}

func (s *GeoStore) Distance(ctx context.Context, key string, member1, member2 any, unit string) (any, error) {
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
	return s.client.Command(ctx, args...)
}

type BloomFilterStore struct{ client *Client }

func (s *BloomFilterStore) Reserve(ctx context.Context, key string, errorRate float64, capacity int64) (bool, error) {
	response, err := s.client.Command(ctx, "BF.RESERVE", key, errorRate, capacity)
	return isOK(response), err
}

func (s *BloomFilterStore) Add(ctx context.Context, key string, element any) (bool, error) {
	encoded, err := s.client.encode(element)
	if err != nil {
		return false, err
	}
	response, err := s.client.Command(ctx, "BF.ADD", key, encoded)
	return asInt64(response) == 1, err
}

func (s *BloomFilterStore) Exists(ctx context.Context, key string, element any) (bool, error) {
	encoded, err := s.client.encode(element)
	if err != nil {
		return false, err
	}
	response, err := s.client.Command(ctx, "BF.EXISTS", key, encoded)
	return asInt64(response) == 1, err
}

type CuckooFilterStore struct{ client *Client }

func (s *CuckooFilterStore) Reserve(ctx context.Context, key string, capacity int64) (bool, error) {
	response, err := s.client.Command(ctx, "CF.RESERVE", key, capacity)
	return isOK(response), err
}

func (s *CuckooFilterStore) Add(ctx context.Context, key string, element any) (bool, error) {
	encoded, err := s.client.encode(element)
	if err != nil {
		return false, err
	}
	response, err := s.client.Command(ctx, "CF.ADD", key, encoded)
	return asBool(response), err
}

func (s *CuckooFilterStore) Exists(ctx context.Context, key string, element any) (bool, error) {
	encoded, err := s.client.encode(element)
	if err != nil {
		return false, err
	}
	response, err := s.client.Command(ctx, "CF.EXISTS", key, encoded)
	return asBool(response), err
}

type CountMinSketchStore struct{ client *Client }

func (s *CountMinSketchStore) InitByDim(ctx context.Context, key string, width, depth int64) (bool, error) {
	response, err := s.client.Command(ctx, "CMS.INITBYDIM", key, width, depth)
	return isOK(response), err
}

func (s *CountMinSketchStore) IncrBy(ctx context.Context, key string, item any, count int64) (any, error) {
	encoded, err := s.client.encode(item)
	if err != nil {
		return nil, err
	}
	return s.client.Command(ctx, "CMS.INCRBY", key, encoded, count)
}

type TopKStore struct{ client *Client }

func (s *TopKStore) Reserve(ctx context.Context, key string, k int64) (bool, error) {
	response, err := s.client.Command(ctx, "TOPK.RESERVE", key, k)
	return isOK(response), err
}

func (s *TopKStore) Add(ctx context.Context, key string, items ...any) ([]any, error) {
	args := []any{"TOPK.ADD", key}
	for _, item := range items {
		encoded, err := s.client.encode(item)
		if err != nil {
			return nil, err
		}
		args = append(args, encoded)
	}
	value, err := s.client.Command(ctx, args...)
	return decodeArray(s.client.codec, value, err)
}

type TDigestStore struct{ client *Client }

func (s *TDigestStore) Create(ctx context.Context, key string, compression *int64) (bool, error) {
	args := []any{"TDIGEST.CREATE", key}
	appendInt64Ptr(&args, "COMPRESSION", compression)
	response, err := s.client.Command(ctx, args...)
	return isOK(response), err
}

func (s *TDigestStore) Add(ctx context.Context, key string, values ...float64) (bool, error) {
	args := []any{"TDIGEST.ADD", key}
	for _, value := range values {
		args = append(args, value)
	}
	response, err := s.client.Command(ctx, args...)
	return isOK(response), err
}

func decodeArray(codec Codec, value any, err error) ([]any, error) {
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		return nil, nil
	}
	out := make([]any, 0, len(items))
	for _, item := range items {
		decoded, err := codec.Decode(item)
		if err != nil {
			return nil, err
		}
		out = append(out, decoded)
	}
	return out, nil
}
