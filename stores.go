package ferricstore

import "context"

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
