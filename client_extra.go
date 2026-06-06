package ferricstore

import "context"

func (c *Client) Type(ctx context.Context, key string) (string, error) {
	value, err := c.Command(ctx, "TYPE", key)
	return asString(value), err
}

func (c *Client) RandomKey(ctx context.Context) (string, error) {
	value, err := c.Command(ctx, "RANDOMKEY")
	return asString(value), err
}

func (c *Client) Scan(ctx context.Context, cursor int64, match string, count *int) (any, error) {
	args := []any{"SCAN", cursor}
	if match != "" {
		args = append(args, "MATCH", match)
	}
	appendIntPtr(&args, "COUNT", count)
	return c.Command(ctx, args...)
}

func (c *Client) DBSize(ctx context.Context) (int64, error) {
	value, err := c.Command(ctx, "DBSIZE")
	return asInt64(value), err
}

func (c *Client) Keys(ctx context.Context, pattern string) ([]string, error) {
	value, err := c.Command(ctx, "KEYS", pattern)
	return stringArray(value, err)
}

func (c *Client) Unlink(ctx context.Context, keys ...string) (int64, error) {
	return c.keyWrite(ctx, "UNLINK", keys...)
}

func (c *Client) Delete(ctx context.Context, keys ...string) (int64, error) {
	return c.keyWrite(ctx, "DEL", keys...)
}

func (c *Client) keyWrite(ctx context.Context, command string, keys ...string) (int64, error) {
	args := []any{command}
	for _, key := range keys {
		args = append(args, key)
	}
	value, err := c.Command(ctx, args...)
	return asInt64(value), err
}

func (c *Client) Rename(ctx context.Context, key, newKey string) error {
	_, err := c.Command(ctx, "RENAME", key, newKey)
	return err
}

func (c *Client) RenameNX(ctx context.Context, key, newKey string) (bool, error) {
	value, err := c.Command(ctx, "RENAMENX", key, newKey)
	return asBool(value), err
}

func (c *Client) Copy(ctx context.Context, source, destination string, replace bool) (bool, error) {
	args := []any{"COPY", source, destination}
	if replace {
		args = append(args, "REPLACE")
	}
	value, err := c.Command(ctx, args...)
	return asBool(value), err
}

func (c *Client) Ping(ctx context.Context, message ...string) (string, error) {
	args := []any{"PING"}
	if len(message) > 0 {
		args = append(args, message[0])
	}
	value, err := c.Command(ctx, args...)
	return asString(value), err
}

func (c *Client) Echo(ctx context.Context, message string) (string, error) {
	value, err := c.Command(ctx, "ECHO", message)
	return asString(value), err
}

func (c *Client) ServerInfo(ctx context.Context, section ...string) (map[string]any, error) {
	args := []any{"INFO"}
	if len(section) > 0 && section[0] != "" {
		args = append(args, section[0])
	}
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	return kvResponse(value)
}

func (c *Client) FlushDB(ctx context.Context, mode string) error {
	args := []any{"FLUSHDB"}
	if mode != "" {
		args = append(args, mode)
	}
	_, err := c.Command(ctx, args...)
	return err
}

func (c *Client) FlushAll(ctx context.Context, mode string) error {
	args := []any{"FLUSHALL"}
	if mode != "" {
		args = append(args, mode)
	}
	_, err := c.Command(ctx, args...)
	return err
}

func (c *Client) CommandInfo(ctx context.Context, names ...string) (any, error) {
	args := []any{"COMMAND", "INFO"}
	for _, name := range names {
		args = append(args, name)
	}
	return c.Command(ctx, args...)
}

func (c *Client) CommandCount(ctx context.Context) (int64, error) {
	value, err := c.Command(ctx, "COMMAND", "COUNT")
	return asInt64(value), err
}

func (c *Client) CommandList(ctx context.Context) ([]string, error) {
	value, err := c.Command(ctx, "COMMAND", "LIST")
	return stringArray(value, err)
}

func (c *Client) CommandDocs(ctx context.Context, names ...string) (any, error) {
	args := []any{"COMMAND", "DOCS"}
	for _, name := range names {
		args = append(args, name)
	}
	return c.Command(ctx, args...)
}

func (c *Client) CommandGetKeys(ctx context.Context, command ...any) (any, error) {
	args := []any{"COMMAND", "GETKEYS"}
	args = append(args, command...)
	return c.Command(ctx, args...)
}

func (c *Client) ConfigGet(ctx context.Context, pattern string) (any, error) {
	return c.Command(ctx, "CONFIG", "GET", pattern)
}

func (c *Client) ConfigSet(ctx context.Context, parameter, value string) error {
	_, err := c.Command(ctx, "CONFIG", "SET", parameter, value)
	return err
}

func (c *Client) ConfigResetStat(ctx context.Context) error {
	_, err := c.Command(ctx, "CONFIG", "RESETSTAT")
	return err
}

func (c *Client) ConfigRewrite(ctx context.Context) error {
	_, err := c.Command(ctx, "CONFIG", "REWRITE")
	return err
}

func (c *Client) SlowLogGet(ctx context.Context, count *int) (any, error) {
	args := []any{"SLOWLOG", "GET"}
	if count != nil {
		args = append(args, *count)
	}
	return c.Command(ctx, args...)
}

func (c *Client) SlowLogLen(ctx context.Context) (int64, error) {
	value, err := c.Command(ctx, "SLOWLOG", "LEN")
	return asInt64(value), err
}

func (c *Client) SlowLogReset(ctx context.Context) error {
	_, err := c.Command(ctx, "SLOWLOG", "RESET")
	return err
}

func (c *Client) Select(ctx context.Context, db int) error {
	_, err := c.Command(ctx, "SELECT", db)
	return err
}

func (c *Client) Wait(ctx context.Context, replicas, timeoutMS int64) (int64, error) {
	value, err := c.Command(ctx, "WAIT", replicas, timeoutMS)
	return asInt64(value), err
}

func (c *Client) WaitAOF(ctx context.Context, local, replicas, timeoutMS int64) (any, error) {
	return c.Command(ctx, "WAITAOF", local, replicas, timeoutMS)
}

func (c *Client) Object(ctx context.Context, args ...any) (any, error) {
	command := append([]any{"OBJECT"}, args...)
	return c.Command(ctx, command...)
}

func (c *Client) ObjectRefCount(ctx context.Context, key string) (int64, error) {
	value, err := c.Command(ctx, "OBJECT", "REFCOUNT", key)
	return asInt64(value), err
}

func (c *Client) ObjectHelp(ctx context.Context) (any, error) {
	return c.Command(ctx, "OBJECT", "HELP")
}

func (c *Client) Publish(ctx context.Context, channel, message string) (int64, error) {
	value, err := c.Command(ctx, "PUBLISH", channel, message)
	return asInt64(value), err
}

func (c *Client) PubSubChannels(ctx context.Context, pattern string) ([]string, error) {
	args := []any{"PUBSUB", "CHANNELS"}
	if pattern != "" {
		args = append(args, pattern)
	}
	value, err := c.Command(ctx, args...)
	return stringArray(value, err)
}

func (c *Client) PubSubNumSub(ctx context.Context, channels ...string) (map[string]int64, error) {
	args := []any{"PUBSUB", "NUMSUB"}
	for _, channel := range channels {
		args = append(args, channel)
	}
	value, err := c.Command(ctx, args...)
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		return nil, nil
	}
	out := make(map[string]int64, len(items)/2)
	for i := 0; i+1 < len(items); i += 2 {
		out[asString(items[i])] = asInt64(items[i+1])
	}
	return out, nil
}

func (c *Client) PubSubNumPat(ctx context.Context) (int64, error) {
	value, err := c.Command(ctx, "PUBSUB", "NUMPAT")
	return asInt64(value), err
}

func (c *Client) Save(ctx context.Context) error {
	_, err := c.Command(ctx, "SAVE")
	return err
}

func (c *Client) BgSave(ctx context.Context) error {
	_, err := c.Command(ctx, "BGSAVE")
	return err
}

func (c *Client) LastSave(ctx context.Context) (int64, error) {
	value, err := c.Command(ctx, "LASTSAVE")
	return asInt64(value), err
}

func (c *Client) Memory(ctx context.Context, args ...any) (any, error) {
	command := append([]any{"MEMORY"}, args...)
	return c.Command(ctx, command...)
}

func (c *Client) Module(ctx context.Context, args ...any) (any, error) {
	command := append([]any{"MODULE"}, args...)
	return c.Command(ctx, command...)
}

func (c *Client) FerricStoreDoctor(ctx context.Context, args ...any) (any, error) {
	command := append([]any{"FERRICSTORE.DOCTOR"}, args...)
	return c.Command(ctx, command...)
}
