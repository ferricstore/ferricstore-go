package ferricstore

import (
	"context"
	"errors"
	"fmt"
)

func (c *Client) SlowLogGet(ctx context.Context, count *int) (any, error) {
	if count != nil && *count < 0 {
		return nil, errors.New("SLOWLOG GET count must be non-negative")
	}
	args := []any{"SLOWLOG", "GET"}
	if count != nil {
		args = append(args, *count)
	}
	return c.typedReply(ctx, args...)
}

func (c *Client) SlowLogLen(ctx context.Context) (int64, error) {
	value, err := c.typedReply(ctx, "SLOWLOG", "LEN")
	return nonNegativeInt64Response("SLOWLOG LEN", value, err)
}

func (c *Client) SlowLogReset(ctx context.Context) error {
	return c.typedStatus(ctx, "SLOWLOG", "RESET")
}

func (c *Client) Select(ctx context.Context, db int) error {
	if db < 0 {
		return errors.New("database index must be non-negative")
	}
	return c.typedStatus(ctx, "SELECT", db)
}

func (c *Client) Wait(ctx context.Context, replicas, timeoutMS int64) (int64, error) {
	if replicas < 0 {
		return 0, errors.New("WAIT replicas must be non-negative")
	}
	if timeoutMS < 0 {
		return 0, errors.New("WAIT timeout milliseconds must be non-negative")
	}
	value, err := c.typedReply(ctx, "WAIT", replicas, timeoutMS)
	return nonNegativeInt64Response("WAIT", value, err)
}

func (c *Client) WaitAOF(ctx context.Context, local, replicas, timeoutMS int64) (any, error) {
	if local < 0 {
		return nil, errors.New("WAITAOF local acknowledgements must be non-negative")
	}
	if replicas < 0 {
		return nil, errors.New("WAITAOF replicas must be non-negative")
	}
	if timeoutMS < 0 {
		return nil, errors.New("WAITAOF timeout milliseconds must be non-negative")
	}
	value, err := c.typedReply(ctx, "WAITAOF", local, replicas, timeoutMS)
	return waitAOFResponse(value, err)
}

func (c *Client) Object(ctx context.Context, args ...any) (any, error) {
	command := append([]any{"OBJECT"}, args...)
	return c.typedReply(ctx, command...)
}

func (c *Client) ObjectRefCount(ctx context.Context, key string) (int64, error) {
	value, err := c.typedReply(ctx, "OBJECT", "REFCOUNT", key)
	return nonNegativeInt64Response("OBJECT REFCOUNT", value, err)
}

func (c *Client) ObjectHelp(ctx context.Context) (any, error) {
	return c.typedReply(ctx, "OBJECT", "HELP")
}

func (c *Client) Publish(ctx context.Context, channel, message string) (int64, error) {
	value, err := c.typedReply(ctx, "PUBLISH", channel, message)
	return nonNegativeInt64Response("PUBLISH", value, err)
}

func (c *Client) Subscribe(ctx context.Context, channels ...string) (any, error) {
	if len(channels) == 0 {
		return nil, errors.New("SUBSCRIBE requires at least one channel")
	}
	args := []any{"SUBSCRIBE"}
	for _, channel := range channels {
		args = append(args, channel)
	}
	return c.typedReply(ctx, args...)
}

func (c *Client) Unsubscribe(ctx context.Context, channels ...string) (any, error) {
	args := []any{"UNSUBSCRIBE"}
	for _, channel := range channels {
		args = append(args, channel)
	}
	return c.typedReply(ctx, args...)
}

func (c *Client) PSubscribe(ctx context.Context, patterns ...string) (any, error) {
	if len(patterns) == 0 {
		return nil, errors.New("PSUBSCRIBE requires at least one pattern")
	}
	args := []any{"PSUBSCRIBE"}
	for _, pattern := range patterns {
		args = append(args, pattern)
	}
	return c.typedReply(ctx, args...)
}

func (c *Client) PUnsubscribe(ctx context.Context, patterns ...string) (any, error) {
	args := []any{"PUNSUBSCRIBE"}
	for _, pattern := range patterns {
		args = append(args, pattern)
	}
	return c.typedReply(ctx, args...)
}

func (c *Client) PubSubChannels(ctx context.Context, pattern string) ([]string, error) {
	args := []any{"PUBSUB", "CHANNELS"}
	if pattern != "" {
		args = append(args, pattern)
	}
	value, err := c.typedReply(ctx, args...)
	return stringArray(value, err)
}

func (c *Client) PubSubNumSub(ctx context.Context, channels ...string) (map[string]int64, error) {
	args := []any{"PUBSUB", "NUMSUB"}
	for _, channel := range channels {
		args = append(args, channel)
	}
	value, err := c.typedReply(ctx, args...)
	if err != nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected PUBSUB NUMSUB response array, got %T", value)
	}
	if len(items)%2 != 0 {
		return nil, fmt.Errorf("expected PUBSUB NUMSUB channel/count pairs, got %d items", len(items))
	}
	out := make(map[string]int64, len(items)/2)
	for i := 0; i < len(items); i += 2 {
		if items[i] == nil {
			return nil, fmt.Errorf("invalid PUBSUB NUMSUB channel at pair %d: response is nil", i/2)
		}
		channel, err := responseString(items[i], nil)
		if err != nil {
			return nil, fmt.Errorf("invalid PUBSUB NUMSUB channel at pair %d: %w", i/2, err)
		}
		count, err := nonNegativeInt64Response("PUBSUB NUMSUB", items[i+1], nil)
		if err != nil {
			return nil, fmt.Errorf("invalid PUBSUB NUMSUB count for %q: %w", channel, err)
		}
		if _, exists := out[channel]; exists {
			return nil, fmt.Errorf("duplicate PUBSUB NUMSUB channel %q", channel)
		}
		out[channel] = count
	}
	return out, nil
}

func (c *Client) PubSubNumPat(ctx context.Context) (int64, error) {
	value, err := c.typedReply(ctx, "PUBSUB", "NUMPAT")
	return nonNegativeInt64Response("PUBSUB NUMPAT", value, err)
}

func (c *Client) Save(ctx context.Context) error {
	return c.typedStatus(ctx, "SAVE")
}

func (c *Client) BgSave(ctx context.Context) error {
	return c.typedExpectedStatus(ctx, "Background saving started", "BGSAVE")
}

func (c *Client) LastSave(ctx context.Context) (int64, error) {
	value, err := c.typedReply(ctx, "LASTSAVE")
	return nonNegativeInt64Response("LASTSAVE", value, err)
}

func (c *Client) Memory(ctx context.Context, args ...any) (any, error) {
	command := append([]any{"MEMORY"}, args...)
	return c.typedReply(ctx, command...)
}

func (c *Client) Module(ctx context.Context, args ...any) (any, error) {
	command := append([]any{"MODULE"}, args...)
	return c.typedReply(ctx, command...)
}

func (c *Client) FerricStoreDoctor(ctx context.Context, args ...any) (any, error) {
	command := append([]any{"FERRICSTORE.DOCTOR"}, args...)
	return c.typedReply(ctx, command...)
}
