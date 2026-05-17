package ferricstore

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type BufferedExecutor struct {
	client       *redis.Client
	commands     [][]any
	Flushes      int64
	CommandsSent int64
	MaxDepth     int64
}

func NewBufferedExecutor(client *redis.Client) *BufferedExecutor {
	return &BufferedExecutor{client: client}
}

func (e *BufferedExecutor) Do(ctx context.Context, args ...any) *redis.Cmd {
	copied := append([]any(nil), args...)
	e.commands = append(e.commands, copied)
	cmd := redis.NewCmd(ctx, args...)
	cmd.SetVal([]byte("QUEUED"))
	return cmd
}

func (e *BufferedExecutor) Flush(ctx context.Context) ([]any, error) {
	if len(e.commands) == 0 {
		return nil, nil
	}
	commands := e.commands
	e.commands = nil
	pipe := e.client.Pipeline()
	for _, command := range commands {
		pipe.Do(ctx, command...)
	}
	cmds, err := pipe.Exec(ctx)
	depth := int64(len(commands))
	e.Flushes++
	e.CommandsSent += depth
	if depth > e.MaxDepth {
		e.MaxDepth = depth
	}
	results := make([]any, 0, len(cmds))
	for _, cmd := range cmds {
		results = append(results, cmd.(*redis.Cmd).Val())
	}
	return results, err
}
