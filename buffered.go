package ferricstore

import "context"

type BufferedExecutor struct {
	client       *Client
	commands     [][]any
	Flushes      int64
	CommandsSent int64
	MaxDepth     int64
}

func NewBufferedExecutor(client *Client) *BufferedExecutor {
	return &BufferedExecutor{client: client}
}

func (e *BufferedExecutor) Do(ctx context.Context, args ...any) (any, error) {
	copied := append([]any(nil), args...)
	e.commands = append(e.commands, copied)
	return []byte("QUEUED"), nil
}

func (e *BufferedExecutor) Flush(ctx context.Context) ([]any, error) {
	if len(e.commands) == 0 {
		return nil, nil
	}
	commands := e.commands
	e.commands = nil
	depth := int64(len(commands))
	e.Flushes++
	e.CommandsSent += depth
	if depth > e.MaxDepth {
		e.MaxDepth = depth
	}
	if e.client == nil {
		return nil, nil
	}
	return e.client.Pipeline(ctx, commands)
}
