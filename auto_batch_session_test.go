package ferricstore

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type autoBatchSessionProvider struct {
	directCalls atomic.Int64
	session     *autoBatchCommandSession
}

func (p *autoBatchSessionProvider) Do(context.Context, ...any) (any, error) {
	p.directCalls.Add(1)
	return []byte("OK"), nil
}

func (p *autoBatchSessionProvider) acquireCommandSession(context.Context, ...any) (commandSession, error) {
	return p.session, nil
}

type autoBatchCommandSession struct{}

func (*autoBatchCommandSession) Do(_ context.Context, args ...any) (any, error) {
	switch asString(args[0]) {
	case "MULTI", "DISCARD":
		return []byte("OK"), nil
	default:
		return []byte("QUEUED"), nil
	}
}

func (*autoBatchCommandSession) Abort(error) {}
func (*autoBatchCommandSession) Release()    {}

func TestAutoBatchUsesActiveLegacySession(t *testing.T) {
	provider := &autoBatchSessionProvider{session: &autoBatchCommandSession{}}
	base := NewClientWithExecutor(provider)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := base.Multi(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = base.Discard(context.Background()) }()

	auto := NewAutoBatchExecutor(base, AutoBatchOptions{MaxSize: 1, FlushInterval: time.Hour})
	defer func() { _ = auto.Close() }()
	value, err := auto.Do(ctx, "SET", "key", "value")
	if err != nil {
		t.Fatal(err)
	}
	if got := asString(value); got != "QUEUED" {
		t.Fatalf("AutoBatch response = %q; want active session response QUEUED", got)
	}
	if calls := provider.directCalls.Load(); calls != 0 {
		t.Fatalf("AutoBatch bypassed active session with %d direct executor call(s)", calls)
	}
}
