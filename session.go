package ferricstore

import (
	"context"
	"errors"
	"net"
	"sync"
)

var errTransactionConnectionLost = errors.New("ferricstore transaction connection was lost")

// sessionGate is a context-aware, writer-preferring reader/writer gate. A
// transaction holds the writer side for its lifetime; ordinary multiplexed
// requests briefly hold the reader side.
type sessionGate struct {
	mu             sync.Mutex
	readers        int
	writer         bool
	waitingWriters int
	changed        chan struct{}
}

func (g *sessionGate) readLock(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for {
		g.mu.Lock()
		if err := ctx.Err(); err != nil {
			g.mu.Unlock()
			return err
		}
		if !g.writer && g.waitingWriters == 0 {
			g.readers++
			g.mu.Unlock()
			return nil
		}
		changed := g.changedLocked()
		g.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (g *sessionGate) tryReadLock() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.writer || g.waitingWriters != 0 {
		return false
	}
	g.readers++
	return true
}

func (g *sessionGate) readUnlock() {
	g.mu.Lock()
	if g.readers > 0 {
		g.readers--
	}
	if g.readers == 0 {
		g.signalLocked()
	}
	g.mu.Unlock()
}

func (g *sessionGate) lock(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.Lock()
	g.waitingWriters++
	g.mu.Unlock()
	registered := true
	defer func() {
		if !registered {
			return
		}
		g.mu.Lock()
		g.waitingWriters--
		g.signalLocked()
		g.mu.Unlock()
	}()
	for {
		g.mu.Lock()
		if err := ctx.Err(); err != nil {
			g.mu.Unlock()
			return err
		}
		if !g.writer && g.readers == 0 {
			g.waitingWriters--
			registered = false
			g.writer = true
			g.mu.Unlock()
			return nil
		}
		changed := g.changedLocked()
		g.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (g *sessionGate) unlock() {
	g.mu.Lock()
	if g.writer {
		g.writer = false
		g.signalLocked()
	}
	g.mu.Unlock()
}

func (g *sessionGate) changedLocked() <-chan struct{} {
	if g.changed == nil {
		g.changed = make(chan struct{})
	}
	return g.changed
}

func (g *sessionGate) signalLocked() {
	if g.changed == nil {
		return
	}
	close(g.changed)
	g.changed = make(chan struct{})
}

type commandSession interface {
	Do(context.Context, ...any) (any, error)
	Abort(error)
	Release()
}

type commandSessionProvider interface {
	acquireCommandSession(context.Context, ...any) (commandSession, error)
}

type executorCommandSession struct {
	exec    Executor
	release func()
	once    sync.Once
	mu      sync.Mutex
	closed  bool
}

func (s *executorCommandSession) Do(ctx context.Context, args ...any) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, net.ErrClosed
	}
	return s.exec.Do(ctx, args...)
}

func (s *executorCommandSession) Abort(_ error) { s.Release() }

func (s *executorCommandSession) Release() {
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		if s.release != nil {
			s.release()
		}
	})
}
