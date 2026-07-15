package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

type Transaction struct {
	session  commandSession
	done     chan struct{}
	once     sync.Once
	opMu     contextMutex
	stateMu  sync.Mutex
	closed   bool
	closeErr error
}

// Watch starts a connection-affine optimistic transaction session. A following
// Transaction call reuses that same physical session.
func (c *Client) Watch(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return errors.New("WATCH requires at least one key")
	}
	if err := c.legacyGate.lock(ctx); err != nil {
		return err
	}
	defer c.legacyGate.unlock()
	args := []any{"WATCH"}
	for _, key := range keys {
		args = append(args, key)
	}
	c.legacyMu.Lock()
	defer c.legacyMu.Unlock()
	if c.legacy != nil {
		return errors.New("a WATCH/MULTI session is already active")
	}
	keyArgs := make([]any, len(keys))
	for index, key := range keys {
		keyArgs[index] = key
	}
	session, err := c.acquireCommandSession(ctx, keyArgs...)
	if err != nil {
		return err
	}
	response, err := session.Do(ctx, args...)
	if err != nil {
		session.Abort(err)
		return err
	}
	if _, err = responseOK(response, nil); err != nil {
		session.Abort(err)
		return err
	}
	c.setLegacySessionLocked(session)
	return nil
}

func (c *Client) Unwatch(ctx context.Context) error {
	if err := c.legacyGate.lock(ctx); err != nil {
		return err
	}
	defer c.legacyGate.unlock()
	c.legacyMu.Lock()
	if c.legacy == nil {
		c.legacyMu.Unlock()
		return errors.New("UNWATCH requires an active WATCH session")
	}
	if c.legacyMulti {
		c.legacyMu.Unlock()
		return errors.New("UNWATCH cannot be used during MULTI")
	}
	session := c.legacy
	c.setLegacySessionLocked(nil)
	c.legacyMu.Unlock()
	response, err := session.Do(ctx, "UNWATCH")
	if err == nil {
		_, err = responseOK(response, nil)
	}
	finishCommandSession(session, err)
	return err
}

// Multi starts a connection-affine legacy transaction session. Prefer
// Transaction, which makes the session lifetime explicit.
func (c *Client) Multi(ctx context.Context) error {
	if err := c.legacyGate.lock(ctx); err != nil {
		return err
	}
	defer c.legacyGate.unlock()
	c.legacyMu.Lock()
	defer c.legacyMu.Unlock()
	if c.legacy != nil && c.legacyMulti {
		return errors.New("MULTI is already active")
	}
	if c.legacy == nil {
		session, err := c.acquireCommandSession(ctx)
		if err != nil {
			return err
		}
		c.setLegacySessionLocked(session)
	}
	response, err := c.legacy.Do(ctx, "MULTI")
	if err != nil {
		c.legacy.Abort(err)
		c.setLegacySessionLocked(nil)
		return err
	}
	if _, err = responseOK(response, nil); err != nil {
		c.legacy.Abort(err)
		c.setLegacySessionLocked(nil)
		return err
	}
	c.legacyMulti = true
	return nil
}

func (c *Client) Exec(ctx context.Context) ([]any, error) {
	if err := c.legacyGate.lock(ctx); err != nil {
		return nil, err
	}
	defer c.legacyGate.unlock()
	c.legacyMu.Lock()
	if c.legacy == nil {
		c.legacyMu.Unlock()
		return nil, errors.New("EXEC requires an active MULTI session")
	}
	if !c.legacyMulti {
		c.legacyMu.Unlock()
		return nil, errors.New("EXEC requires MULTI after WATCH")
	}
	session := c.legacy
	c.setLegacySessionLocked(nil)
	c.legacyMu.Unlock()
	value, err := session.Do(ctx, "EXEC")
	if err != nil {
		session.Abort(err)
		return nil, err
	}
	items, parseErr := transactionArray(value, nil)
	finishCommandSession(session, parseErr)
	return items, parseErr
}

func (c *Client) Discard(ctx context.Context) error {
	if err := c.legacyGate.lock(ctx); err != nil {
		return err
	}
	defer c.legacyGate.unlock()
	c.legacyMu.Lock()
	if c.legacy == nil {
		c.legacyMu.Unlock()
		return errors.New("DISCARD requires an active MULTI session")
	}
	if !c.legacyMulti {
		c.legacyMu.Unlock()
		return errors.New("DISCARD requires MULTI after WATCH")
	}
	session := c.legacy
	c.setLegacySessionLocked(nil)
	c.legacyMu.Unlock()
	response, err := session.Do(ctx, "DISCARD")
	if err == nil {
		_, err = responseOK(response, nil)
	}
	finishCommandSession(session, err)
	return err
}

func (c *Client) currentLegacySession() commandSession {
	session, _ := c.currentLegacySessionState()
	return session
}

func (c *Client) currentLegacySessionState() (commandSession, bool) {
	if !c.legacyActive.Load() {
		return nil, false
	}
	c.legacyMu.Lock()
	defer c.legacyMu.Unlock()
	return c.legacy, c.legacyMulti
}

func (c *Client) setLegacySessionLocked(session commandSession) {
	c.legacy = session
	c.legacyMulti = false
	c.legacyActive.Store(session != nil)
}

func affineCommandArgs(args []any) []any {
	if len(args) == 0 || strings.EqualFold(asString(args[0]), "COMMAND_EXEC") {
		return append([]any(nil), args...)
	}
	out := make([]any, 0, len(args)+1)
	out = append(out, "COMMAND_EXEC", asString(args[0]))
	return append(out, args[1:]...)
}

func connectionStateCommand(args []any) (string, bool) {
	if len(args) == 0 {
		return "", false
	}
	name := strings.ToUpper(asString(args[0]))
	if name == "COMMAND_EXEC" && len(args) > 1 {
		name = strings.ToUpper(asString(args[1]))
	}
	switch name {
	case "WATCH", "UNWATCH", "MULTI", "EXEC", "DISCARD":
		return name, true
	default:
		return name, false
	}
}

func connectionStateMutationCommand(args []any) (string, bool) {
	if len(args) == 0 {
		return "", false
	}
	offset := 0
	name := strings.ToUpper(asString(args[0]))
	if name == "COMMAND_EXEC" && len(args) > 1 {
		offset = 1
		name = strings.ToUpper(asString(args[1]))
	}
	if name == "CLIENT" && len(args) > offset+1 && strings.EqualFold(asString(args[offset+1]), "SETNAME") {
		name = "CLIENT.SETNAME"
	}
	switch name {
	case "AUTH", "CLIENT.SETNAME", "HELLO", "QUIT", "RESET", "SANDBOX", "STARTUP", "SUBSCRIBE_EVENTS", "UNSUBSCRIBE_EVENTS", "WINDOW_UPDATE":
		return name, true
	default:
		return name, false
	}
}

func finishCommandSession(session commandSession, err error) {
	if err != nil {
		session.Abort(err)
		return
	}
	session.Release()
}

func transactionArray(value any, err error) ([]any, error) {
	if err != nil || value == nil {
		return nil, err
	}
	items, ok := value.([]any)
	if !ok {
		return nil, errors.New("expected EXEC array response")
	}
	return items, nil
}

func (c *Client) CommandExec(ctx context.Context, command string, args ...any) (any, error) {
	return c.CommandExecWithContext(ctx, command, nil, args...)
}

func (c *Client) CommandExecWithContext(ctx context.Context, command string, requestContext *RequestContext, args ...any) (any, error) {
	payload := []any{"COMMAND_EXEC", command}
	payload = append(payload, args...)
	if requestContext != nil {
		payload = appendNativeRequestContext(payload, requestContext)
	}
	return c.Command(ctx, payload...)
}

func (c *Client) Transaction(ctx context.Context) (*Transaction, error) {
	return c.transactionForKeys(ctx, nil)
}

// TransactionForKeys starts a transaction pinned to the hash slot containing
// keys. Topology clients require this method unless a preceding Watch already
// established the affine session.
func (c *Client) TransactionForKeys(ctx context.Context, keys ...string) (*Transaction, error) {
	if len(keys) == 0 {
		return nil, errors.New("TransactionForKeys requires at least one key")
	}
	keyArgs := make([]any, len(keys))
	for index, key := range keys {
		keyArgs[index] = key
	}
	return c.transactionForKeys(ctx, keyArgs)
}

func (c *Client) transactionForKeys(ctx context.Context, keys []any) (*Transaction, error) {
	if err := c.legacyGate.lock(ctx); err != nil {
		return nil, err
	}
	defer c.legacyGate.unlock()
	c.legacyMu.Lock()
	if c.legacy != nil && c.legacyMulti {
		c.legacyMu.Unlock()
		return nil, errors.New("cannot start an explicit transaction while legacy MULTI is active")
	}
	session := c.legacy
	c.setLegacySessionLocked(nil)
	c.legacyMu.Unlock()
	if session == nil {
		var err error
		session, err = c.acquireCommandSession(ctx, keys...)
		if err != nil {
			return nil, err
		}
	}
	response, err := session.Do(ctx, "MULTI")
	if err != nil {
		session.Abort(err)
		return nil, err
	}
	if _, err = responseOK(response, nil); err != nil {
		session.Abort(err)
		return nil, err
	}
	t := &Transaction{session: session, done: make(chan struct{})}
	if ctx != nil && ctx.Done() != nil {
		go func() {
			select {
			case <-ctx.Done():
				t.abort(ctx.Err())
			case <-t.done:
			}
		}()
	}
	return t, nil
}

func (c *Client) acquireCommandSession(ctx context.Context, keys ...any) (commandSession, error) {
	if provider, ok := c.exec.(commandSessionProvider); ok {
		session, err := provider.acquireCommandSession(ctx, keys...)
		if err != nil {
			return nil, err
		}
		if interfaceIsNil(session) {
			return nil, errors.New("transaction executor returned a nil connection-affine session")
		}
		return session, nil
	}
	return nil, errors.New("transactions require an executor with connection-affine session support")
}

type clientCommandSession struct {
	commandSession
	release func()
	once    sync.Once
}

func (s *clientCommandSession) Abort(err error) {
	s.commandSession.Abort(err)
	s.finish()
}

func (s *clientCommandSession) Release() {
	s.commandSession.Release()
	s.finish()
}

func (s *clientCommandSession) finish() {
	s.once.Do(func() {
		if s.release != nil {
			s.release()
		}
	})
}

func (t *Transaction) Command(ctx context.Context, args ...any) (any, error) {
	if err := validateCommandArgs(args); err != nil {
		return nil, err
	}
	if name, stateful := connectionStateCommand(args); stateful {
		return nil, fmt.Errorf("%s cannot be queued inside a transaction", name)
	}
	if name, mutates := connectionStateMutationCommand(args); mutates {
		return nil, fmt.Errorf("%s is connection-local and cannot be queued inside a transaction", name)
	}
	if err := t.opMu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer t.opMu.Unlock()
	if err := t.closedStateError(); err != nil {
		return nil, err
	}
	payload := affineCommandArgs(args)
	value, err := t.session.Do(ctx, payload...)
	if err != nil {
		t.abort(err)
		return nil, err
	}
	if !strings.EqualFold(asString(value), "QUEUED") {
		err := fmt.Errorf("transaction command returned %q, expected QUEUED", asString(value))
		t.abort(err)
		return nil, err
	}
	if err := t.closedStateError(); err != nil {
		return nil, err
	}
	return value, nil
}

func (t *Transaction) Exec(ctx context.Context) ([]any, error) {
	if err := t.opMu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer t.opMu.Unlock()
	if err := t.closedStateError(); err != nil {
		return nil, err
	}
	value, err := t.session.Do(ctx, "EXEC")
	if err != nil {
		t.abort(err)
		return nil, err
	}
	items, err := transactionArray(value, nil)
	if err != nil {
		t.abort(err)
		return nil, err
	}
	if err := t.closedStateError(); err != nil {
		return nil, err
	}
	t.release()
	return items, nil
}

func (t *Transaction) Discard(ctx context.Context) error {
	if err := t.opMu.LockContext(ctx); err != nil {
		return err
	}
	defer t.opMu.Unlock()
	if err := t.closedStateError(); err != nil {
		return err
	}
	response, err := t.session.Do(ctx, "DISCARD")
	if err != nil {
		t.abort(err)
		return err
	}
	if _, err = responseOK(response, nil); err != nil {
		t.abort(err)
		return err
	}
	if err := t.closedStateError(); err != nil {
		return err
	}
	t.release()
	return nil
}

func (t *Transaction) abort(err error) {
	if !t.markClosed(err) {
		return
	}
	t.session.Abort(err)
	t.finish()
}

func (t *Transaction) release() {
	if !t.markClosed(nil) {
		return
	}
	t.session.Release()
	t.finish()
}

func (t *Transaction) markClosed(err error) bool {
	t.stateMu.Lock()
	defer t.stateMu.Unlock()
	if t.closed {
		return false
	}
	t.closed = true
	t.closeErr = err
	return true
}

func (t *Transaction) closedStateError() error {
	t.stateMu.Lock()
	defer t.stateMu.Unlock()
	if !t.closed {
		return nil
	}
	if t.closeErr != nil {
		return t.closeErr
	}
	return errors.New("transaction is closed")
}

func (t *Transaction) finish() {
	t.once.Do(func() { close(t.done) })
}
