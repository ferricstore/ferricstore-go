package ferricstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

type typedNilCommandSession struct{}

func (*typedNilCommandSession) Do(context.Context, ...any) (any, error) { return []byte("OK"), nil }
func (*typedNilCommandSession) Abort(error)                             {}
func (*typedNilCommandSession) Release()                                {}

type typedNilSessionProvider struct{}

func (*typedNilSessionProvider) Do(context.Context, ...any) (any, error) {
	return []byte("OK"), nil
}

func (*typedNilSessionProvider) acquireCommandSession(context.Context, ...any) (commandSession, error) {
	var session *typedNilCommandSession
	return session, nil
}

func TestTransactionEntryPointsRejectTypedNilSessions(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Client) error
	}{
		{name: "watch", run: func(client *Client) error {
			return client.Watch(context.Background(), "key")
		}},
		{name: "multi", run: func(client *Client) error {
			return client.Multi(context.Background())
		}},
		{name: "transaction", run: func(client *Client) error {
			tx, err := client.Transaction(context.Background())
			if tx != nil {
				_ = tx.Discard(context.Background())
			}
			return err
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := NewClientWithExecutor(&typedNilSessionProvider{})
			if err := test.run(client); err == nil {
				t.Fatal("transaction entry point accepted a typed-nil session")
			}
		})
	}
}

func TestCanceledContextsDoNotEnterClientOrTransactionSessionGates(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, *Client) error
	}{
		{name: "command", run: func(ctx context.Context, client *Client) error {
			_, err := client.Command(ctx, "PING")
			return err
		}},
		{name: "pipeline", run: func(ctx context.Context, client *Client) error {
			_, err := client.Pipeline(ctx, [][]any{{"PING"}})
			return err
		}},
		{name: "watch", run: func(ctx context.Context, client *Client) error {
			return client.Watch(ctx, "key")
		}},
		{name: "multi", run: func(ctx context.Context, client *Client) error {
			return client.Multi(ctx)
		}},
		{name: "transaction", run: func(ctx context.Context, client *Client) error {
			_, err := client.Transaction(ctx)
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := test.run(ctx, NewClientWithExecutor(exec)); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled call error = %v, want context.Canceled", err)
			}
			if len(exec.calls) != 0 {
				t.Fatalf("canceled call reached executor: %#v", exec.calls)
			}
		})
	}
}

func TestKeyPinnedTransactionEntryPointsRequireKeysLocally(t *testing.T) {
	for _, test := range []struct {
		name string
		run  func(*Client) error
	}{
		{name: "watch", run: func(client *Client) error {
			return client.Watch(context.Background())
		}},
		{name: "transaction for keys", run: func(client *Client) error {
			_, err := client.TransactionForKeys(context.Background())
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			if err := test.run(NewClientWithExecutor(exec)); err == nil {
				t.Fatal("key-pinned transaction entry point accepted zero keys")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid key-pinned call reached executor: %#v", exec.calls)
			}
		})
	}
}

func TestExplicitTransactionRejectsCanceledOperationBeforeSession(t *testing.T) {
	for _, test := range []struct {
		name string
		run  func(context.Context, *Transaction) error
	}{
		{name: "command", run: func(ctx context.Context, tx *Transaction) error {
			_, err := tx.Command(ctx, "SET", "key", "value")
			return err
		}},
		{name: "exec", run: func(ctx context.Context, tx *Transaction) error {
			_, err := tx.Exec(ctx)
			return err
		}},
		{name: "discard", run: func(ctx context.Context, tx *Transaction) error {
			return tx.Discard(ctx)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			tx, err := NewClientWithExecutor(exec).Transaction(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := test.run(ctx, tx); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled transaction operation error = %v, want context.Canceled", err)
			}
			if len(exec.calls) != 1 {
				t.Fatalf("canceled transaction operation reached session: %#v", exec.calls)
			}
			_ = tx.Discard(context.Background())
		})
	}
}

func TestExplicitTransactionOperationWaitHonorsContext(t *testing.T) {
	session := &blockingTransactionSession{entered: make(chan struct{}), aborted: make(chan struct{})}
	tx, err := NewClientWithExecutor(&blockingTransactionProvider{session: session}).
		Transaction(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() {
		_, err := tx.Command(context.Background(), "SET", "first", "value")
		firstDone <- err
	}()
	select {
	case <-session.entered:
	case <-time.After(time.Second):
		t.Fatal("first transaction operation did not start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	secondDone := make(chan error, 1)
	go func() {
		_, err := tx.Command(ctx, "SET", "second", "value")
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			session.Abort(context.Canceled)
			<-firstDone
			t.Fatalf("waiting transaction operation error = %v, want deadline exceeded", err)
		}
	case <-time.After(100 * time.Millisecond):
		session.Abort(context.Canceled)
		<-firstDone
		<-secondDone
		t.Fatal("waiting transaction operation ignored its context deadline")
	}
	session.Abort(context.Canceled)
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first transaction operation error = %v, want cancellation", err)
	}
}
