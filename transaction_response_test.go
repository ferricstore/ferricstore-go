package ferricstore

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type transactionAckProvider struct {
	session *transactionAckSession
}

func (*transactionAckProvider) Do(context.Context, ...any) (any, error) {
	return nil, errors.New("unexpected non-session command")
}

func (p *transactionAckProvider) acquireCommandSession(context.Context, ...any) (commandSession, error) {
	return p.session, nil
}

type transactionAckSession struct {
	mu        sync.Mutex
	responses map[string]any
	aborted   int
	released  int
}

type queuedStatusStringer struct{}

func (queuedStatusStringer) String() string { return "QUEUED" }

func (s *transactionAckSession) Do(_ context.Context, args ...any) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.responses[commandName(args)], nil
}

func (s *transactionAckSession) Abort(error) {
	s.mu.Lock()
	s.aborted++
	s.mu.Unlock()
}

func (s *transactionAckSession) Release() {
	s.mu.Lock()
	s.released++
	s.mu.Unlock()
}

func (s *transactionAckSession) finishCounts() (aborted, released int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.aborted, s.released
}

func TestTransactionFinalizersRejectMalformedStatusAcknowledgements(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Client) error
	}{
		{
			name: "legacy UNWATCH",
			run: func(client *Client) error {
				if err := client.Watch(context.Background(), "key"); err != nil {
					return err
				}
				return client.Unwatch(context.Background())
			},
		},
		{
			name: "legacy DISCARD",
			run: func(client *Client) error {
				if err := client.Multi(context.Background()); err != nil {
					return err
				}
				return client.Discard(context.Background())
			},
		},
		{
			name: "explicit DISCARD",
			run: func(client *Client) error {
				tx, err := client.Transaction(context.Background())
				if err != nil {
					return err
				}
				return tx.Discard(context.Background())
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session := &transactionAckSession{responses: map[string]any{
				"WATCH":   []byte("OK"),
				"MULTI":   []byte("OK"),
				"UNWATCH": []byte("NOT-OK"),
				"DISCARD": []byte("NOT-OK"),
			}}
			client := NewClientWithExecutor(&transactionAckProvider{session: session})
			if err := test.run(client); err == nil {
				t.Fatal("malformed transaction finalizer acknowledgement was accepted")
			}
			aborted, released := session.finishCounts()
			if aborted != 1 || released != 0 {
				t.Fatalf("session finalized with abort/release = %d/%d; want 1/0", aborted, released)
			}
		})
	}
}

func TestExplicitTransactionRejectsCoercedQueuedAcknowledgement(t *testing.T) {
	session := &transactionAckSession{responses: map[string]any{
		"MULTI":        []byte("OK"),
		"COMMAND_EXEC": queuedStatusStringer{},
	}}
	tx, err := NewClientWithExecutor(&transactionAckProvider{session: session}).Transaction(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if value, err := tx.Command(context.Background(), "SET", "key", "value"); err == nil {
		t.Fatalf("transaction accepted coerced acknowledgement %#v", value)
	}
	aborted, released := session.finishCounts()
	if aborted != 1 || released != 0 {
		t.Fatalf("session finalized with abort/release = %d/%d; want 1/0", aborted, released)
	}
}

func TestLegacyTransactionHelpersPreserveSessionOnInvalidStateTransition(t *testing.T) {
	tests := []struct {
		name     string
		prepare  func(*Client) error
		invalid  func(*Client) error
		finalize func(*Client) error
	}{
		{
			name:    "EXEC before MULTI",
			prepare: func(client *Client) error { return client.Watch(context.Background(), "key") },
			invalid: func(client *Client) error {
				_, err := client.Exec(context.Background())
				return err
			},
			finalize: func(client *Client) error { return client.Unwatch(context.Background()) },
		},
		{
			name:     "DISCARD before MULTI",
			prepare:  func(client *Client) error { return client.Watch(context.Background(), "key") },
			invalid:  func(client *Client) error { return client.Discard(context.Background()) },
			finalize: func(client *Client) error { return client.Unwatch(context.Background()) },
		},
		{
			name:     "UNWATCH during MULTI",
			prepare:  func(client *Client) error { return client.Multi(context.Background()) },
			invalid:  func(client *Client) error { return client.Unwatch(context.Background()) },
			finalize: func(client *Client) error { return client.Discard(context.Background()) },
		},
		{
			name:     "nested MULTI",
			prepare:  func(client *Client) error { return client.Multi(context.Background()) },
			invalid:  func(client *Client) error { return client.Multi(context.Background()) },
			finalize: func(client *Client) error { return client.Discard(context.Background()) },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session := &transactionAckSession{responses: map[string]any{
				"WATCH": []byte("OK"), "MULTI": []byte("OK"), "UNWATCH": []byte("OK"),
				"EXEC": []any{}, "DISCARD": []byte("OK"),
			}}
			client := NewClientWithExecutor(&transactionAckProvider{session: session})
			if err := test.prepare(client); err != nil {
				t.Fatal(err)
			}
			if err := test.invalid(client); err == nil {
				t.Fatal("invalid transaction state transition succeeded")
			}
			if err := test.finalize(client); err != nil {
				t.Fatalf("valid finalizer after rejected transition: %v", err)
			}
			aborted, released := session.finishCounts()
			if aborted != 0 || released != 1 {
				t.Fatalf("session finalized with abort/release = %d/%d; want 0/1", aborted, released)
			}
		})
	}
}

func TestExplicitTransactionDoesNotStealActiveLegacyMultiSession(t *testing.T) {
	session := &transactionAckSession{responses: map[string]any{
		"MULTI": []byte("OK"), "DISCARD": []byte("OK"),
	}}
	client := NewClientWithExecutor(&transactionAckProvider{session: session})
	if err := client.Multi(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Transaction(context.Background()); err == nil {
		t.Fatal("explicit transaction reused an active legacy MULTI session")
	}
	if err := client.Discard(context.Background()); err != nil {
		t.Fatalf("legacy DISCARD after rejected explicit transaction: %v", err)
	}
	aborted, released := session.finishCounts()
	if aborted != 0 || released != 1 {
		t.Fatalf("session finalized with abort/release = %d/%d; want 0/1", aborted, released)
	}
}
