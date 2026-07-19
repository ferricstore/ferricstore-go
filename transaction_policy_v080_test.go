package ferricstore

import (
	"context"
	"strings"
	"testing"
)

func TestV080ExplicitTransactionRejectsUnsupportedCommandsBeforeSession(t *testing.T) {
	tests := [][]any{
		{"FLOW.GET", "flow"},
		{"TOPK.ADD", "sketch", "value"},
		{"XADD", "stream", "*", "field", "value"},
		{"FETCH_OR_COMPUTE", "key", 1000},
		{"DBSIZE"},
	}
	for _, args := range tests {
		name := commandName(args)
		t.Run(name, func(t *testing.T) {
			exec := &transactionRecordingExecutor{calls: make(chan []any, 4)}
			tx, err := NewClientWithExecutor(exec).Transaction(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			<-exec.calls // MULTI

			_, commandErr := tx.Command(context.Background(), args...)
			select {
			case call := <-exec.calls:
				t.Fatalf("unsupported %s reached transaction session: %#v", name, call)
			default:
			}
			if commandErr == nil || !strings.Contains(strings.ToLower(commandErr.Error()), "not supported") {
				t.Fatalf("unsupported %s error = %v", name, commandErr)
			}
			if err := tx.Discard(context.Background()); err != nil {
				t.Fatalf("local rejection closed transaction: %v", err)
			}
		})
	}
}

func TestV080ExplicitTransactionAllowsLocalNoKeyCommands(t *testing.T) {
	for _, args := range [][]any{{"PING"}, {"ECHO", "hello"}, {"CLUSTER.KEYSLOT", "key"}} {
		name := commandName(args)
		t.Run(name, func(t *testing.T) {
			exec := &transactionRecordingExecutor{calls: make(chan []any, 4)}
			tx, err := NewClientWithExecutor(exec).Transaction(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			<-exec.calls // MULTI
			if _, err := tx.Command(context.Background(), args...); err != nil {
				t.Fatalf("supported local transaction command failed: %v", err)
			}
			if err := tx.Discard(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestV080LegacyMultiRejectsUnsupportedCommandBeforeSession(t *testing.T) {
	exec := &transactionRecordingExecutor{calls: make(chan []any, 4)}
	client := NewClientWithExecutor(exec)
	if err := client.Multi(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-exec.calls // MULTI

	_, commandErr := client.Command(context.Background(), "FLOW.GET", "flow")
	select {
	case call := <-exec.calls:
		t.Fatalf("unsupported legacy MULTI command reached session: %#v", call)
	default:
	}
	if commandErr == nil || !strings.Contains(strings.ToLower(commandErr.Error()), "not supported") {
		t.Fatalf("unsupported legacy MULTI command error = %v", commandErr)
	}
	if err := client.Discard(context.Background()); err != nil {
		t.Fatalf("local rejection closed legacy transaction: %v", err)
	}
}
