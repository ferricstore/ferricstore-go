package ferricstore

import (
	"context"
	"testing"
)

type commandNameStringer struct{}

func (commandNameStringer) String() string { return "PING" }

type namedCommandString string
type namedCommandBytes []byte

func TestClientRejectsMalformedCommandsBeforeExecution(t *testing.T) {
	for _, args := range [][]any{
		nil,
		{},
		{""},
		{[]byte{}},
		{" PING"},
		{"PING\n"},
		{int64(1)},
		{true},
		{commandNameStringer{}},
	} {
		t.Run(commandValidationTestName(args), func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			if _, err := NewClientWithExecutor(exec).Command(context.Background(), args...); err == nil {
				t.Fatal("malformed command was accepted")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("malformed command reached executor: %#v", exec.calls)
			}
		})
	}
}

func TestClientAcceptsNamedTextCommandTypes(t *testing.T) {
	for _, command := range []any{namedCommandString("PING"), namedCommandBytes("PING")} {
		exec := &fakeExecutor{value: []byte("PONG")}
		if _, err := NewClientWithExecutor(exec).Command(context.Background(), command); err != nil {
			t.Fatalf("named text command %T failed: %v", command, err)
		}
		if len(exec.calls) != 1 {
			t.Fatalf("named text command %T reached executor %d times", command, len(exec.calls))
		}
	}
}

func TestPipelineRejectsMalformedCommandsBeforePartialExecution(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	_, err := NewClientWithExecutor(exec).Pipeline(context.Background(), [][]any{
		{"PING"},
		{},
		{"PING"},
	})
	if err == nil {
		t.Fatal("pipeline accepted an empty command")
	}
	if len(exec.calls) != 0 {
		t.Fatalf("invalid pipeline partially executed: %#v", exec.calls)
	}
}

func TestBufferedExecutorRejectsMalformedCommandsBeforeQueueing(t *testing.T) {
	exec := NewBufferedExecutor(nil)
	for _, args := range [][]any{nil, {}, {""}, {[]byte{}}} {
		if _, err := exec.Do(context.Background(), args...); err == nil {
			t.Fatalf("buffer accepted malformed command %#v", args)
		}
	}
	if len(exec.commands) != 0 {
		t.Fatalf("malformed commands were buffered: %#v", exec.commands)
	}
}

func TestExplicitTransactionRejectsMalformedCommandsWithoutClosingSession(t *testing.T) {
	exec := &transactionRecordingExecutor{calls: make(chan []any, 8)}
	tx, err := NewClientWithExecutor(exec).Transaction(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if call := <-exec.calls; asString(call[0]) != "MULTI" {
		t.Fatalf("transaction start = %#v, want MULTI", call)
	}
	defer func() { _ = tx.Discard(context.Background()) }()

	for _, args := range [][]any{nil, {}, {""}, {[]byte{}}} {
		if _, err := tx.Command(context.Background(), args...); err == nil {
			t.Fatalf("transaction accepted malformed command %#v", args)
		}
		select {
		case call := <-exec.calls:
			t.Fatalf("malformed transaction command reached session: %#v", call)
		default:
		}
	}

	if _, err := tx.Command(context.Background(), "SET", "key", "value"); err != nil {
		t.Fatalf("valid command after local validation error: %v", err)
	}
	if call := <-exec.calls; len(call) < 2 || asString(call[1]) != "SET" {
		t.Fatalf("valid transaction command = %#v, want COMMAND_EXEC SET", call)
	}
}

func TestExplicitTransactionRejectsConnectionStateCommandsWithoutClosingSession(t *testing.T) {
	exec := &transactionRecordingExecutor{calls: make(chan []any, 8)}
	tx, err := NewClientWithExecutor(exec).Transaction(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if call := <-exec.calls; asString(call[0]) != "MULTI" {
		t.Fatalf("transaction start = %#v, want MULTI", call)
	}
	defer func() { _ = tx.Discard(context.Background()) }()

	for _, args := range [][]any{
		{"MULTI"},
		{"EXEC"},
		{"DISCARD"},
		{"WATCH", "key"},
		{"UNWATCH"},
		{"AUTH", "secret"},
		{"CLIENT", "SETNAME", "tx-name"},
		{"COMMAND_EXEC", "RESET"},
	} {
		if _, err := tx.Command(context.Background(), args...); err == nil {
			t.Fatalf("transaction accepted connection-state command %#v", args)
		}
		select {
		case call := <-exec.calls:
			t.Fatalf("connection-state command reached transaction session: %#v", call)
		default:
		}
	}

	if _, err := tx.Command(context.Background(), "SET", "key", "value"); err != nil {
		t.Fatalf("valid command after local rejection: %v", err)
	}
	if call := <-exec.calls; len(call) < 2 || asString(call[1]) != "SET" {
		t.Fatalf("valid transaction command = %#v, want COMMAND_EXEC SET", call)
	}
}

func commandValidationTestName(args []any) string {
	if len(args) == 0 {
		return "missing"
	}
	return "empty"
}
