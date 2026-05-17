package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestBufferedExecutorQueuesCopiedCommands(t *testing.T) {
	exec := NewBufferedExecutor(nil)
	args := []any{"SET", "k", "v"}

	cmd := exec.Do(context.Background(), args...)
	args[0] = "GET"

	if string(cmd.Val().([]byte)) != "QUEUED" {
		t.Fatalf("unexpected placeholder value: %#v", cmd.Val())
	}
	want := [][]any{{"SET", "k", "v"}}
	if !reflect.DeepEqual(exec.commands, want) {
		t.Fatalf("unexpected buffered commands\n got: %#v\nwant: %#v", exec.commands, want)
	}
}

func TestBufferedExecutorEmptyFlush(t *testing.T) {
	exec := NewBufferedExecutor(nil)

	results, err := exec.Flush(context.Background())

	if err != nil {
		t.Fatal(err)
	}
	if results != nil {
		t.Fatalf("expected nil results, got %#v", results)
	}
	if exec.Flushes != 0 || exec.CommandsSent != 0 || exec.MaxDepth != 0 {
		t.Fatalf("unexpected stats: %+v", exec)
	}
}
