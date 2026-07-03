package ferricstore

import (
	"context"
	"reflect"
	"sync"
	"testing"
)

func TestBufferedExecutorQueuesCopiedCommands(t *testing.T) {
	exec := NewBufferedExecutor(nil)
	args := []any{"SET", "k", "v"}

	value, err := exec.Do(context.Background(), args...)
	args[0] = "GET"

	if err != nil {
		t.Fatal(err)
	}
	if string(value.([]byte)) != "QUEUED" {
		t.Fatalf("unexpected placeholder value: %#v", value)
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

func TestBufferedExecutorConcurrentDo(t *testing.T) {
	exec := NewBufferedExecutor(nil)
	const count = 64

	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := exec.Do(context.Background(), "SET", i, i); err != nil {
				t.Errorf("Do failed: %v", err)
			}
		}(i)
	}
	wg.Wait()

	results, err := exec.Flush(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if results != nil {
		t.Fatalf("expected nil results without client, got %#v", results)
	}
	if exec.Flushes != 1 || exec.CommandsSent != count || exec.MaxDepth != count {
		t.Fatalf("unexpected stats after concurrent flush: %+v", exec)
	}
}
