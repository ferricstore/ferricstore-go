package ferricstore

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestBufferedFlushWithoutClientRetainsQueuedCommands(t *testing.T) {
	exec := NewBufferedExecutor(nil)
	if _, err := exec.Do(context.Background(), "SET", "key", "value"); err != nil {
		t.Fatal(err)
	}

	values, err := exec.Flush(context.Background())
	if err == nil || !strings.Contains(err.Error(), "requires a client") {
		t.Fatalf("Flush error = %v; want a missing-client error", err)
	}
	if values != nil {
		t.Fatalf("Flush values = %#v; want nil", values)
	}
	if len(exec.commands) != 1 {
		t.Fatalf("Flush retained %d commands; want 1", len(exec.commands))
	}
	if stats := exec.Stats(); stats != (BufferedStats{}) {
		t.Fatalf("failed pre-dispatch Flush changed stats: %+v", stats)
	}
}

func TestBufferedCanceledFlushRetainsQueuedCommands(t *testing.T) {
	pipeline := &fakePipelineExecutor{}
	exec := NewBufferedExecutor(NewClientWithExecutor(pipeline))
	if _, err := exec.Do(context.Background(), "SET", "key", "value"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	values, err := exec.Flush(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Flush error = %v, want context.Canceled", err)
	}
	if values != nil {
		t.Fatalf("Flush values = %#v, want nil", values)
	}
	if pipeline.batchCount() != 0 || len(exec.commands) != 1 {
		t.Fatalf("canceled Flush dispatched=%d retained=%d, want 0/1", pipeline.batchCount(), len(exec.commands))
	}
	if stats := exec.Stats(); stats != (BufferedStats{}) {
		t.Fatalf("canceled pre-dispatch Flush changed stats: %+v", stats)
	}
}

func TestBufferedFlushWaitHonorsContext(t *testing.T) {
	blocked := make(chan struct{})
	pipeline := &fakePipelineExecutor{blocking: blocked}
	exec := NewBufferedExecutor(NewClientWithExecutor(pipeline))
	_, _ = exec.Do(context.Background(), "SET", "first", "value")
	firstDone := make(chan error, 1)
	go func() {
		_, err := exec.Flush(context.Background())
		firstDone <- err
	}()
	deadline := time.Now().Add(time.Second)
	for exec.Stats().CommandsSent != 1 {
		if time.Now().After(deadline) {
			close(blocked)
			t.Fatal("first Flush did not start")
		}
		time.Sleep(time.Millisecond)
	}
	_, _ = exec.Do(context.Background(), "SET", "second", "value")

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	secondDone := make(chan error, 1)
	go func() {
		_, err := exec.Flush(ctx)
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			close(blocked)
			t.Fatalf("waiting Flush error = %v, want deadline exceeded", err)
		}
	case <-time.After(100 * time.Millisecond):
		close(blocked)
		<-firstDone
		<-secondDone
		t.Fatal("waiting Flush ignored its context deadline")
	}
	if len(exec.commands) != 1 || exec.Stats().CommandsSent != 1 {
		close(blocked)
		<-firstDone
		t.Fatalf("timed-out Flush retained=%d stats=%+v, want one queued and one sent", len(exec.commands), exec.Stats())
	}
	close(blocked)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}
