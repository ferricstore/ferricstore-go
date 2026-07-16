//go:build integration

package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestIntegrationExplicitAndWatchedTransactions(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationClient(StringCodec{})
	defer client.Close()
	other := integrationDirectClient(StringCodec{})
	defer other.Close()

	prefix := "go-sdk:transaction:" + integrationSuffix("stateful") + ":"
	defer cleanupPrefix(t, ctx, client, prefix)

	explicitKey := prefix + "explicit"
	tx, err := client.TransactionForKeys(ctx, explicitKey)
	if err != nil {
		t.Fatal(err)
	}
	if value, err := tx.Command(ctx, "SET", explicitKey, "inside"); err != nil || !isQueued(value) {
		t.Fatalf("transaction SET = %#v, %v", value, err)
	}
	if value, err := tx.Command(ctx, "GET", explicitKey); err != nil || !isQueued(value) {
		t.Fatalf("transaction GET = %#v, %v", value, err)
	}
	results, err := tx.Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || !isOK(results[0]) || asString(results[1]) != "inside" {
		t.Fatalf("transaction EXEC = %#v", results)
	}

	watchKey := prefix + "watched"
	if err := client.KV().Set(ctx, watchKey, "before"); err != nil {
		t.Fatal(err)
	}
	if err := client.Watch(ctx, watchKey); err != nil {
		t.Fatal(err)
	}
	if value, err := client.KV().Get(ctx, watchKey); err != nil || value != "before" {
		t.Fatalf("watched GET = %#v, %v", value, err)
	}
	if err := other.KV().Set(ctx, watchKey, "outside"); err != nil {
		t.Fatal(err)
	}
	if err := client.Multi(ctx); err != nil {
		t.Fatal(err)
	}
	if value, err := client.Command(ctx, "SET", watchKey, "inside"); err != nil || !isQueued(value) {
		t.Fatalf("watched transaction SET = %#v, %v", value, err)
	}
	results, err = client.Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if results != nil {
		t.Fatalf("conflicted WATCH EXEC = %#v; want nil", results)
	}
	if value, err := other.KV().Get(ctx, watchKey); err != nil || value != "outside" {
		t.Fatalf("conflicted WATCH changed value to %#v, %v", value, err)
	}
}

func TestIntegrationBufferedAndAutoBatchExecution(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	base := integrationClient(StringCodec{})
	defer base.Close()
	prefix := "go-sdk:batching:" + integrationSuffix("stateful") + ":"
	defer cleanupPrefix(t, ctx, base, prefix)

	buffered := NewBufferedExecutor(base)
	bufferedClient := NewClientWithExecutor(buffered, WithCodec(StringCodec{}))
	if err := bufferedClient.KV().Set(ctx, prefix+"buffered:a", "a"); err != nil {
		t.Fatal(err)
	}
	if value, err := bufferedClient.Command(ctx, "SET", prefix+"buffered:b", "b"); err != nil || !isQueued(value) {
		t.Fatalf("buffered command = %#v, %v", value, err)
	}
	results, err := buffered.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || !isOK(results[0]) || !isOK(results[1]) {
		t.Fatalf("buffered flush = %#v", results)
	}

	auto := NewAutoBatchClient(
		integrationAddress(),
		AutoBatchOptions{MaxSize: 8, FlushInterval: time.Millisecond, QueueSize: 64},
		WithCodec(StringCodec{}),
	)
	defer auto.Close()
	const commands = 32
	errs := make(chan error, commands)
	var wg sync.WaitGroup
	for index := range commands {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			key := fmt.Sprintf("%sauto:%d", prefix, index)
			if err := auto.KV().Set(ctx, key, fmt.Sprintf("value:%d", index)); err != nil {
				errs <- err
			}
		}(index)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	for index := range commands {
		key := fmt.Sprintf("%sauto:%d", prefix, index)
		want := fmt.Sprintf("value:%d", index)
		if value, err := auto.KV().Get(ctx, key); err != nil || value != want {
			t.Fatalf("autobatch GET %d = %#v, %v", index, value, err)
		}
	}

	canceled, cancelRequest := context.WithCancel(ctx)
	cancelRequest()
	if err := auto.KV().Set(canceled, prefix+"canceled", "value"); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled autobatch SET = %v", err)
	}
	if value, err := base.KV().Get(ctx, prefix+"canceled"); err != nil || value != nil {
		t.Fatalf("pre-canceled autobatch mutated server: %#v, %v", value, err)
	}
}

func TestIntegrationReconnectAndBlockingCancellation(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	exec := NewNativeExecutor(
		integrationAddress(),
		WithNativeReconnect(1),
		WithNativeHeartbeat(0, 0),
		WithNativeGoAwayDrainTimeout(time.Second),
	)
	client := NewClientWithExecutor(exec, WithCodec(StringCodec{}))
	client.closer = exec.Close
	defer client.Close()

	if _, err := client.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	exec.mu.Lock()
	conn := exec.conn
	exec.mu.Unlock()
	if conn == nil {
		t.Fatal("native client did not establish a connection")
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		exec.mu.Lock()
		closed := exec.conn == nil
		exec.mu.Unlock()
		if closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("reader did not observe forced socket closure")
		}
		time.Sleep(time.Millisecond)
	}
	if pong, err := client.Ping(ctx); err != nil || pong != "PONG" {
		t.Fatalf("PING after reconnect = %q, %v", pong, err)
	}

	blockingKey := "go-sdk:blocking:" + integrationSuffix("cancel")
	requestCtx, cancelRequest := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelRequest()
	started := time.Now()
	_, err := client.ListStore().BLPop(requestCtx, 10, blockingKey)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled BLPOP = %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("BLPOP cancellation took %v", elapsed)
	}
	if pong, err := client.Ping(ctx); err != nil || pong != "PONG" {
		t.Fatalf("PING after BLPOP cancellation = %q, %v", pong, err)
	}
}

func isQueued(value any) bool {
	return strings.EqualFold(asString(value), "QUEUED")
}
