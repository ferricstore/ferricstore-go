package ferricstore

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type overlapDetectingCodec struct {
	active     atomic.Int32
	overlapped atomic.Bool
}

func (c *overlapDetectingCodec) Encode(value any) (any, error) {
	c.enter()
	defer c.leave()
	return []byte(fmt.Sprint(value)), nil
}

func (c *overlapDetectingCodec) Decode(value any) (any, error) {
	c.enter()
	defer c.leave()
	return value, nil
}

func (c *overlapDetectingCodec) enter() {
	if c.active.Add(1) > 1 {
		c.overlapped.Store(true)
	}
	time.Sleep(2 * time.Millisecond)
}

func (c *overlapDetectingCodec) leave() {
	c.active.Add(-1)
}

func (c *overlapDetectingCodec) reset() {
	c.overlapped.Store(false)
}

type codecConcurrencyExecutor struct{}

func (codecConcurrencyExecutor) Do(_ context.Context, args ...any) (any, error) {
	if commandName(args) == "GET" {
		return []byte("value"), nil
	}
	return []byte("OK"), nil
}

type deferredCodecConcurrencyExecutor struct{ codecConcurrencyExecutor }

func (deferredCodecConcurrencyExecutor) supportsDeferredCodec(Codec) bool { return true }

type scratchOutputCodec struct {
	buffer []byte
}

func newScratchOutputCodec() *scratchOutputCodec {
	return &scratchOutputCodec{buffer: make([]byte, 64)}
}

func (c *scratchOutputCodec) Encode(value any) (any, error) {
	encoded := []byte(fmt.Sprint(value))
	copy(c.buffer, encoded)
	return c.buffer[:len(encoded)], nil
}

func (c *scratchOutputCodec) Decode(value any) (any, error) {
	encoded := []byte(asString(value))
	copy(c.buffer, encoded)
	return c.buffer[:len(encoded)], nil
}

type codecOutputOwnershipExecutor struct {
	firstEntered  chan struct{}
	secondEntered chan struct{}
	getCalls      atomic.Int32
}

func (e *codecOutputOwnershipExecutor) Do(_ context.Context, args ...any) (any, error) {
	switch commandName(args) {
	case "SET":
		switch asString(args[1]) {
		case "first":
			close(e.firstEntered)
			<-e.secondEntered
			if got := asString(args[2]); got != "first" {
				return nil, fmt.Errorf("first encoded value changed to %q", got)
			}
		case "second":
			close(e.secondEntered)
		}
		return []byte("OK"), nil
	case "GET":
		if e.getCalls.Add(1) == 1 {
			return []byte("first"), nil
		}
		return []byte("second"), nil
	default:
		return nil, fmt.Errorf("unexpected command %q", commandName(args))
	}
}

func TestBuiltInCodecsRemainLockFree(t *testing.T) {
	for _, codec := range []Codec{RawCodec{}, StringCodec{}, JSONCodec{}, &RawCodec{}, &StringCodec{}, &JSONCodec{}} {
		client := NewClientWithExecutor(codecConcurrencyExecutor{}, WithCodec(codec))
		if _, serialized := client.codec.(*serializedCodec); serialized {
			t.Errorf("built-in codec %T was wrapped with a serialization mutex", codec)
		}
	}
}

func TestCodecOptionsIgnoreTypedNilCodecs(t *testing.T) {
	var builtin *JSONCodec
	var custom *scratchOutputCodec
	for _, test := range []struct {
		name string
		opt  ClientOption
	}{
		{name: "serialized builtin", opt: WithCodec(builtin)},
		{name: "concurrent builtin", opt: WithConcurrentCodec(builtin)},
		{name: "serialized custom", opt: WithCodec(custom)},
		{name: "concurrent custom", opt: WithConcurrentCodec(custom)},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := NewClientWithExecutor(codecConcurrencyExecutor{}, test.opt)
			if _, ok := client.Codec().(RawCodec); !ok {
				t.Fatalf("typed-nil codec replaced default with %T", client.Codec())
			}
			if err := client.KV().Set(context.Background(), "key", "value"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestClientSerializesCustomCodecCalls(t *testing.T) {
	codec := &overlapDetectingCodec{}
	client := NewClientWithExecutor(codecConcurrencyExecutor{}, WithCodec(codec))
	if client.Codec() != codec {
		t.Fatalf("Client.Codec() = %T; want original codec", client.Codec())
	}
	runConcurrentCodecCalls(t, func() error {
		return client.KV().Set(context.Background(), "key", "value")
	})
	if codec.overlapped.Load() {
		t.Fatal("custom Codec.Encode calls overlapped")
	}

	codec.reset()
	runConcurrentCodecCalls(t, func() error {
		_, err := client.KV().Get(context.Background(), "key")
		return err
	})
	if codec.overlapped.Load() {
		t.Fatal("custom Codec.Decode calls overlapped")
	}
}

func TestConcurrentCodecOptionKeepsSafeCustomCodecLockFree(t *testing.T) {
	codec := &overlapDetectingCodec{}
	client := NewClientWithExecutor(codecConcurrencyExecutor{}, WithConcurrentCodec(codec))
	if client.Codec() != codec {
		t.Fatalf("Client.Codec() = %T; want original codec", client.Codec())
	}
	if codecNeedsSerialEncoding(client.codec) {
		t.Fatal("concurrency-safe codec still requests deferred encoding serialization")
	}

	runConcurrentCodecCalls(t, func() error {
		return client.KV().Set(context.Background(), "key", "value")
	})
	if !codec.overlapped.Load() {
		t.Fatal("explicitly concurrency-safe Codec.Encode calls were serialized")
	}
}

func TestDeferredCustomCodecCallsRemainSerialized(t *testing.T) {
	codec := &overlapDetectingCodec{}
	client := NewClientWithExecutor(deferredCodecConcurrencyExecutor{}, WithCodec(codec))

	runConcurrentCodecCalls(t, func() error {
		value, err := client.encode("value")
		if err != nil {
			return err
		}
		deferred, ok := value.(nativeDeferredCodecValue)
		if !ok {
			return fmt.Errorf("encoded value = %T; want deferred codec value", value)
		}
		_, err = encodeNativeDeferredCodecValue(deferred)
		return err
	})
	if codec.overlapped.Load() {
		t.Fatal("deferred custom Codec.Encode calls overlapped")
	}
}

func TestSerializedCodecOwnsMutableEncodeResults(t *testing.T) {
	exec := &codecOutputOwnershipExecutor{
		firstEntered:  make(chan struct{}),
		secondEntered: make(chan struct{}),
	}
	client := NewClientWithExecutor(exec, WithCodec(newScratchOutputCodec()))
	firstErr := make(chan error, 1)
	go func() {
		firstErr <- client.KV().Set(context.Background(), "first", "first")
	}()
	<-exec.firstEntered
	if err := client.KV().Set(context.Background(), "second", "second"); err != nil {
		t.Fatal(err)
	}
	if err := <-firstErr; err != nil {
		t.Fatal(err)
	}
}

func TestSerializedCodecOwnsMutableDecodeResults(t *testing.T) {
	exec := &codecOutputOwnershipExecutor{}
	client := NewClientWithExecutor(exec, WithCodec(newScratchOutputCodec()))
	first, err := client.KV().Get(context.Background(), "first")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.KV().Get(context.Background(), "second"); err != nil {
		t.Fatal(err)
	}
	if got := asString(first); got != "first" {
		t.Fatalf("first decoded value changed to %q", got)
	}
}

func runConcurrentCodecCalls(t *testing.T, call func() error) {
	t.Helper()
	const callers = 16
	start := make(chan struct{})
	errs := make(chan error, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	for range callers {
		go func() {
			ready.Done()
			<-start
			errs <- call()
		}()
	}
	ready.Wait()
	close(start)
	for range callers {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}
