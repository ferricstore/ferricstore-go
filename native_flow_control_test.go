package ferricstore

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNativeBlockingCommandExtendsDefaultTimeout(t *testing.T) {
	tests := []struct {
		name string
		args []any
	}{
		{name: "BLPOP", args: []any{"BLPOP", "jobs", 1}},
		{name: "XREAD BLOCK zero", args: []any{"XREAD", "BLOCK", 0, "STREAMS", "events", "$"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = listener.Close() }()
			errCh := make(chan error, 1)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					errCh <- err
					return
				}
				defer func() { _ = conn.Close() }()
				reader := bufio.NewReader(conn)
				writer := bufio.NewWriter(conn)
				startup, err := readNativeRequestFrame(reader)
				if err != nil {
					errCh <- err
					return
				}
				if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
					errCh <- err
					return
				}
				command, err := readNativeRequestFrame(reader)
				if err != nil {
					errCh <- err
					return
				}
				time.Sleep(90 * time.Millisecond)
				errCh <- writeNativeTestResponse(writer, command, nativeStatusOK, nil)
			}()

			exec := NewNativeExecutor(listener.Addr().String(),
				WithNativeTimeout(30*time.Millisecond),
				WithNativeReconnect(0),
			)
			defer func() { _ = exec.Close() }()
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			if _, err := exec.Do(ctx, tc.args...); err != nil {
				t.Fatalf("blocking command used transport timeout: %v", err)
			}
			if err := <-errCh; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNativeChunksPipelinesAtStartupLimit(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	sizes := make(chan int, 3)
	errCh := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader := bufio.NewReader(conn)
		writer := bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"limits": map[string]any{"max_pipeline_commands": int64(2)}}); err != nil {
			errCh <- err
			return
		}
		total := 0
		for total < 5 {
			frame, err := readNativeRequestFrame(reader)
			if err != nil {
				errCh <- err
				return
			}
			value, _, err := decodeNativeValue(frame.body)
			if err != nil {
				errCh <- err
				return
			}
			payload := value.(map[string]any)
			commands := payload["commands"].([]any)
			sizes <- len(commands)
			total += len(commands)
			responses := make([]any, len(commands))
			for i := range responses {
				responses[i] = []any{"ok", fmt.Sprintf("value-%d", total-len(commands)+i)}
			}
			if err := writeNativeTestResponse(writer, frame, nativeStatusOK, responses); err != nil {
				errCh <- err
				return
			}
		}
		errCh <- nil
	}()
	exec := NewNativeExecutor(listener.Addr().String(), WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0))
	defer func() { _ = exec.Close() }()
	commands := make([][]any, 5)
	for i := range commands {
		commands[i] = []any{"CUSTOM.GET", fmt.Sprintf("key-%d", i)}
	}
	values, err := exec.Pipeline(context.Background(), commands)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 5 {
		t.Fatalf("pipeline returned %d values", len(values))
	}
	close(sizes)
	got := make([]int, 0, 3)
	for size := range sizes {
		got = append(got, size)
	}
	if !reflect.DeepEqual(got, []int{2, 2, 1}) {
		t.Fatalf("pipeline chunk sizes = %v, want [2 2 1]", got)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestNativeChunkedPipelineContinuesAfterLocalBuildError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	errCh := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{
			"limits": map[string]any{"max_pipeline_commands": int64(3)},
		}); err != nil {
			errCh <- err
			return
		}
		for index := range 2 {
			frame, err := readNativeRequestFrame(reader)
			if err != nil {
				errCh <- err
				return
			}
			value, _, err := decodeNativeValue(frame.body)
			if err != nil {
				errCh <- err
				return
			}
			payload, ok := value.(map[string]any)
			if !ok || len(payload["commands"].([]any)) != 1 {
				errCh <- fmt.Errorf("pipeline payload = %#v; want one valid command", value)
				return
			}
			if err := writeNativeTestResponse(writer, frame, nativeStatusOK, []any{[]any{"ok", fmt.Sprintf("value-%d", index)}}); err != nil {
				errCh <- err
				return
			}
		}
		errCh <- nil
	}()

	exec := NewNativeExecutor(listener.Addr().String(), WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0))
	defer func() { _ = exec.Close() }()
	values, err := exec.Pipeline(context.Background(), [][]any{
		{"CUSTOM.GET", "first"},
		{},
		{"CUSTOM.GET", "third"},
	})
	if err == nil {
		t.Fatal("expected malformed middle command to be reported")
	}
	if len(values) != 3 || asString(values[0]) != "value-0" || asString(values[2]) != "value-1" {
		t.Fatalf("pipeline values = %#v; want successful commands around local error", values)
	}
	if _, ok := values[1].(error); !ok {
		t.Fatalf("pipeline middle value = %#v; want build error", values[1])
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestNativeSequentialPipelineMatchesChunkedLocalErrorSemantics(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	errCh := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{
			"limits": map[string]any{"max_pipeline_commands": int64(0)},
		}); err != nil {
			errCh <- err
			return
		}
		for index := range 2 {
			frame, err := readNativeRequestFrame(reader)
			if err != nil {
				errCh <- err
				return
			}
			if frame.opcode == nativeOpPipeline {
				errCh <- fmt.Errorf("sequential fallback sent PIPELINE opcode")
				return
			}
			if err := writeNativeTestResponse(writer, frame, nativeStatusOK, fmt.Sprintf("value-%d", index)); err != nil {
				errCh <- err
				return
			}
		}
		errCh <- nil
	}()

	exec := NewNativeExecutor(listener.Addr().String(), WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0))
	defer func() { _ = exec.Close() }()
	values, err := exec.Pipeline(context.Background(), [][]any{
		{"CUSTOM.GET", "first"},
		{},
		{"CUSTOM.GET", "third"},
	})
	if err == nil {
		t.Fatal("expected malformed middle command to be reported")
	}
	if len(values) != 3 || asString(values[0]) != "value-0" || asString(values[2]) != "value-1" {
		t.Fatalf("sequential pipeline values = %#v; want successful commands around local error", values)
	}
	if _, ok := values[1].(error); !ok {
		t.Fatalf("sequential pipeline middle value = %#v; want build error", values[1])
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestNativeCompactFlowShortcutsYieldToLegacySession(t *testing.T) {
	client := NewClientWithExecutor(newNativeExecutor(defaultNativeOptions("127.0.0.1:6388", false)))
	client.legacyMu.Lock()
	client.setLegacySessionLocked(&responseCommandSession{response: []byte("OK")})
	client.legacyMu.Unlock()

	if _, ok, err := client.tryCreateManyNativeCompact(context.Background(), CreateManyOptions{
		Type:  "order",
		Items: []CreateItem{{ID: "one", Payload: []byte("payload")}},
	}, "queued", 1, 1, false, "AUTO"); err != nil || ok {
		t.Fatalf("create-many compact shortcut = ok %t, err %v; want session fallback", ok, err)
	}
	if _, ok, err := client.tryClaimDueNativeCompact(context.Background(), ClaimDueOptions{
		Type: "order", Worker: "worker",
	}, 1000, 1); err != nil || ok {
		t.Fatalf("claim-due compact shortcut = ok %t, err %v; want session fallback", ok, err)
	}
	if _, ok, err := client.tryCompleteManyNativeCompact(context.Background(), CompleteManyOptions{
		PartitionKey: "tenant",
		Items:        []ClaimedItem{{ID: "one", LeaseToken: "lease", FencingToken: 1}},
	}, 1); err != nil || ok {
		t.Fatalf("complete-many compact shortcut = ok %t, err %v; want session fallback", ok, err)
	}
}

func TestNativeSplitPipelinePreservesSuccessfulPrefixOnLaterFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	errCh := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"max_frame_bytes": int64(150)}); err != nil {
			errCh <- err
			return
		}
		first, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, first, nativeStatusOK, []any{[]any{"ok", "first"}}); err != nil {
			errCh <- err
			return
		}
		if _, err := readNativeRequestFrame(reader); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	exec := NewNativeExecutor(listener.Addr().String(),
		WithNativeTimeout(time.Second),
		WithNativeHeartbeat(0, 0),
		WithNativeReconnect(0),
	)
	defer func() { _ = exec.Close() }()
	longKey := strings.Repeat("k", 100)
	values, err := exec.Pipeline(context.Background(), [][]any{
		{"GET", longKey + "1"},
		{"GET", longKey + "2"},
		{"GET", longKey + "3"},
	})
	if err == nil {
		t.Fatal("expected the second split request to fail")
	}
	if len(values) != 3 || asString(values[0]) != "first" {
		t.Fatalf("split pipeline results = %#v; want successful prefix", values)
	}
	itemErr, ok := values[1].(error)
	if !ok || !errors.Is(err, itemErr) {
		t.Fatalf("second split result = %#v, aggregate error = %v", values[1], err)
	}
	notExecuted, ok := values[2].(error)
	if !ok || !errors.Is(notExecuted, ErrPipelineNotExecuted) {
		t.Fatalf("third split result = %#v; want ErrPipelineNotExecuted", values[2])
	}
	if serverErr := <-errCh; serverErr != nil {
		t.Fatal(serverErr)
	}
}

func TestNativeRotatesAcrossAdvertisedDataLanes(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	lanes := make(chan uint32, 4)
	errCh := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"multiplexing": map[string]any{"max_lanes_per_connection": int64(2)}}); err != nil {
			errCh <- err
			return
		}
		for i := 0; i < 4; i++ {
			frame, err := readNativeRequestFrame(reader)
			if err != nil {
				errCh <- err
				return
			}
			lanes <- frame.laneID
			if err := writeNativeTestResponse(writer, frame, nativeStatusOK, []byte("value")); err != nil {
				errCh <- err
				return
			}
		}
		errCh <- nil
	}()
	exec := NewNativeExecutor(listener.Addr().String(), WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0))
	defer func() { _ = exec.Close() }()
	for i := 0; i < 4; i++ {
		if _, err := exec.Do(context.Background(), "GET", fmt.Sprintf("key-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	close(lanes)
	got := make([]uint32, 0, 4)
	for lane := range lanes {
		got = append(got, lane)
	}
	if !reflect.DeepEqual(got, []uint32{1, 2, 1, 2}) {
		t.Fatalf("data lanes = %v, want [1 2 1 2]", got)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestNativeRejectsRequestAboveStartupFrameLimit(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	commandSeen := make(chan bool, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			return
		}
		_ = writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"limits": map[string]any{"max_frame_bytes": int64(64)}})
		_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		_, err = readNativeRequestFrame(reader)
		commandSeen <- err == nil
	}()
	exec := NewNativeExecutor(listener.Addr().String(), WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0), WithNativeReconnect(0))
	defer func() { _ = exec.Close() }()
	if _, err := exec.Do(context.Background(), "SET", "key", bytes.Repeat([]byte{'x'}, 256)); err == nil {
		t.Fatal("expected oversized request to be rejected")
	}
	if <-commandSeen {
		t.Fatal("oversized request reached the server")
	}
}

func TestNativeHonorsStartupInflightCredits(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	errCh := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"flow_control": map[string]any{"max_inflight_per_connection": int64(1), "max_inflight_per_lane": int64(1)}}); err != nil {
			errCh <- err
			return
		}
		first, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		second, earlyErr := readNativeRequestFrame(reader)
		_ = conn.SetReadDeadline(time.Time{})
		if earlyErr == nil {
			_ = writeNativeTestResponse(writer, first, nativeStatusOK, []byte("first"))
			_ = writeNativeTestResponse(writer, second, nativeStatusOK, []byte("second"))
			errCh <- errors.New("second request bypassed advertised inflight credit")
			return
		}
		if err := writeNativeTestResponse(writer, first, nativeStatusOK, []byte("first")); err != nil {
			errCh <- err
			return
		}
		second, err = readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		errCh <- writeNativeTestResponse(writer, second, nativeStatusOK, []byte("second"))
	}()
	exec := NewNativeExecutor(listener.Addr().String(), WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0))
	defer func() { _ = exec.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			_, err := exec.Do(ctx, "GET", fmt.Sprintf("key-%d", index))
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestNativeControlRequestsBypassZeroDataCredits(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	errCh := make(chan error, 1)
	releaseServer := make(chan struct{})
	var releaseServerOnce sync.Once
	release := func() { releaseServerOnce.Do(func() { close(releaseServer) }) }
	defer release()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{
			"flow_control": map[string]any{
				"max_inflight_per_connection": int64(0),
				"max_inflight_per_lane":       int64(0),
			},
		}); err != nil {
			errCh <- err
			return
		}
		ping, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		if ping.opcode != nativeOpPing {
			errCh <- errUnexpectedFrame(ping)
			return
		}
		if err := writeNativeTestResponse(writer, ping, nativeStatusOK, []byte("PONG")); err != nil {
			errCh <- err
			return
		}
		<-releaseServer
		errCh <- nil
	}()

	exec := NewNativeExecutor(listener.Addr().String(), WithNativeHeartbeat(0, 0))
	defer func() { _ = exec.Close() }()
	if got, err := exec.Do(context.Background(), "PING"); err != nil || asString(got) != "PONG" {
		t.Fatalf("control request with zero data credits = %#v, %v", got, err)
	}
	if _, err := exec.Do(context.Background(), "GET", "key"); err == nil {
		t.Fatal("data request bypassed zero STARTUP credits")
	}
	release()
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}
