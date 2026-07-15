package ferricstore

import (
	"bufio"
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestTopologyAbandonedUnlimitedRefreshIsReplaced(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	endpoint := topologyEndpointFromListener(t, listener)
	shardsRead := make(chan struct{})
	serverErr := make(chan error, 1)
	go func() {
		first, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		reader, writer := bufio.NewReader(first), bufio.NewWriter(first)
		startup, err := readNativeRequestFrame(reader)
		if err == nil {
			err = writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true})
		}
		if err == nil {
			_, err = readNativeRequestFrame(reader)
		}
		if err != nil {
			serverErr <- err
			return
		}
		close(shardsRead)
		_ = first.SetReadDeadline(time.Now().Add(750 * time.Millisecond))
		if _, err = reader.ReadByte(); err == nil {
			serverErr <- errors.New("abandoned topology refresh remained readable")
			return
		} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
			serverErr <- errors.New("abandoned topology refresh connection was not closed")
			return
		}
		_ = first.Close()

		second, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer func() { _ = second.Close() }()
		reader, writer = bufio.NewReader(second), bufio.NewWriter(second)
		startup, err = readNativeRequestFrame(reader)
		if err == nil {
			err = writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true})
		}
		var request nativeFrame
		if err == nil {
			request, err = readNativeRequestFrame(reader)
		}
		if err == nil {
			err = writeNativeTestResponse(writer, request, nativeStatusOK, map[string]any{
				"route_epoch": int64(1), "shard_count": int64(1),
				"ranges": []any{map[string]any{
					"first_slot": int64(0), "last_slot": int64(1023),
					"shard": int64(0), "lane_id": int64(1),
					"endpoint": map[string]any{
						"node": endpoint.Node, "host": endpoint.Host,
						"native_port": int64(endpoint.NativePort),
					},
				}},
			})
		}
		serverErr <- err
	}()

	exec, err := NewTopologyNativeExecutor(
		[]string{"ferric://" + listener.Addr().String()},
		WithTopologyNativeOptions(
			WithNativeTimeout(0), WithNativeHeartbeat(0, 0), WithNativeReconnect(0),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	firstResult := make(chan error, 1)
	go func() { firstResult <- exec.RefreshTopology(ctx) }()
	select {
	case <-shardsRead:
	case <-time.After(time.Second):
		t.Fatal("server did not receive first SHARDS request")
	}
	if err := <-firstResult; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first refresh error = %v; want deadline exceeded", err)
	}

	retryCtx, retryCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer retryCancel()
	if err := exec.RefreshTopology(retryCtx); err != nil {
		t.Fatalf("replacement topology refresh failed: %v", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}
