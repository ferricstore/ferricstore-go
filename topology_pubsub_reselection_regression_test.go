package ferricstore

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestTopologyPubSubReselectsRetiredLearnedEndpoint(t *testing.T) {
	oldListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = oldListener.Close() }()
	newListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = newListener.Close() }()
	unusedSeed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	seedAddress := unusedSeed.Addr().String()
	_ = unusedSeed.Close()

	oldDone := make(chan error, 1)
	go func() {
		conn, err := oldListener.Accept()
		if err != nil {
			oldDone <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		if err := serveNativeStartup(reader, writer); err != nil {
			oldDone <- err
			return
		}
		subscribe, err := readNativeRequestFrame(reader)
		if err != nil {
			oldDone <- err
			return
		}
		if err := writeNativeTestResponse(writer, subscribe, nativeStatusOK, []any{"subscribe", "jobs", int64(1)}); err != nil {
			oldDone <- err
			return
		}
		if _, err := readNativeRequestFrame(reader); err == nil {
			oldDone <- fmt.Errorf("retired endpoint received a command")
			return
		}
		oldDone <- nil
	}()

	newDone := make(chan error, 1)
	go func() {
		conn, err := newListener.Accept()
		if err != nil {
			newDone <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		if err := serveNativeStartup(reader, writer); err != nil {
			newDone <- err
			return
		}
		replay, err := readNativeRequestFrame(reader)
		if err != nil {
			newDone <- err
			return
		}
		if replay.opcode != nativeOpCommandExec {
			newDone <- fmt.Errorf("replay opcode = %d, want COMMAND_EXEC", replay.opcode)
			return
		}
		if err := writeNativeTestResponse(writer, replay, nativeStatusOK, []any{"subscribe", "jobs", int64(1)}); err != nil {
			newDone <- err
			return
		}
		subscribe, err := readNativeRequestFrame(reader)
		if err != nil {
			newDone <- err
			return
		}
		newDone <- writeNativeTestResponse(writer, subscribe, nativeStatusOK, []any{"subscribe", "alerts", int64(2)})
	}()

	exec, err := NewTopologyNativeExecutor(
		[]string{"ferric://" + seedAddress},
		WithTopologyEndpointPolicy(EndpointPolicyAny),
		WithTopologyNativeOptions(
			WithNativeTimeout(100*time.Millisecond),
			WithNativeHeartbeat(0, 0),
			WithNativeReconnect(0),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	oldEndpoint := topologyEndpointFromListener(t, oldListener)
	if err := exec.installTopology(topologyForEndpoint(oldEndpoint, 1)); err != nil {
		t.Fatal(err)
	}

	pubsub, err := NewClientWithExecutor(exec).OpenPubSub()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pubsub.Subscribe(context.Background(), "jobs"); err != nil {
		t.Fatal(err)
	}
	newEndpoint := topologyEndpointFromListener(t, newListener)
	if err := exec.installTopology(topologyForEndpoint(newEndpoint, 2)); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	message, err := pubsub.Subscribe(ctx, "alerts")
	if err != nil {
		t.Fatal(err)
	}
	if message.Kind != "subscribe" || message.Channel != "alerts" || message.Count != 2 {
		t.Fatalf("subscription acknowledgement = %#v", message)
	}
	if err := <-oldDone; err != nil {
		t.Fatal(err)
	}
	if err := <-newDone; err != nil {
		t.Fatal(err)
	}
}
