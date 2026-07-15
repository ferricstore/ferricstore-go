package ferricstore

import (
	"bufio"
	"context"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestTopologyTransactionForKeysPinsOwningEndpointAndLane(t *testing.T) {
	seed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = seed.Close() })
	routed, frames, errCh := startRoutedNativeEndpoint(t, func(_ nativeFrame, request int) any {
		switch request {
		case 0:
			return []byte("OK")
		case 1:
			return []byte("QUEUED")
		default:
			return []any{[]byte("OK")}
		}
	})
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(t, seed, routed)
	t.Cleanup(func() { _ = exec.Close() })
	client := NewClientWithExecutor(exec)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := client.TransactionForKeys(ctx, keyA, keyB); err == nil {
		t.Fatal("expected cross-slot topology transaction to fail")
	}
	tx, err := client.TransactionForKeys(ctx, keyB)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Command(ctx, "SET", keyB, "value"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	for index, wantOpcode := range []uint16{nativeOpCommandExec, nativeOpCommandExec, nativeOpCommandExec} {
		select {
		case frame := <-frames:
			if frame.opcode != wantOpcode || frame.laneID != 2 {
				t.Fatalf("frame %d = opcode %d lane %d; want opcode %d lane 2", index, frame.opcode, frame.laneID, wantOpcode)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for transaction frame %d", index)
		}
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestTopologyRejectsMixedSeedTransports(t *testing.T) {
	_, err := NewTopologyNativeExecutor([]string{
		"ferric://db.example:6388",
		"ferrics://db.example:6389",
	})
	if err == nil {
		t.Fatal("expected mixed plaintext and TLS seed URLs to be rejected")
	}
}

func TestTopologyRejectsNativeTransportOptionsThatConflictWithSeedScheme(t *testing.T) {
	tests := []struct {
		name string
		url  string
		opt  NativeOption
	}{
		{name: "TLS option with plaintext seed", url: "ferric://db.example:6388", opt: WithNativeTLS(nil)},
		{name: "plaintext option with TLS seed", url: "ferrics://db.example:6389", opt: func(options *NativeOptions) { options.TLS = false }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec, err := NewTopologyNativeExecutor(
				[]string{test.url},
				WithTopologyNativeOptions(test.opt),
			)
			if exec != nil {
				_ = exec.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "seed URL scheme") {
				t.Fatalf("transport conflict error = %v", err)
			}
		})
	}
}

func TestTopologyAdapterRejectsConditionalNativeTransportConflict(t *testing.T) {
	exec, err := NewTopologyNativeExecutor(
		[]string{"ferric://seed.example:6388"},
		WithTopologyEndpointPolicy(EndpointPolicyAny),
		WithTopologyNativeOptions(func(options *NativeOptions) {
			if strings.Contains(options.Addr, "learned.example") {
				options.TLS = true
			}
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()

	adapter, err := exec.adapterForURL("ferric://learned.example:6388")
	if adapter != nil {
		_ = adapter.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "seed URL scheme") {
		t.Fatalf("conditional transport conflict error = %v", err)
	}
}

func TestTopologyRefreshCandidatesExcludeUntrustedLearnedEndpoints(t *testing.T) {
	exec, err := NewTopologyNativeExecutor([]string{"ferric://seed.example:6388"})
	if err != nil {
		t.Fatal(err)
	}
	exec.topology = &RoutingTopology{endpoints: map[string]RoutingEndpoint{
		"unsafe.internal:6388": {Host: "unsafe.internal", NativePort: 6388},
	}}

	for _, candidate := range exec.refreshCandidateURLs() {
		if strings.Contains(candidate, "unsafe.internal") {
			t.Fatalf("untrusted learned endpoint was accepted as a refresh candidate: %s", candidate)
		}
	}
}

func TestTopologyRefreshPrioritizesLastSuccessfulEndpoint(t *testing.T) {
	exec, err := NewTopologyNativeExecutor([]string{"ferric://seed.example:6388"}, WithTopologyEndpointPolicy(EndpointPolicyAny))
	if err != nil {
		t.Fatal(err)
	}
	exec.lastSuccessfulURL = "ferric://leader.example:6388"
	exec.topology = &RoutingTopology{endpoints: map[string]RoutingEndpoint{
		"leader": {Host: "leader.example", NativePort: 6388},
	}}
	candidates := exec.refreshCandidateURLs()
	if len(candidates) == 0 || candidates[0] != exec.lastSuccessfulURL {
		t.Fatalf("refresh candidates = %#v, want last successful first", candidates)
	}
}

func TestTopologyControlCommandFailsOverSeeds(t *testing.T) {
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := dead.Addr().String()
	_ = dead.Close()

	live, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = live.Close() }()
	errCh := make(chan error, 1)
	go func() {
		conn, err := live.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer func() { _ = conn.Close() }()
		errCh <- serveNativeWireTest(conn)
	}()

	exec, err := NewTopologyNativeExecutor(
		[]string{"ferric://" + deadAddr, "ferric://" + live.Addr().String()},
		WithTopologyNativeOptions(
			WithNativeTimeout(200*time.Millisecond),
			WithNativeHeartbeat(0, 0),
			WithNativeReconnect(0),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	value, err := exec.Do(ctx, "PING", "hello")
	if err != nil {
		t.Fatal(err)
	}
	if asString(value) != "PONG" {
		t.Fatalf("unexpected failover response: %#v", value)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestTopologyClosePreventsAdapterRecreation(t *testing.T) {
	exec, err := NewTopologyNativeExecutor([]string{"ferric://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := exec.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = exec.Do(ctx, "PING")
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "closed") {
		t.Fatalf("expected closed topology executor error, got %v", err)
	}
	if len(exec.adapters) != 0 {
		t.Fatalf("closed topology executor recreated adapters: %#v", exec.adapters)
	}
}

func TestTopologyCloseIncludesAdaptersAlreadyRetiring(t *testing.T) {
	adapter := NewNativeExecutor("127.0.0.1:1")
	adapter.mu.Lock()
	adapter.activeRequests = 1
	adapter.mu.Unlock()
	exec := &TopologyNativeExecutor{
		adapters:         map[string]*NativeExecutor{"old": adapter},
		seedEndpointKeys: map[string]struct{}{},
		topology:         emptyRoutingTopology(),
	}

	if err := exec.installTopology(emptyRoutingTopology()); err != nil {
		t.Fatal(err)
	}
	adapter.mu.Lock()
	retiring, closed := adapter.retiring, adapter.isClosed
	adapter.mu.Unlock()
	if !retiring || closed {
		t.Fatalf("expected active stale adapter to be retiring, got retiring=%t closed=%t", retiring, closed)
	}

	if err := exec.Close(); err != nil {
		t.Fatal(err)
	}
	adapter.mu.Lock()
	closed = adapter.isClosed
	adapter.mu.Unlock()
	if !closed {
		t.Fatal("topology close lost ownership of an adapter that was still retiring")
	}
}

func TestTopologyWarmConnectionsAndRetiresRemovedEndpoints(t *testing.T) {
	oldListener, oldReady, oldClosed := startTopologyEndpoint(t)
	newListener, newReady, _ := startTopologyEndpoint(t)
	seed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = seed.Close() }()
	go serveTopologySequence(seed, []RoutingEndpoint{
		topologyEndpointFromListener(t, oldListener),
		topologyEndpointFromListener(t, newListener),
	})

	exec, err := NewTopologyNativeExecutor(
		[]string{"ferric://" + seed.Addr().String()},
		WithTopologyTrustedHosts("127.0.0.1"),
		WithTopologyWarmConnections(true),
		WithTopologyNativeOptions(
			WithNativeTimeout(time.Second),
			WithNativeHeartbeat(0, 0),
			WithNativeReconnect(0),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := exec.RefreshTopology(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-oldReady:
	case <-time.After(time.Second):
		t.Fatal("warm topology endpoint was not connected")
	}
	if err := exec.RefreshTopology(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-newReady:
	case <-time.After(time.Second):
		t.Fatal("replacement topology endpoint was not warmed")
	}
	select {
	case <-oldClosed:
	case <-time.After(time.Second):
		t.Fatal("removed topology endpoint remained connected")
	}
}

func startTopologyEndpoint(t *testing.T) (net.Listener, <-chan struct{}, <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	ready := make(chan struct{})
	closed := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		reader := bufio.NewReader(conn)
		writer := bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			return
		}
		close(ready)
		var one [1]byte
		_, _ = conn.Read(one[:])
		close(closed)
	}()
	return listener, ready, closed
}

func topologyEndpointFromListener(t *testing.T, listener net.Listener) RoutingEndpoint {
	t.Helper()
	host, rawPort, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		t.Fatal(err)
	}
	return RoutingEndpoint{Node: host, Host: host, NativePort: port}
}

func serveTopologySequence(listener net.Listener, endpoints []RoutingEndpoint) {
	conn, err := listener.Accept()
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	startup, err := readNativeRequestFrame(reader)
	if err != nil {
		return
	}
	if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
		return
	}
	for epoch, endpoint := range endpoints {
		request, err := readNativeRequestFrame(reader)
		if err != nil {
			return
		}
		payload := map[string]any{
			"route_epoch": int64(epoch + 1),
			"shard_count": int64(1),
			"ranges": []any{map[string]any{
				"first_slot": int64(0), "last_slot": int64(1023), "shard": int64(0), "lane_id": int64(1),
				"endpoint": map[string]any{
					"node": endpoint.Node, "host": endpoint.Host, "native_port": int64(endpoint.NativePort),
				},
			}},
		}
		if err := writeNativeTestResponse(writer, request, nativeStatusOK, payload); err != nil {
			return
		}
	}
}

func TestRoutingTopologyRejectsLeaderUnknown(t *testing.T) {
	_, err := buildRoutingTopology(map[string]any{
		"ranges": []any{
			map[string]any{"hint": "leader_unknown"},
		},
	})
	if err == nil {
		t.Fatal("expected leader_unknown topology range to fail")
	}
}

func TestParseFerricURLRejectsInvalidPort(t *testing.T) {
	if _, err := parseFerricURL("ferric://127.0.0.1:not-a-port"); err == nil {
		t.Fatal("expected invalid URL port to fail")
	}
}
