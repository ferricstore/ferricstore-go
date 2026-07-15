package ferricstore

import (
	"bufio"
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

func TestInstallTopologyAcceptsChangedLowerRouteEpoch(t *testing.T) {
	current := topologyForEndpoint(RoutingEndpoint{Host: "old.example", NativePort: 6388}, 10)
	exec := &TopologyNativeExecutor{
		adapters:         make(map[string]*NativeExecutor),
		retiringAdapters: make(map[*NativeExecutor]struct{}),
		seedEndpointKeys: make(map[string]struct{}),
		endpointPolicy:   EndpointPolicyAny,
		topology:         current,
		topologyVersion:  7,
	}

	changed := topologyForEndpoint(RoutingEndpoint{Host: "new.example", NativePort: 6389}, 9)
	if err := exec.installTopology(changed); err != nil {
		t.Fatal(err)
	}

	if exec.topology != changed {
		t.Fatalf("topology with lower hash epoch %d was not installed over %d", changed.RouteEpoch, current.RouteEpoch)
	}
	if exec.topologyVersion != 8 {
		t.Fatalf("topology version = %d; want 8 after changed snapshot", exec.topologyVersion)
	}
	route, err := exec.Route("key")
	if err != nil {
		t.Fatal(err)
	}
	if route.Endpoint.Host != "new.example" || route.Endpoint.NativePort != 6389 {
		t.Fatalf("installed route endpoint = %+v; want new.example:6389", route.Endpoint)
	}
}

func TestInstallTopologyKeepsIdenticalSnapshotCurrent(t *testing.T) {
	endpoint := RoutingEndpoint{Node: "node-a", Host: "node-a.example", NativePort: 6388}
	current := topologyForEndpoint(endpoint, 10)
	exec := &TopologyNativeExecutor{
		adapters:         make(map[string]*NativeExecutor),
		retiringAdapters: make(map[*NativeExecutor]struct{}),
		seedEndpointKeys: make(map[string]struct{}),
		endpointPolicy:   EndpointPolicyAny,
		topology:         current,
		topologyVersion:  7,
	}
	snapshot, err := exec.captureRoutingTopology()
	if err != nil {
		t.Fatal(err)
	}

	if err := exec.installTopology(topologyForEndpoint(endpoint, 10)); err != nil {
		t.Fatal(err)
	}

	if exec.topology != current {
		t.Fatal("identical topology replaced the installed snapshot")
	}
	if exec.topologyVersion != 7 {
		t.Fatalf("topology version = %d; want 7 after identical snapshot", exec.topologyVersion)
	}
	if !exec.routingSnapshotCurrent(snapshot) {
		t.Fatal("identical topology invalidated an in-flight routing snapshot")
	}
}

func TestInstallTopologyRejectsUnsafeEndpointsWithoutReplacingCurrentRoutes(t *testing.T) {
	seed := RoutingEndpoint{Node: "seed", Host: "seed.example", NativePort: 6388}
	seedKey := connectionKeyForEndpoint(seed, false)
	current := topologyForEndpoint(seed, 1)
	adapter := NewNativeExecutor("seed.example:6388", WithNativeHeartbeat(0, 0))
	t.Cleanup(func() { _ = adapter.Close() })
	exec := &TopologyNativeExecutor{
		adapters:         map[string]*NativeExecutor{seedKey: adapter},
		retiringAdapters: make(map[*NativeExecutor]struct{}),
		seedEndpointKeys: map[string]struct{}{seedKey: {}},
		endpointPolicy:   EndpointPolicySeedHosts,
		topology:         current,
		topologyVersion:  3,
	}

	unsafe := topologyForEndpoint(RoutingEndpoint{
		Node: "untrusted", Host: "untrusted.example", NativePort: 6388,
	}, 2)
	if err := exec.installTopology(unsafe); err == nil {
		t.Fatal("topology containing an unsafe learned endpoint was installed")
	}
	if exec.topology != current || exec.topologyVersion != 3 {
		t.Fatalf("unsafe topology replaced current routes at version %d", exec.topologyVersion)
	}
	if exec.adapters[seedKey] != adapter {
		t.Fatal("unsafe topology retired the current seed adapter")
	}
}

func TestRouteDataReturnsRefreshCancellation(t *testing.T) {
	exec := &TopologyNativeExecutor{topology: emptyRoutingTopology()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := exec.routeData(ctx, []any{"GET", "missing"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("routeData error = %v; want context.Canceled", err)
	}
}

func TestFlowRoutingDoesNotTreatOptionValuesAsPartitionMarkers(t *testing.T) {
	want, ok := flowAutoIDRoutingKey("job-1")
	if !ok {
		t.Fatal("failed to build expected Flow auto-ID routing key")
	}
	tests := []struct {
		name string
		args []any
	}{
		{
			name: "type value",
			args: []any{"FLOW.CREATE", "job-1", "TYPE", "PARTITION", "STATE", "queued", "NOW", int64(1)},
		},
		{
			name: "state value",
			args: []any{"FLOW.CREATE", "job-1", "TYPE", "email", "STATE", "PARTITION", "NOW", int64(1)},
		},
		{
			name: "payload value",
			args: []any{"FLOW.CREATE", "job-1", "TYPE", "email", "STATE", "queued", "PAYLOAD", "PARTITION", "NOW", int64(1)},
		},
		{
			name: "named value payload",
			args: []any{
				"FLOW.CREATE", "job-1", "TYPE", "email", "STATE", "queued", "NOW", int64(1),
				"VALUE", "first", "PARTITION", "VALUE", "second", "ordinary",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, routed := routingKeyForCommand(tc.args)
			if !routed || asString(got) != asString(want) {
				t.Fatalf("routingKeyForCommand(%#v) = %#v, %t; want %#v, true", tc.args, got, routed, want)
			}
		})
	}
}

func TestCommandExecRoutingRemovesRequestContextMetadata(t *testing.T) {
	ctx := &RequestContext{Subject: "worker"}
	first, second := "first:{tenant}", "second:{tenant}"
	args := appendNativeRequestContext([]any{
		"COMMAND_EXEC", "MSET", first, "1", second, "2",
	}, ctx)
	key, ok := routingKeyForCommand(args)
	if !ok || asString(key) != first {
		t.Fatalf("wrapped MSET route = %#v, %t; want %q", key, ok, first)
	}
	keys, required := sameSlotCommandKeys(args)
	if !required || len(keys) != 2 || asString(keys[0]) != first || asString(keys[1]) != second {
		t.Fatalf("wrapped MSET same-slot keys = %#v, %t", keys, required)
	}

	scatterArgs := appendNativeRequestContext([]any{"COMMAND_EXEC", "UNLINK", first, second}, ctx)
	name, scatterKeys, scatter := safeScatterCommand(scatterArgs)
	if !scatter || name != "UNLINK" || len(scatterKeys) != 2 {
		t.Fatalf("wrapped UNLINK scatter = %q, %#v, %t", name, scatterKeys, scatter)
	}
}

func TestFlowRoutingUsesCommandSpecificOptionBoundaries(t *testing.T) {
	want, ok := flowLogicalPartitionRoutingKey("tenant:1")
	if !ok {
		t.Fatal("failed to build expected partition routing key")
	}
	tests := [][]any{
		{"FLOW.BUDGET.LIST", "SCOPE", "budget", "PARTITION", "tenant:1"},
		{"FLOW.LIMIT.LIST", "SCOPE", "limit", "PARTITION", "tenant:1"},
		{"FLOW.ATTRIBUTE_VALUES", "order", "region", "PARTITION", "tenant:1"},
	}
	for _, args := range tests {
		got, routed := routingKeyForCommand(args)
		if !routed || asString(got) != asString(want) {
			t.Errorf("routingKeyForCommand(%#v) = %#v, %t; want %#v, true", args, got, routed, want)
		}
	}
}

func TestTopologyRejectsCrossShardDestructiveScatterByDefault(t *testing.T) {
	listenerA, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return int64(1) })
	listenerB, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return int64(1) })
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(t, listenerA, listenerB)
	t.Cleanup(func() { _ = exec.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := exec.Do(ctx, "DEL", keyA, keyB)
	if err == nil {
		t.Fatal("expected cross-shard DEL to require an explicit write policy")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "cross-shard") {
		t.Fatalf("cross-shard DEL error = %v", err)
	}
}

func TestTopologyOptInReportsPartialCrossShardWrite(t *testing.T) {
	listenerA, _, errsA := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return int64(1) })
	listenerB, errsB := startFailingRoutedNativeEndpoint(t)
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(
		t, listenerA, listenerB,
		WithTopologyCrossShardWritePolicy(CrossShardWritePerShard),
	)
	t.Cleanup(func() { _ = exec.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := exec.Do(ctx, "DEL", keyA, keyB)
	var partial *TopologyPartialWriteError
	if !errors.As(err, &partial) {
		t.Fatalf("cross-shard DEL error = %T %v; want TopologyPartialWriteError", err, err)
	}
	if partial.Command != "DEL" || partial.Succeeded != 1 || len(partial.Failures) != 1 {
		t.Fatalf("partial write = %+v; want DEL with one success and one failure", partial)
	}
	assertTopologyWriteFailure(t, partial.Failures[0], keyB, listenerB)
	if err := <-errsA; err != nil {
		t.Fatal(err)
	}
	if err := <-errsB; err != nil {
		t.Fatal(err)
	}
}

func TestTopologyGenericPartialWriteIdentifiesFailedRoute(t *testing.T) {
	listenerA, _, errsA := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return int64(1) })
	listenerB, errsB := startFailingRoutedNativeEndpoint(t)
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(
		t, listenerA, listenerB,
		WithTopologyCrossShardWritePolicy(CrossShardWritePerShard),
	)
	t.Cleanup(func() { _ = exec.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := exec.Do(ctx, "UNLINK", keyA, keyB)
	var partial *TopologyPartialWriteError
	if !errors.As(err, &partial) {
		t.Fatalf("cross-shard UNLINK error = %T %v; want TopologyPartialWriteError", err, err)
	}
	if partial.Command != "UNLINK" || partial.Succeeded != 1 || len(partial.Failures) != 1 {
		t.Fatalf("partial write = %+v; want UNLINK with one success and one failure", partial)
	}
	assertTopologyWriteFailure(t, partial.Failures[0], keyB, listenerB)
	if err := <-errsA; err != nil {
		t.Fatal(err)
	}
	if err := <-errsB; err != nil {
		t.Fatal(err)
	}
}

func assertTopologyWriteFailure(t *testing.T, err error, wantKey string, listener net.Listener) {
	t.Helper()
	var failure *TopologyWriteFailure
	if !errors.As(err, &failure) {
		t.Fatalf("partial failure = %T %v; want TopologyWriteFailure", err, err)
	}
	if len(failure.Keys) != 1 || failure.Keys[0] != wantKey {
		t.Fatalf("partial failure keys = %#v; want [%q]", failure.Keys, wantKey)
	}
	wantPort := listener.Addr().(*net.TCPAddr).Port
	if failure.Route.Endpoint.NativePort != wantPort {
		t.Fatalf("partial failure route = %+v; want native port %d", failure.Route, wantPort)
	}
	var nativeErr NativeError
	if !errors.As(failure, &nativeErr) {
		t.Fatalf("partial failure cause = %T %v; want NativeError", failure.Err, failure.Err)
	}
}

func startFailingRoutedNativeEndpoint(t *testing.T) (net.Listener, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
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
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			errCh <- err
			return
		}
		request, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		errCh <- writeNativeTestResponse(writer, request, 1, map[string]any{"message": "shard write failed"})
	}()
	return listener, errCh
}
