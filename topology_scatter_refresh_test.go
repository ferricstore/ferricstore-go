package ferricstore

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"
)

func TestGenericScatterRefreshesTopologyAfterRetryableRouteError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	endpoint := topologyEndpointFromListener(t, listener)
	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			serverErr <- err
			return
		}
		request, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if err := writeNativeTestResponse(writer, request, nativeStatusReroute, map[string]any{
			"code":          "reroute",
			"message":       "stale epoch",
			"retryable":     true,
			"safe_to_retry": false,
		}); err != nil {
			serverErr <- err
			return
		}
		refresh, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		serverErr <- writeNativeTestResponse(writer, refresh, nativeStatusOK, topologyResponseForEndpoint(endpoint, 2))
	}()

	exec, err := NewTopologyNativeExecutor(
		[]string{"ferric://" + listener.Addr().String()},
		WithTopologyEndpointPolicy(EndpointPolicyAny),
		WithTopologyNativeOptions(WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0), WithNativeReconnect(0)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	if err := exec.installTopology(topologyForEndpoint(endpoint, 1)); err != nil {
		t.Fatal(err)
	}

	_, err = exec.Do(context.Background(), "MGET", "key")
	if err == nil {
		t.Fatal("MGET unexpectedly succeeded after retryable route error")
	}
	exec.mu.Lock()
	epoch, version := exec.topology.RouteEpoch, exec.topologyVersion
	exec.mu.Unlock()
	if epoch != 2 || version != 2 {
		t.Fatalf("topology after retryable scatter error = epoch %d, version %d; want epoch 2, version 2", epoch, version)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestGenericScatterRetriesServerDeclaredSafeReroute(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	endpoint := topologyEndpointFromListener(t, listener)
	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			serverErr <- err
			return
		}
		request, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if err := writeNativeTestResponse(writer, request, nativeStatusReroute, map[string]any{
			"code":          "reroute",
			"message":       "stale epoch",
			"retryable":     true,
			"safe_to_retry": true,
		}); err != nil {
			serverErr <- err
			return
		}
		refresh, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if err := writeNativeTestResponse(writer, refresh, nativeStatusOK, topologyResponseForEndpoint(endpoint, 2)); err != nil {
			serverErr <- err
			return
		}
		retry, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		serverErr <- writeNativeTestResponse(writer, retry, nativeStatusOK, []any{[]byte("value")})
	}()

	exec, err := NewTopologyNativeExecutor(
		[]string{"ferric://" + listener.Addr().String()},
		WithTopologyEndpointPolicy(EndpointPolicyAny),
		WithTopologyNativeOptions(WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0), WithNativeReconnect(0)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	if err := exec.installTopology(topologyForEndpoint(endpoint, 1)); err != nil {
		t.Fatal(err)
	}

	value, err := exec.Do(context.Background(), "MGET", "key")
	if err != nil {
		t.Fatalf("safe reroute was not retried: %v", err)
	}
	items, ok := value.([]any)
	if !ok || len(items) != 1 || string(items[0].([]byte)) != "value" {
		t.Fatalf("safe reroute result = %#v", value)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestTypedKVScatterRetriesServerDeclaredSafeReroute(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	endpoint := topologyEndpointFromListener(t, listener)
	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			serverErr <- err
			return
		}
		request, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if err := writeNativeTestResponse(writer, request, nativeStatusReroute, map[string]any{
			"code":          "reroute",
			"message":       "stale epoch",
			"retryable":     true,
			"safe_to_retry": true,
		}); err != nil {
			serverErr <- err
			return
		}
		refresh, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if err := writeNativeTestResponse(writer, refresh, nativeStatusOK, topologyResponseForEndpoint(endpoint, 2)); err != nil {
			serverErr <- err
			return
		}
		retry, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		serverErr <- writeNativeTestResponse(writer, retry, nativeStatusOK, []any{[]byte("value")})
	}()

	exec, err := NewTopologyNativeExecutor(
		[]string{"ferric://" + listener.Addr().String()},
		WithTopologyEndpointPolicy(EndpointPolicyAny),
		WithTopologyNativeOptions(WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0), WithNativeReconnect(0)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	if err := exec.installTopology(topologyForEndpoint(endpoint, 1)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := exec.captureRoutingTopology()
	if err != nil {
		t.Fatal(err)
	}
	route := *snapshot.topology.slots[routeSlotForString("key")]
	groups := map[topologyRouteIdentity]*stringKeyRouteGroup{
		routeIdentity(route): {route: route, keys: []string{"key"}, positions: []int{0}},
	}

	value, err := exec.scatterStringMGet(context.Background(), []string{"key"}, groups, snapshot)
	if err != nil {
		t.Fatalf("typed safe reroute was not retried: %v", err)
	}
	items, ok := value.([]any)
	if !ok || len(items) != 1 || string(items[0].([]byte)) != "value" {
		t.Fatalf("typed safe reroute result = %#v", value)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestTypedKVCountScatterRetriesServerDeclaredSafeReroute(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	endpoint := topologyEndpointFromListener(t, listener)
	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer func() { _ = conn.Close() }()
		reader, writer := bufio.NewReader(conn), bufio.NewWriter(conn)
		startup, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			serverErr <- err
			return
		}
		request, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if err := writeNativeTestResponse(writer, request, nativeStatusReroute, map[string]any{
			"code":          "reroute",
			"message":       "stale epoch",
			"retryable":     true,
			"safe_to_retry": true,
		}); err != nil {
			serverErr <- err
			return
		}
		refresh, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		if err := writeNativeTestResponse(writer, refresh, nativeStatusOK, topologyResponseForEndpoint(endpoint, 2)); err != nil {
			serverErr <- err
			return
		}
		retry, err := readNativeRequestFrame(reader)
		if err != nil {
			serverErr <- err
			return
		}
		serverErr <- writeNativeTestResponse(writer, retry, nativeStatusOK, int64(1))
	}()

	exec, err := NewTopologyNativeExecutor(
		[]string{"ferric://" + listener.Addr().String()},
		WithTopologyEndpointPolicy(EndpointPolicyAny),
		WithTopologyNativeOptions(WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0), WithNativeReconnect(0)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	if err := exec.installTopology(topologyForEndpoint(endpoint, 1)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := exec.captureRoutingTopology()
	if err != nil {
		t.Fatal(err)
	}
	route := *snapshot.topology.slots[routeSlotForString("key")]
	groups := map[topologyRouteIdentity]*stringKeyRouteGroup{
		routeIdentity(route): {route: route, keys: []string{"key"}},
	}

	value, err := exec.scatterStringCountCommand(context.Background(), "EXISTS", groups, snapshot)
	if err != nil {
		t.Fatalf("typed count safe reroute was not retried: %v", err)
	}
	if value != int64(1) {
		t.Fatalf("typed count safe reroute result = %#v", value)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func topologyForEndpoint(endpoint RoutingEndpoint, epoch int64) *RoutingTopology {
	topology := &RoutingTopology{
		RouteEpoch: epoch,
		ShardCount: 1,
		endpoints:  map[string]RoutingEndpoint{endpointKey(endpoint): endpoint},
	}
	route := &RoutingRoute{Shard: 0, LaneID: 1, EndpointKey: endpointKey(endpoint), Endpoint: endpoint}
	for slot := range topology.slots {
		topology.slots[slot] = route
	}
	return topology
}

func topologyResponseForEndpoint(endpoint RoutingEndpoint, epoch int64) map[string]any {
	return map[string]any{
		"route_epoch": epoch,
		"shard_count": int64(1),
		"ranges": []any{map[string]any{
			"first_slot": int64(0), "last_slot": int64(routeSlotCount - 1),
			"shard": int64(0), "lane_id": int64(1),
			"endpoint": map[string]any{
				"node": endpoint.Node, "host": endpoint.Host, "native_port": int64(endpoint.NativePort),
			},
		}},
	}
}
