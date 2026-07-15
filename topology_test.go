package ferricstore

import (
	"bufio"
	"context"
	"errors"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRoutingTopologyBuildRouteAndEndpointPolicy(t *testing.T) {
	payload := map[string]any{
		"route_epoch": int64(7),
		"shard_count": int64(1),
		"ranges": []any{
			map[string]any{
				"first_slot": int64(0),
				"last_slot":  int64(1023),
				"shard":      int64(0),
				"lane_id":    int64(3),
				"endpoint": map[string]any{
					"node":            "node-a",
					"host":            "127.0.0.1",
					"native_port":     int64(6388),
					"native_tls_port": int64(6389),
				},
			},
		},
	}

	topology, err := buildRoutingTopology(payload)
	if err != nil {
		t.Fatal(err)
	}
	route, err := topology.RouteKey("tenant:1:key")
	if err != nil {
		t.Fatal(err)
	}
	if route.Shard != 0 || route.LaneID != 3 || route.Endpoint.Host != "127.0.0.1" || route.Endpoint.NativePort != 6388 {
		t.Fatalf("unexpected route: %#v", route)
	}

	pool := &TopologyNativeExecutor{
		endpointPolicy:   EndpointPolicySeedHosts,
		seedEndpointKeys: stringSet(connectionKeyForEndpoint(RoutingEndpoint{Host: "127.0.0.1", NativePort: 6388}, false)),
	}
	if err := pool.validateEndpoint(RoutingEndpoint{Host: "127.0.0.1", NativePort: 6388}); err != nil {
		t.Fatalf("expected exact seed endpoint to be trusted: %v", err)
	}
	if err := pool.validateEndpoint(RoutingEndpoint{Host: "127.0.0.1", NativePort: 6389}); err == nil {
		t.Fatal("expected same-host different-port learned endpoint to be rejected by default")
	}
	pool.trustedHosts = stringSet("127.0.0.1")
	if err := pool.validateEndpoint(RoutingEndpoint{Host: "127.0.0.1", NativePort: 6389}); err != nil {
		t.Fatalf("expected trusted host opt-in to allow learned port: %v", err)
	}
}

func TestTopologyTLSSeedMatchesLearnedTLSEndpoint(t *testing.T) {
	exec, err := NewTopologyNativeExecutor([]string{"ferrics://db.example:6389"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()

	err = exec.validateEndpoint(RoutingEndpoint{
		Host:          "db.example",
		NativePort:    6388,
		NativeTLSPort: 6389,
	})
	if err != nil {
		t.Fatalf("TLS seed and learned TLS socket should have the same identity: %v", err)
	}
}

func TestTopologyClientOwnsExecutorAndAppliesClientOptions(t *testing.T) {
	client, err := NewTopologyClientFromURLs(
		[]string{"ferric://127.0.0.1:6388"},
		WithTopologyClientOptions(WithCodec(StringCodec{})),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := client.Codec().(StringCodec); !ok {
		t.Fatalf("topology client codec = %T, want StringCodec", client.Codec())
	}
	exec, ok := client.exec.(*TopologyNativeExecutor)
	if !ok {
		t.Fatalf("topology client executor = %T", client.exec)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := exec.assertOpen(); !errors.Is(err, errTopologyClosed) {
		t.Fatalf("owned topology executor remained open: %v", err)
	}
}

func TestTopologyRejectsConnectionLocalStateMutations(t *testing.T) {
	exec, err := NewTopologyNativeExecutor([]string{"ferric://127.0.0.1:6388"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	for _, command := range [][]any{
		{"CLIENT", "SETNAME", "unsafe"},
		{"WINDOW_UPDATE", "MAX_INFLIGHT_PER_CONNECTION", 1},
		{"AUTH", "secret"},
	} {
		if _, err := exec.Do(context.Background(), command...); err == nil || !strings.Contains(err.Error(), "connection-local") {
			t.Fatalf("topology command %#v error = %v", command, err)
		}
	}
	exec.mu.Lock()
	adapters := len(exec.adapters)
	exec.mu.Unlock()
	if adapters != 0 {
		t.Fatalf("rejected connection-local commands created %d adapters", adapters)
	}
}

func TestTopologyEndpointPolicyNoneAllowsOnlyExactSeeds(t *testing.T) {
	exec, err := NewTopologyNativeExecutor(
		[]string{"ferric://seed.example:6388"},
		WithTopologyEndpointPolicy(EndpointPolicyNone),
		WithTopologyTrustedHosts("trusted.example"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	if err := exec.validateEndpoint(RoutingEndpoint{Host: "seed.example", NativePort: 6388}); err != nil {
		t.Fatalf("exact seed should be accepted: %v", err)
	}
	for _, endpoint := range []RoutingEndpoint{
		{Host: "seed.example", NativePort: 6389},
		{Host: "trusted.example", NativePort: 6388},
		{Host: "learned.internal", NativePort: 6388},
	} {
		if err := exec.validateEndpoint(endpoint); err == nil {
			t.Fatalf("EndpointPolicyNone accepted non-seed endpoint %#v", endpoint)
		}
	}
}

func TestTopologyScopesCredentialsToSeedConnection(t *testing.T) {
	exec, err := NewTopologyNativeExecutor([]string{
		"ferric://alice:first@seed-a.example:6388",
		"ferric://bob:second@seed-b.example:6388",
	}, WithTopologyEndpointPolicy(EndpointPolicyAny))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()

	first, err := exec.adapterForURL("ferric://alice:first@seed-a.example:6388")
	if err != nil {
		t.Fatal(err)
	}
	second, err := exec.adapterForURL("ferric://bob:second@seed-b.example:6388")
	if err != nil {
		t.Fatal(err)
	}
	learned, err := exec.adapterForEndpoint(RoutingEndpoint{Host: "learned.example", NativePort: 6388})
	if err != nil {
		t.Fatal(err)
	}
	if first.opts.Username != "alice" || first.opts.Password != "first" {
		t.Fatalf("first seed received wrong credentials: %q/%q", first.opts.Username, first.opts.Password)
	}
	if second.opts.Username != "bob" || second.opts.Password != "second" {
		t.Fatalf("second seed received wrong credentials: %q/%q", second.opts.Username, second.opts.Password)
	}
	if learned.opts.Username != "" || learned.opts.Password != "" {
		t.Fatalf("seed credentials leaked to learned endpoint: %q/%q", learned.opts.Username, learned.opts.Password)
	}
}

func TestRoutingTopologyRejectsIncompleteOrInconsistentCoverage(t *testing.T) {
	validRange := func(first, last, shard, lane, port int64) map[string]any {
		return map[string]any{
			"first_slot": first, "last_slot": last, "shard": shard, "lane_id": lane,
			"endpoint": map[string]any{"node": "node-a", "host": "127.0.0.1", "native_port": port},
		}
	}
	tests := []struct {
		name    string
		payload map[string]any
	}{
		{"missing slots", map[string]any{"route_epoch": int64(1), "shard_count": int64(1), "ranges": []any{validRange(0, 100, 0, 1, 6388)}}},
		{"overlap", map[string]any{"route_epoch": int64(1), "shard_count": int64(1), "ranges": []any{validRange(0, 600, 0, 1, 6388), validRange(600, 1023, 0, 1, 6388)}}},
		{"negative shard", map[string]any{"route_epoch": int64(1), "shard_count": int64(1), "ranges": []any{validRange(0, 1023, -1, 1, 6388)}}},
		{"shard count mismatch", map[string]any{"route_epoch": int64(1), "shard_count": int64(2), "ranges": []any{validRange(0, 1023, 0, 1, 6388)}}},
		{"invalid port", map[string]any{"route_epoch": int64(1), "shard_count": int64(1), "ranges": []any{validRange(0, 1023, 0, 1, 70000)}}},
		{"inconsistent shard route", map[string]any{"route_epoch": int64(1), "shard_count": int64(1), "ranges": []any{validRange(0, 511, 0, 1, 6388), validRange(512, 1023, 0, 2, 6388)}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := buildRoutingTopology(tc.payload); err == nil {
				t.Fatalf("accepted invalid topology: %#v", tc.payload)
			}
		})
	}
}

func TestTopologyRefreshCoalescesConcurrentCallers(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	var shards atomic.Int64
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
		request, err := readNativeRequestFrame(reader)
		if err != nil {
			errCh <- err
			return
		}
		shards.Add(1)
		time.Sleep(50 * time.Millisecond)
		endpoint := topologyEndpointFromListener(t, listener)
		errCh <- writeNativeTestResponse(writer, request, nativeStatusOK, map[string]any{
			"route_epoch": int64(1), "shard_count": int64(1),
			"ranges": []any{map[string]any{
				"first_slot": int64(0), "last_slot": int64(1023), "shard": int64(0), "lane_id": int64(1),
				"endpoint": map[string]any{"node": endpoint.Node, "host": endpoint.Host, "native_port": int64(endpoint.NativePort)},
			}},
		})
	}()

	exec, err := NewTopologyNativeExecutor([]string{"ferric://" + listener.Addr().String()}, WithTopologyNativeOptions(
		WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0), WithNativeReconnect(0),
	))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- exec.RefreshTopology(ctx)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := shards.Load(); got != 1 {
		t.Fatalf("concurrent refresh issued %d SHARDS requests, expected 1", got)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestTopologySharedRefreshIsNotOwnedByFirstCallerContext(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	shardsRead := make(chan struct{})
	releaseShards := make(chan struct{})
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
		close(shardsRead)
		<-releaseShards
		endpoint := topologyEndpointFromListener(t, listener)
		serverErr <- writeNativeTestResponse(writer, request, nativeStatusOK, map[string]any{
			"route_epoch": int64(1), "shard_count": int64(1),
			"ranges": []any{map[string]any{
				"first_slot": int64(0), "last_slot": int64(1023), "shard": int64(0), "lane_id": int64(1),
				"endpoint": map[string]any{"node": endpoint.Node, "host": endpoint.Host, "native_port": int64(endpoint.NativePort)},
			}},
		})
	}()

	exec, err := NewTopologyNativeExecutor([]string{"ferric://" + listener.Addr().String()}, WithTopologyNativeOptions(
		WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0), WithNativeReconnect(0),
	))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderErr := make(chan error, 1)
	go func() { leaderErr <- exec.RefreshTopology(leaderCtx) }()
	<-shardsRead

	followerCtx, cancelFollower := context.WithTimeout(context.Background(), time.Second)
	defer cancelFollower()
	followerStarted := make(chan struct{})
	followerErr := make(chan error, 1)
	go func() {
		close(followerStarted)
		followerErr <- exec.RefreshTopology(followerCtx)
	}()
	<-followerStarted
	time.Sleep(10 * time.Millisecond)
	cancelLeader()
	if err := <-leaderErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader error = %v, want context cancellation", err)
	}
	close(releaseShards)

	if err := <-followerErr; err != nil {
		t.Fatalf("follower inherited leader cancellation: %v", err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestTopologyChangedEventProactivelyRefreshesRoutes(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	endpoint := topologyEndpointFromListener(t, listener)
	topologyPayload := func(epoch int64) map[string]any {
		return map[string]any{
			"route_epoch": epoch, "shard_count": int64(1),
			"ranges": []any{map[string]any{
				"first_slot": int64(0), "last_slot": int64(1023), "shard": int64(0), "lane_id": int64(1),
				"endpoint": map[string]any{"node": endpoint.Node, "host": endpoint.Host, "native_port": int64(endpoint.NativePort)},
			}},
		}
	}
	sendEvent := make(chan struct{})
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
		value, rest, err := decodeNativeValue(startup.body)
		if err != nil || len(rest) != 0 {
			errCh <- errUnexpectedValue("topology STARTUP payload", value)
			return
		}
		payload, err := nativeMap(value)
		if err != nil || !slices.Contains(stringList(payload["events"]), "TOPOLOGY_CHANGED") {
			errCh <- errUnexpectedValue("topology STARTUP events", payload["events"])
			return
		}
		if err := writeNativeTestResponse(writer, startup, nativeStatusOK, map[string]any{"ready": true}); err != nil {
			errCh <- err
			return
		}
		first, err := readNativeRequestFrame(reader)
		if err != nil || first.opcode != nativeOpCommandExec {
			errCh <- errUnexpectedFrame(first)
			return
		}
		if err := writeNativeTestResponse(writer, first, nativeStatusOK, topologyPayload(1)); err != nil {
			errCh <- err
			return
		}
		<-sendEvent
		if err := writeNativeTestResponse(writer, nativeFrame{opcode: nativeOpEvent}, nativeStatusOK, map[string]any{
			"event": "TOPOLOGY_CHANGED", "payload": map[string]any{"route_epoch": int64(2)},
		}); err != nil {
			errCh <- err
			return
		}
		second, err := readNativeRequestFrame(reader)
		if err != nil || second.opcode != nativeOpCommandExec {
			errCh <- errUnexpectedFrame(second)
			return
		}
		errCh <- writeNativeTestResponse(writer, second, nativeStatusOK, topologyPayload(2))
	}()

	exec, err := NewTopologyNativeExecutor([]string{"ferric://" + listener.Addr().String()}, WithTopologyNativeOptions(
		WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0), WithNativeReconnect(0),
	))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = exec.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := exec.RefreshTopology(ctx); err != nil {
		close(sendEvent)
		t.Fatal(err)
	}
	close(sendEvent)
	deadline := time.Now().Add(time.Second)
	for {
		exec.mu.Lock()
		epoch := exec.topology.RouteEpoch
		exec.mu.Unlock()
		if epoch == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("topology route epoch remained %d after TOPOLOGY_CHANGED", epoch)
		}
		time.Sleep(time.Millisecond)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestTopologyPipelineGroupsCommandsByEndpointAndLane(t *testing.T) {
	listenerA, framesA, errsA := startRoutedNativeEndpoint(t, func(frame nativeFrame, request int) any {
		return []any{[]any{"ok", "a1"}, []any{"ok", "a2"}}
	})
	listenerB, framesB, errsB := startRoutedNativeEndpoint(t, func(frame nativeFrame, request int) any {
		return []any{[]any{"ok", "b1"}, []any{"ok", "b2"}}
	})
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(t, listenerA, listenerB)
	defer func() { _ = exec.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	values, err := exec.Pipeline(ctx, [][]any{
		{"SET", keyA, "1"},
		{"SET", keyB, "2"},
		{"SET", keyA, "3"},
		{"SET", keyB, "4"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a1", "b1", "a2", "b2"}
	if len(values) != len(want) {
		t.Fatalf("unexpected grouped pipeline results: %#v", values)
	}
	for i := range want {
		if asString(values[i]) != want[i] {
			t.Fatalf("result %d = %#v, want %q", i, values[i], want[i])
		}
	}
	for name, frames := range map[string]<-chan nativeFrame{"a": framesA, "b": framesB} {
		select {
		case frame := <-frames:
			if frame.opcode != nativeOpPipeline {
				t.Fatalf("endpoint %s received opcode %d, want pipeline", name, frame.opcode)
			}
		case <-ctx.Done():
			t.Fatalf("endpoint %s did not receive its pipeline group", name)
		}
	}
	if err := <-errsA; err != nil {
		t.Fatal(err)
	}
	if err := <-errsB; err != nil {
		t.Fatal(err)
	}
}

func TestTopologyPipelineKeepsSingleRouteScatterInRoutedBatch(t *testing.T) {
	listenerA, framesA, errsA := startRoutedNativeEndpoint(t, func(frame nativeFrame, _ int) any {
		if frame.opcode != nativeOpPipeline {
			return nil
		}
		return []any{[]any{"ok", "set"}, []any{"ok", []any{"value-a"}}}
	})
	listenerB, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any { return nil })
	exec, keyA, _ := topologyExecutorForTwoEndpoints(t, listenerA, listenerB)
	defer func() { _ = exec.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	values, err := exec.Pipeline(ctx, [][]any{
		{"SET", keyA, "value-a"},
		{"MGET", keyA},
	})
	if err != nil {
		t.Fatal(err)
	}
	items, ok := values[1].([]any)
	if !ok || len(items) != 1 || asString(items[0]) != "value-a" {
		t.Fatalf("unexpected single-route scatter result: %#v", values)
	}
	select {
	case frame := <-framesA:
		if frame.opcode != nativeOpPipeline {
			t.Fatalf("single-route scatter was not batched: opcode %d", frame.opcode)
		}
	case <-ctx.Done():
		t.Fatal("single-route routed batch was not sent")
	}
	select {
	case frame := <-framesA:
		t.Fatalf("single-route scatter used an extra request: %#v", frame)
	case <-time.After(50 * time.Millisecond):
	}
	if err := <-errsA; err != nil {
		t.Fatal(err)
	}
}

func TestTopologyPipelineDoesNotRunScatterBeforeEarlierWrites(t *testing.T) {
	var setApplied atomic.Bool
	scatterStarted := make(chan struct{})
	listenerA, _, errsA := startRoutedNativeEndpoint(t, func(frame nativeFrame, _ int) any {
		switch frame.opcode {
		case nativeOpPipeline:
			select {
			case <-scatterStarted:
			case <-time.After(100 * time.Millisecond):
			}
			setApplied.Store(true)
			return []any{[]any{"ok", "set"}}
		case nativeOpMGet:
			return []any{"value-a"}
		default:
			return nil
		}
	})
	listenerB, _, errsB := startRoutedNativeEndpoint(t, func(frame nativeFrame, _ int) any {
		if frame.opcode != nativeOpMGet {
			return nil
		}
		close(scatterStarted)
		if setApplied.Load() {
			return []any{"fresh"}
		}
		return []any{"stale"}
	})
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(t, listenerA, listenerB)
	defer func() { _ = exec.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	values, err := exec.Pipeline(ctx, [][]any{
		{"SET", keyA, "value-a"},
		{"MGET", keyA, keyB},
	})
	if err != nil {
		t.Fatal(err)
	}
	items, ok := values[1].([]any)
	if !ok || len(items) != 2 || asString(items[1]) != "fresh" {
		t.Fatalf("scatter observed stale state by overtaking SET: %#v", values)
	}
	if err := <-errsA; err != nil {
		t.Fatal(err)
	}
	if err := <-errsB; err != nil {
		t.Fatal(err)
	}
}

func TestTopologyTaskFanoutIsBounded(t *testing.T) {
	const taskCount = 128
	const limit = 7
	var active atomic.Int64
	var maximum atomic.Int64
	release := make(chan struct{})
	started := make(chan struct{}, taskCount)
	tasks := make([]func(), taskCount)
	for i := range tasks {
		tasks[i] = func() {
			current := active.Add(1)
			for previous := maximum.Load(); current > previous && !maximum.CompareAndSwap(previous, current); previous = maximum.Load() {
			}
			started <- struct{}{}
			<-release
			active.Add(-1)
		}
	}
	done := make(chan struct{})
	go func() {
		runBoundedTopologyTasks(limit, tasks)
		close(done)
	}()
	for range limit {
		<-started
	}
	time.Sleep(10 * time.Millisecond)
	if got := maximum.Load(); got > limit {
		t.Fatalf("topology task concurrency = %d, want <= %d", got, limit)
	}
	if got := len(started); got != 0 {
		t.Fatalf("started %d tasks beyond concurrency limit", got)
	}
	close(release)
	<-done
	if got := maximum.Load(); got != limit {
		t.Fatalf("maximum topology concurrency = %d, want %d", got, limit)
	}
}

func TestTopologyScattersSafeCrossShardCommands(t *testing.T) {
	listenerA, framesA, errsA := startRoutedNativeEndpoint(t, func(frame nativeFrame, request int) any {
		if frame.opcode == nativeOpMGet {
			return []any{"value-a"}
		}
		return int64(1)
	})
	listenerB, framesB, errsB := startRoutedNativeEndpoint(t, func(frame nativeFrame, request int) any {
		if frame.opcode == nativeOpMGet {
			return []any{"value-b"}
		}
		return int64(1)
	})
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(t, listenerA, listenerB, WithTopologyCrossShardWritePolicy(CrossShardWritePerShard))
	defer func() { _ = exec.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	value, err := exec.Do(ctx, "MGET", keyA, keyB)
	if err != nil {
		t.Fatal(err)
	}
	items, ok := value.([]any)
	if !ok || len(items) != 2 || asString(items[0]) != "value-a" || asString(items[1]) != "value-b" {
		t.Fatalf("MGET scatter did not preserve key order: %#v", value)
	}
	value, err = exec.Do(ctx, "DEL", keyA, keyB)
	if err != nil {
		t.Fatal(err)
	}
	if asInt64(value) != 2 {
		t.Fatalf("DEL scatter count = %#v, want 2", value)
	}
	for name, frames := range map[string]<-chan nativeFrame{"a": framesA, "b": framesB} {
		for i := 0; i < 2; i++ {
			select {
			case <-frames:
			case <-ctx.Done():
				t.Fatalf("endpoint %s received only %d scatter requests", name, i)
			}
		}
	}
	if err := <-errsA; err != nil {
		t.Fatal(err)
	}
	if err := <-errsB; err != nil {
		t.Fatal(err)
	}
}

func startRoutedNativeEndpoint(t *testing.T, response func(nativeFrame, int) any) (net.Listener, <-chan nativeFrame, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	frames := make(chan nativeFrame, 4)
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
		for request := 0; request < cap(frames); request++ {
			_ = conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
			frame, err := readNativeRequestFrame(reader)
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() && request > 0 {
					errCh <- nil
					return
				}
				errCh <- err
				return
			}
			frames <- frame
			if err := writeNativeTestResponse(writer, frame, nativeStatusOK, response(frame, request)); err != nil {
				errCh <- err
				return
			}
		}
		errCh <- nil
	}()
	return listener, frames, errCh
}

func topologyExecutorForTwoEndpoints(t *testing.T, listenerA, listenerB net.Listener, extraOptions ...TopologyOption) (*TopologyNativeExecutor, string, string) {
	t.Helper()
	endpointA := topologyEndpointFromListener(t, listenerA)
	endpointB := topologyEndpointFromListener(t, listenerB)
	options := []TopologyOption{
		WithTopologyEndpointPolicy(EndpointPolicyAny),
		WithTopologyNativeOptions(WithNativeTimeout(time.Second), WithNativeHeartbeat(0, 0), WithNativeReconnect(0)),
	}
	options = append(options, extraOptions...)
	exec, err := NewTopologyNativeExecutor(
		[]string{"ferric://" + listenerA.Addr().String()},
		options...,
	)
	if err != nil {
		t.Fatal(err)
	}
	keyA, keyB := differentSlotKeys(t)
	slotA, slotB := routeSlotForKey(keyA), routeSlotForKey(keyB)
	topology := &RoutingTopology{RouteEpoch: 1, ShardCount: 2, endpoints: map[string]RoutingEndpoint{
		endpointKey(endpointA): endpointA,
		endpointKey(endpointB): endpointB,
	}}
	for slot := range topology.slots {
		endpoint, shard := endpointA, 0
		if slot == slotB {
			endpoint, shard = endpointB, 1
		}
		topology.slots[slot] = &RoutingRoute{Shard: shard, LaneID: uint32(shard + 1), EndpointKey: endpointKey(endpoint), Endpoint: endpoint, Slot: slot}
	}
	// Explicitly retain A for its selected slot if the fixtures ever change.
	topology.slots[slotA] = &RoutingRoute{Shard: 0, LaneID: 1, EndpointKey: endpointKey(endpointA), Endpoint: endpointA, Slot: slotA}
	if err := exec.installTopology(topology); err != nil {
		t.Fatal(err)
	}
	return exec, keyA, keyB
}
