package ferricstore

import (
	"context"
	"hash/crc32"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

var topologyStringRouteGroupCountSink int

func TestTopologySingleTaskExecutionDoesNotAllocate(t *testing.T) {
	var calls atomic.Int64
	tasks := []func(){func() { calls.Add(1) }}
	allocs := testing.AllocsPerRun(1_000, func() {
		runBoundedTopologyTasks(maxTopologyConcurrentTasks, tasks)
	})
	if allocs != 0 {
		t.Fatalf("single topology task allocations = %.0f; want 0", allocs)
	}
	if got := calls.Load(); got != 1_001 {
		t.Fatalf("single topology task calls = %d; want 1001", got)
	}
}

func TestTopologyStringCRCMatchesStandardLibrary(t *testing.T) {
	for _, value := range []string{"", "key", "account:{42}:value", "tenant:שלום"} {
		if got, want := routeCRC32(value), crc32.ChecksumIEEE([]byte(value)); got != want {
			t.Fatalf("route CRC32(%q) = %d; want %d", value, got, want)
		}
	}
}

func BenchmarkTopologySingleTask(b *testing.B) {
	tasks := []func(){func() {}}
	b.ReportAllocs()
	for b.Loop() {
		runBoundedTopologyTasks(maxTopologyConcurrentTasks, tasks)
	}
}

func TestTopologySameRouteDetectionDoesNotAllocate(t *testing.T) {
	exec, route := singleRouteTopologyBenchmarkFixture()
	keys := []string{"account:1", "account:2", "account:3"}
	var got RoutingRoute
	var same bool
	allocs := testing.AllocsPerRun(1_000, func() {
		var err error
		got, same, err = exec.singleRouteForStringKeys(context.Background(), keys)
		if err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("same-route detection allocations = %.0f; want 0", allocs)
	}
	if !same || routeIdentity(got) != routeIdentity(*route) {
		t.Fatalf("same-route detection = %#v, %t", got, same)
	}
}

func TestTopologySameRoutePlanDoesNotAllocate(t *testing.T) {
	exec, _ := singleRouteTopologyBenchmarkFixture()
	keys := []string{"account:1", "account:2", "account:3"}
	allocs := testing.AllocsPerRun(1_000, func() {
		_, groups, err := exec.planStringKeyRoutes(context.Background(), keys, true)
		if err != nil {
			panic(err)
		}
		if groups != nil {
			panic("same-route plan created scatter groups")
		}
	})
	if allocs != 0 {
		t.Fatalf("same-route planning allocations = %.0f; want 0", allocs)
	}
}

func TestTopologyCrossShardMGetUsesOneTopologySnapshot(t *testing.T) {
	listenerA, framesA, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any {
		return []any{[]byte("old-a")}
	})
	listenerB, framesB, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any {
		return []any{[]byte("old-b")}
	})
	listenerC, _, _ := startRoutedNativeEndpoint(t, func(nativeFrame, int) any {
		return []any{[]byte("new-a"), []byte("new-b")}
	})
	exec, keyA, keyB := topologyExecutorForTwoEndpoints(t, listenerA, listenerB)
	t.Cleanup(func() { _ = exec.Close() })
	newTopology := topologyForEndpoint(topologyEndpointFromListener(t, listenerC), 2)
	var validations atomic.Int64
	var installErr error
	exec.endpointValidator = func(RoutingEndpoint) bool {
		if validations.Add(1) == 2 {
			installErr = exec.installTopology(newTopology)
		}
		return true
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	values, err := NewClientWithExecutor(exec).KV().MGet(ctx, keyA, keyB)
	if err != nil {
		t.Fatal(err)
	}
	if installErr != nil {
		t.Fatal(installErr)
	}
	if len(values) != 2 || asString(values[0]) != "new-a" || asString(values[1]) != "new-b" {
		t.Fatalf("MGET mixed topology snapshots: %#v; want [new-a new-b]", values)
	}
	assertNoNativeFrame(t, framesA, "old first typed topology endpoint")
	assertNoNativeFrame(t, framesB, "old second typed topology endpoint")
}

func BenchmarkTopologySameRouteDetection100(b *testing.B) {
	exec, _ := singleRouteTopologyBenchmarkFixture()
	keys := make([]string, 100)
	for index := range keys {
		keys[index] = "account:" + strconv.Itoa(index)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, same, err := exec.singleRouteForStringKeys(context.Background(), keys); err != nil || !same {
			b.Fatalf("same-route detection = %t, %v", same, err)
		}
	}
}

func BenchmarkTopologySameRoutePlan100(b *testing.B) {
	exec, _ := singleRouteTopologyBenchmarkFixture()
	keys := make([]string, 100)
	for index := range keys {
		keys[index] = "account:" + strconv.Itoa(index)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, groups, err := exec.planStringKeyRoutes(context.Background(), keys, true); err != nil || groups != nil {
			b.Fatalf("same-route plan groups = %d, error = %v", len(groups), err)
		}
	}
}

func BenchmarkTopologyCrossShardMGetPlanning1000(b *testing.B) {
	exec, keys := multiRouteTopologyBenchmarkFixture(1_000)
	b.Run("legacy-per-key-lock", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			groups := make(map[topologyRouteIdentity]*stringKeyRouteGroup)
			for position, key := range keys {
				route, err := exec.routeWithRefresh(context.Background(), key)
				if err != nil {
					b.Fatal(err)
				}
				identity := routeIdentity(route)
				group := groups[identity]
				if group == nil {
					group = &stringKeyRouteGroup{route: route}
					groups[identity] = group
				}
				group.keys = append(group.keys, key)
				group.positions = append(group.positions, position)
			}
			topologyStringRouteGroupCountSink = len(groups)
		}
	})
	b.Run("single-snapshot", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, groups, err := exec.planStringKeyRoutes(context.Background(), keys, true)
			if err != nil {
				b.Fatal(err)
			}
			topologyStringRouteGroupCountSink = len(groups)
		}
	})
}

func multiRouteTopologyBenchmarkFixture(keyCount int) (*TopologyNativeExecutor, []string) {
	endpoint := RoutingEndpoint{Host: "127.0.0.1", NativePort: 6388}
	topology := &RoutingTopology{
		ShardCount: 8,
		endpoints:  map[string]RoutingEndpoint{endpointKey(endpoint): endpoint},
	}
	routes := make([]*RoutingRoute, topology.ShardCount)
	for shard := range routes {
		routes[shard] = &RoutingRoute{
			Shard: shard, LaneID: uint32(shard + 1), EndpointKey: endpointKey(endpoint), Endpoint: endpoint,
		}
	}
	for slot := range topology.slots {
		topology.slots[slot] = routes[slot%len(routes)]
	}
	keys := make([]string, keyCount)
	for index := range keys {
		keys[index] = "account:" + strconv.Itoa(index)
	}
	return &TopologyNativeExecutor{topology: topology, endpointPolicy: EndpointPolicyAny}, keys
}

func singleRouteTopologyBenchmarkFixture() (*TopologyNativeExecutor, *RoutingRoute) {
	endpoint := RoutingEndpoint{Host: "127.0.0.1", NativePort: 6388}
	route := &RoutingRoute{
		Shard: 0, LaneID: 1, EndpointKey: endpointKey(endpoint), Endpoint: endpoint,
	}
	topology := &RoutingTopology{ShardCount: 1, endpoints: map[string]RoutingEndpoint{endpointKey(endpoint): endpoint}}
	for slot := range topology.slots {
		topology.slots[slot] = route
	}
	return &TopologyNativeExecutor{topology: topology, endpointPolicy: EndpointPolicyAny}, route
}
