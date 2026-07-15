package ferricstore

import (
	"crypto/sha256"
	"encoding/base64"
	"hash/crc32"
	"math"
	"strconv"
	"testing"
)

var topologyRouteIdentitySink topologyRouteIdentity

func TestTopologyRoutingKeyMatchesLatestSDKs(t *testing.T) {
	if key, ok := routingKeyForCommand([]any{"INCR", "counter"}); !ok || asString(key) != "counter" {
		t.Fatalf("INCR should route by its key, got key=%#v ok=%v", key, ok)
	}
	if key, ok := routingKeyForCommand([]any{"XACK", "events", "workers", "1-0"}); !ok || asString(key) != "events" {
		t.Fatalf("XACK should route by stream key, got key=%#v ok=%v", key, ok)
	}
	if key, ok := routingKeyForCommand([]any{"KEY_INFO", "account:1"}); !ok || asString(key) != "account:1" {
		t.Fatalf("KEY_INFO should route by its key, got key=%#v ok=%v", key, ok)
	}
	if key, ok := routingKeyForCommand([]any{"FLOW.SEARCH", "TYPE", "order"}); ok || key != nil {
		t.Fatalf("unpartitioned FLOW.SEARCH should stay on control, got key=%#v ok=%v", key, ok)
	}
	if key, ok := routingKeyForCommand([]any{"FLOW.SEARCH", "TYPE", "order", "PARTITION", "tenant:1"}); !ok || asString(key) != testFlowPartitionRouteKey("tenant:1") {
		t.Fatalf("partitioned FLOW.SEARCH should route by partition, got key=%#v ok=%v", key, ok)
	}

	taggedA := "acct:{42}:a"
	taggedB := "acct:{42}:b"
	if key, ok := routingKeyForCommand([]any{"BLPOP", taggedA, taggedB, 1}); !ok || asString(key) != taggedA {
		t.Fatalf("BLPOP should route same-slot keys, got key=%#v ok=%v", key, ok)
	}
	if key, ok := routingKeyForCommand([]any{"CMS.MERGE", taggedA, 1, taggedB}); !ok || asString(key) != taggedA {
		t.Fatalf("CMS.MERGE should route destination and sources, got key=%#v ok=%v", key, ok)
	}
	if key, ok := routingKeyForCommand([]any{"BITOP", "OR", taggedA, taggedB}); !ok || asString(key) != taggedA {
		t.Fatalf("BITOP should route by destination and source keys, got key=%#v ok=%v", key, ok)
	}
	if key, ok := routingKeyForCommand([]any{"XREAD", "COUNT", 1, "STREAMS", taggedA, taggedB, "0-0", "0-0"}); !ok || asString(key) != taggedA {
		t.Fatalf("XREAD should route by stream keys, got key=%#v ok=%v", key, ok)
	}

	left, right := differentSlotKeys(t)
	if key, ok := routingKeyForCommand([]any{"RENAME", left, right}); ok || key != nil {
		t.Fatalf("cross-slot RENAME should stay on control, got key=%#v ok=%v", key, ok)
	}
}

func TestSingleShardKeyRejectsUnsupportedKeyTypes(t *testing.T) {
	if key, ok := singleShardKey([]any{"account:{42}:source", 123}); ok || key != nil {
		t.Fatalf("mixed key types produced key=%#v ok=%v; want rejection", key, ok)
	}
}

func TestTopologyRoutesOnlyMemoryUsageByKey(t *testing.T) {
	if key, ok := routingKeyForCommand([]any{"MEMORY", "USAGE", "account:1"}); !ok || asString(key) != "account:1" {
		t.Fatalf("MEMORY USAGE should route by key, got key=%#v ok=%v", key, ok)
	}
	for _, args := range [][]any{{"MEMORY", "STATS"}, {"MEMORY", "HELP"}} {
		if key, ok := routingKeyForCommand(args); ok || key != nil {
			t.Fatalf("%v should stay on control, got key=%#v ok=%v", args, key, ok)
		}
	}
}

func TestTopologyRouteIdentityDoesNotAllocateForInstalledRoutes(t *testing.T) {
	route := RoutingRoute{EndpointKey: "node-a:6388", LaneID: 7}
	allocs := testing.AllocsPerRun(1000, func() {
		topologyRouteIdentitySink = routeIdentity(route)
	})
	if allocs != 0 {
		t.Fatalf("route identity allocations = %.0f; want 0", allocs)
	}
}

func BenchmarkTopologyRouteGrouping1000(b *testing.B) {
	routes := make([]RoutingRoute, 1000)
	for index := range routes {
		routes[index] = RoutingRoute{EndpointKey: "node-a:6388", LaneID: uint32(index%8 + 1)}
	}
	b.ReportAllocs()
	for b.Loop() {
		groups := make(map[topologyRouteIdentity]int, 8)
		for _, route := range routes {
			groups[routeIdentity(route)]++
		}
		topologyRouteIdentitySink = routeIdentity(routes[len(groups)])
	}
}

func TestTopologyFlowRoutingMatchesServerStorageKeys(t *testing.T) {
	tests := []struct {
		name string
		args []any
		want string
		ok   bool
	}{
		{name: "state id", args: []any{"FLOW.GET", "job-1"}, want: testFlowAutoIDRouteKey("job-1"), ok: true},
		{name: "explicit partition", args: []any{"FLOW.COMPLETE", "job-1", "lease-1", "PARTITION", "tenant:1"}, want: testFlowPartitionRouteKey("tenant:1"), ok: true},
		{name: "many explicit partition", args: []any{"FLOW.CREATE_MANY", "tenant:1", "TYPE", "email", "ITEMS"}, want: testFlowPartitionRouteKey("tenant:1"), ok: true},
		{name: "claim global", args: []any{"FLOW.CLAIM_DUE", "email", "PARTITION", "GLOBAL"}, want: "f:{f}:route", ok: true},
		{name: "claim auto", args: []any{"FLOW.CLAIM_DUE", "email", "PARTITION", "AUTO"}},
		{name: "claim any", args: []any{"FLOW.CLAIM_DUE", "email", "PARTITION", "ANY"}},
		{name: "approval id", args: []any{"FLOW.APPROVAL.GET", "approval-1"}, want: testFlowPartitionRouteKey("approval-1"), ok: true},
		{name: "governance scope", args: []any{"FLOW.BUDGET.GET", "tenant:acme"}, want: testFlowPartitionRouteKey("tenant:acme"), ok: true},
		{name: "value owner id", args: []any{"FLOW.VALUE.PUT", []byte("value"), "OWNER_FLOW_ID", "job-1", "NAME", "result"}, want: testFlowAutoIDRouteKey("job-1"), ok: true},
		{name: "value refs", args: []any{"FLOW.VALUE.MGET", "f:{fa:1}:v:a", "f:{fa:1}:v:b", "MAX_BYTES", 10}, want: "f:{fa:1}:v:a", ok: true},
		{name: "schedule get", args: []any{"FLOW.SCHEDULE.GET", "schedule-1"}},
		{name: "schedule create", args: []any{"FLOW.SCHEDULE.CREATE", "schedule-1", "KIND", "interval"}},
		{name: "unknown flow command", args: []any{"FLOW.UNKNOWN", "looks-like-an-id"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key, ok := routingKeyForCommand(tc.args)
			if ok != tc.ok || (tc.ok && asString(key) != tc.want) || (!tc.ok && key != nil) {
				t.Fatalf("routing key = %#v, ok=%v; want %q, ok=%v", key, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestTopologyRoutingRejectsOverflowingCountsWithoutPanicking(t *testing.T) {
	tests := []struct {
		name string
		args []any
	}{
		{name: "blocking pop", args: []any{"BLMPOP", 0, int64(math.MaxInt64), "key", "LEFT"}},
		{name: "counted read", args: []any{"SINTERCARD", int64(math.MaxInt64), "key"}},
		{name: "counted store", args: []any{"CMS.MERGE", "dest", int64(math.MaxInt64), "source"}},
		{name: "flow partitions", args: []any{"FLOW.CLAIM_DUE", "email", "PARTITIONS", int64(math.MaxInt64), "tenant:1"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("routing panicked for malformed count: %v", recovered)
				}
			}()
			if key, ok := routingKeyForCommand(tc.args); ok || key != nil {
				t.Fatalf("malformed count routed with key=%#v ok=%v", key, ok)
			}
		})
	}
}

func testFlowPartitionRouteKey(partition string) string {
	digest := sha256.Sum256([]byte(partition))
	return "f:{f:" + base64.RawURLEncoding.EncodeToString(digest[:]) + "}:route"
}

func testFlowAutoIDRouteKey(id string) string {
	bucket := crc32.ChecksumIEEE([]byte(id)) & 0xff
	return "f:{fa:" + strconv.FormatUint(uint64(bucket), 10) + "}:route"
}

func TestTopologyRoutingKeyCoversPublicNestedAndFallbackCommands(t *testing.T) {
	tests := []struct {
		name string
		args []any
		key  string
	}{
		{name: "command exec", args: []any{"COMMAND_EXEC", "GET", "account:1"}, key: "account:1"},
		{name: "xgroup create", args: []any{"XGROUP", "CREATE", "events", "workers", "0-0"}, key: "events"},
		{name: "xinfo stream", args: []any{"XINFO", "STREAM", "events"}, key: "events"},
		{name: "object refcount", args: []any{"OBJECT", "REFCOUNT", "value"}, key: "value"},
		{name: "extend", args: []any{"EXTEND", "lock", "owner", int64(1000)}, key: "lock"},
		{name: "lpushx", args: []any{"LPUSHX", "list", "value"}, key: "list"},
		{name: "rpushx", args: []any{"RPUSHX", "list", "value"}, key: "list"},
		{name: "zrank", args: []any{"ZRANK", "ranked", "member"}, key: "ranked"},
		{name: "zrevrank", args: []any{"ZREVRANK", "ranked", "member"}, key: "ranked"},
		{name: "zpopmin", args: []any{"ZPOPMIN", "ranked"}, key: "ranked"},
		{name: "zpopmax", args: []any{"ZPOPMAX", "ranked"}, key: "ranked"},
		{name: "tdigest quantile", args: []any{"TDIGEST.QUANTILE", "latency", 0.5}, key: "latency"},
		{name: "tdigest cdf", args: []any{"TDIGEST.CDF", "latency", 10}, key: "latency"},
		{name: "tdigest rank", args: []any{"TDIGEST.RANK", "latency", 10}, key: "latency"},
		{name: "tdigest revrank", args: []any{"TDIGEST.REVRANK", "latency", 10}, key: "latency"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key, ok := routingKeyForCommand(tc.args)
			if !ok || asString(key) != tc.key {
				t.Fatalf("routingKeyForCommand(%#v) = %#v, %t; want %q, true", tc.args, key, ok, tc.key)
			}
		})
	}
}

func differentSlotKeys(t *testing.T) (string, string) {
	t.Helper()
	first := "slot:a"
	firstSlot := routeSlotForKey(first)
	for _, candidate := range []string{"slot:b", "slot:c", "slot:d", "slot:e"} {
		if routeSlotForKey(candidate) != firstSlot {
			return first, candidate
		}
	}
	t.Fatal("test fixture could not find two different slots")
	return "", ""
}

func TestTopologyRouteRejectsUnsupportedKeyTypes(t *testing.T) {
	topology := topologyForEndpoint(RoutingEndpoint{Host: "node.example", NativePort: 6388}, 1)
	exec := &TopologyNativeExecutor{
		endpointPolicy: EndpointPolicyAny,
		topology:       topology,
	}

	for _, key := range []any{nil, 42, struct{ ID string }{ID: "key"}} {
		if route, err := topology.RouteKey(key); err == nil {
			t.Errorf("RoutingTopology.RouteKey(%T) = %#v; want unsupported-type error", key, route)
		}
		if route, err := exec.Route(key); err == nil {
			t.Errorf("TopologyNativeExecutor.Route(%T) = %#v; want unsupported-type error", key, route)
		}
	}
}
