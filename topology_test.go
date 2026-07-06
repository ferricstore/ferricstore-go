package ferricstore

import "testing"

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
		seedEndpointKeys: stringSet(endpointKey(RoutingEndpoint{Host: "127.0.0.1", NativePort: 6388})),
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

func TestTopologyRoutingKeyMatchesLatestSDKs(t *testing.T) {
	if key, ok := routingKeyForCommand([]any{"FLOW.SEARCH", "TYPE", "order"}); ok || key != nil {
		t.Fatalf("unpartitioned FLOW.SEARCH should stay on control, got key=%#v ok=%v", key, ok)
	}
	if key, ok := routingKeyForCommand([]any{"FLOW.SEARCH", "TYPE", "order", "PARTITION", "tenant:1"}); !ok || asString(key) != "tenant:1" {
		t.Fatalf("partitioned FLOW.SEARCH should route by partition, got key=%#v ok=%v", key, ok)
	}

	taggedA := "acct:{42}:a"
	taggedB := "acct:{42}:b"
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
