package ferricstore

import "testing"

func TestEndpointFromRangeRejectsNonTextEndpointIdentity(t *testing.T) {
	tests := []struct {
		name string
		item map[string]any
	}{
		{
			name: "nested host",
			item: map[string]any{
				"endpoint": map[string]any{"host": int64(127), "native_port": int64(6388)},
			},
		},
		{
			name: "nested node",
			item: map[string]any{
				"endpoint": map[string]any{
					"host": "127.0.0.1", "node": int64(1), "native_port": int64(6388),
				},
			},
		},
		{
			name: "outer node fallback",
			item: map[string]any{
				"node":     map[string]any{"unexpected": "value"},
				"endpoint": map[string]any{"host": "127.0.0.1", "native_port": int64(6388)},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if endpoint, err := endpointFromRange(tc.item); err == nil {
				t.Fatalf("accepted malformed endpoint identity as %+v", endpoint)
			}
		})
	}
}

func TestEndpointFromRangeRejectsExplicitZeroTLSPort(t *testing.T) {
	item := map[string]any{
		"endpoint": map[string]any{
			"host": "127.0.0.1", "native_port": int64(6388), "native_tls_port": int64(0),
		},
	}
	if endpoint, err := endpointFromRange(item); err == nil {
		t.Fatalf("accepted explicit zero TLS port as %+v", endpoint)
	}
}

func TestRoutingTopologyRejectsConflictingIdentityForOneEndpoint(t *testing.T) {
	route := func(first, last, shard, lane int64, node string) map[string]any {
		return map[string]any{
			"first_slot": first, "last_slot": last, "shard": shard, "lane_id": lane,
			"endpoint": map[string]any{
				"node": node, "host": "127.0.0.1", "native_port": int64(6388),
			},
		}
	}
	_, err := buildRoutingTopology(map[string]any{
		"route_epoch": int64(1),
		"shard_count": int64(2),
		"ranges": []any{
			route(0, 511, 0, 1, "node-a"),
			route(512, routeSlotCount-1, 1, 2, "node-b"),
		},
	})
	if err == nil {
		t.Fatal("topology accepted conflicting node identities for one endpoint address")
	}
}

func TestTopologyConstructorRejectsInvalidEndpointPolicy(t *testing.T) {
	exec, err := NewTopologyNativeExecutor(
		[]string{"ferric://127.0.0.1:6388"},
		WithTopologyEndpointPolicy(EndpointPolicy("invalid")),
	)
	if exec != nil {
		_ = exec.Close()
	}
	if err == nil {
		t.Fatal("topology constructor accepted an invalid endpoint policy")
	}
}
