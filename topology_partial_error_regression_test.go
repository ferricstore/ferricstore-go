package ferricstore

import (
	"errors"
	"testing"
)

func TestTopologyPartialWriteFailuresHaveDeterministicRouteOrder(t *testing.T) {
	t.Parallel()

	failure := func(endpoint string, shard int, lane uint32, key string) error {
		return &TopologyWriteFailure{
			Route: RoutingRoute{EndpointKey: endpoint, Shard: shard, LaneID: lane},
			Keys:  []string{key},
			Err:   errors.New("write failed"),
		}
	}
	partial := newTopologyPartialWriteError("DEL", 0, []error{
		failure("node-z", 2, 3, "z"),
		failure("node-a", 1, 2, "b"),
		failure("node-a", 1, 1, "c"),
		failure("node-a", 1, 1, "a"),
	})

	wantEndpoints := []string{"node-a", "node-a", "node-a", "node-z"}
	wantLanes := []uint32{1, 1, 2, 3}
	wantKeys := []string{"a", "c", "b", "z"}
	for index, err := range partial.Failures {
		var got *TopologyWriteFailure
		if !errors.As(err, &got) {
			t.Fatalf("failure %d = %T, want TopologyWriteFailure", index, err)
		}
		if got.Route.EndpointKey != wantEndpoints[index] || got.Route.LaneID != wantLanes[index] || got.Keys[0] != wantKeys[index] {
			t.Fatalf("failure %d = endpoint %q lane %d keys %#v", index, got.Route.EndpointKey, got.Route.LaneID, got.Keys)
		}
	}
}
