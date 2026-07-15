package ferricstore

// routingTopologiesEqual compares every field that can affect endpoint, shard,
// or lane selection. Pointer identity is deliberately irrelevant.
func routingTopologiesEqual(left, right *RoutingTopology) bool {
	if left == right {
		return true
	}
	if left == nil || right == nil ||
		left.RouteEpoch != right.RouteEpoch ||
		left.ShardCount != right.ShardCount ||
		len(left.endpoints) != len(right.endpoints) {
		return false
	}
	for key, endpoint := range left.endpoints {
		if other, ok := right.endpoints[key]; !ok || other != endpoint {
			return false
		}
	}
	for slot := range left.slots {
		leftRoute, rightRoute := left.slots[slot], right.slots[slot]
		if leftRoute == nil || rightRoute == nil {
			if leftRoute != rightRoute {
				return false
			}
			continue
		}
		if *leftRoute != *rightRoute {
			return false
		}
	}
	return true
}
