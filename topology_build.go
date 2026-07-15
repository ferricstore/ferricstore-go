package ferricstore

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
)

func buildRoutingTopology(value any) (*RoutingTopology, error) {
	payload, err := nativeMap(value)
	if err != nil {
		return nil, err
	}
	routeEpoch, err := topologyInteger(payload["route_epoch"], "route_epoch")
	if err != nil {
		return nil, fmt.Errorf("invalid SHARDS route_epoch: %w", err)
	}
	if routeEpoch < 0 {
		return nil, errors.New("invalid SHARDS route_epoch: must be non-negative")
	}
	shardCount64, err := topologyInteger(payload["shard_count"], "shard_count")
	if err != nil {
		return nil, fmt.Errorf("invalid SHARDS shard_count: %w", err)
	}
	if shardCount64 <= 0 || shardCount64 > routeSlotCount {
		return nil, fmt.Errorf("invalid SHARDS shard_count: must be between 1 and %d", routeSlotCount)
	}
	if rawSlots, present := payload["slots"]; present {
		slots, slotsErr := topologyInteger(rawSlots, "slots")
		if slotsErr != nil || slots != routeSlotCount {
			return nil, fmt.Errorf("invalid SHARDS slots: expected %d", routeSlotCount)
		}
	}
	rawRanges, ok := normalizeAdminResponse(payload["ranges"]).([]any)
	if !ok || len(rawRanges) == 0 || len(rawRanges) > routeSlotCount {
		return nil, errors.New("invalid SHARDS topology payload")
	}
	topology := &RoutingTopology{
		RouteEpoch: routeEpoch,
		ShardCount: int(shardCount64),
		endpoints:  make(map[string]RoutingEndpoint),
	}
	type shardRoute struct {
		lane     uint32
		endpoint string
		leader   string
	}
	shardRoutes := make(map[int]shardRoute, topology.ShardCount)
	for _, rawRange := range rawRanges {
		item, err := nativeMap(rawRange)
		if err != nil {
			return nil, err
		}
		if strings.EqualFold(asString(item["hint"]), "leader_unknown") {
			return nil, errors.New("SHARDS range has no leader")
		}
		first64, firstErr := topologyInteger(item["first_slot"], "first_slot")
		last64, lastErr := topologyInteger(item["last_slot"], "last_slot")
		shard64, shardErr := topologyInteger(item["shard"], "shard")
		lane64, laneErr := topologyInteger(item["lane_id"], "lane_id")
		if firstErr != nil || lastErr != nil || shardErr != nil || laneErr != nil {
			return nil, fmt.Errorf("invalid SHARDS range metadata: %#v", item)
		}
		endpoint, err := endpointFromRange(item)
		if err != nil {
			return nil, err
		}
		if first64 < 0 || last64 < first64 || last64 >= routeSlotCount || shard64 < 0 || shard64 >= shardCount64 || lane64 <= 0 || lane64 >= math.MaxUint32 {
			return nil, fmt.Errorf("invalid SHARDS range: %#v", item)
		}
		first, last, shard, laneID := int(first64), int(last64), int(shard64), uint32(lane64)
		key := endpointKey(endpoint)
		identity := shardRoute{lane: laneID, endpoint: key, leader: endpoint.Node}
		if previous, exists := shardRoutes[shard]; exists && previous != identity {
			return nil, fmt.Errorf("inconsistent route metadata for shard %d", shard)
		}
		shardRoutes[shard] = identity
		route := RoutingRoute{
			Shard:       shard,
			LaneID:      laneID,
			EndpointKey: key,
			Endpoint:    endpoint,
			LeaderNode:  endpoint.Node,
		}
		if previous, exists := topology.endpoints[key]; exists && previous != endpoint {
			return nil, fmt.Errorf("inconsistent identity for SHARDS endpoint %s", key)
		}
		topology.endpoints[key] = endpoint
		for slot := first; slot <= last; slot++ {
			if topology.slots[slot] != nil {
				return nil, fmt.Errorf("overlapping SHARDS ranges at slot %d", slot)
			}
			slotRoute := route
			slotRoute.Slot = slot
			topology.slots[slot] = &slotRoute
		}
	}
	if len(shardRoutes) != topology.ShardCount {
		return nil, fmt.Errorf("SHARDS declared %d shards but described %d", topology.ShardCount, len(shardRoutes))
	}
	for shard := 0; shard < topology.ShardCount; shard++ {
		if _, ok := shardRoutes[shard]; !ok {
			return nil, fmt.Errorf("SHARDS is missing shard %d", shard)
		}
	}
	for slot, route := range topology.slots {
		if route == nil {
			return nil, fmt.Errorf("SHARDS is missing slot %d", slot)
		}
	}
	return topology, nil
}

func endpointFromRange(item map[string]any) (RoutingEndpoint, error) {
	raw := item
	nestedEndpoint := false
	if endpointValue, ok := item["endpoint"]; ok && endpointValue != nil {
		endpointMap, err := nativeMap(endpointValue)
		if err != nil {
			return RoutingEndpoint{}, err
		}
		raw = endpointMap
		nestedEndpoint = true
	}
	host, hostErr := responseString(firstPresent(raw, "host", "native_host"), nil)
	host = strings.TrimSpace(host)
	nativePort64, err := topologyInteger(firstPresent(raw, "native_port"), "native_port")
	if hostErr != nil || err != nil || host == "" || nativePort64 <= 0 || nativePort64 > 65535 {
		return RoutingEndpoint{}, fmt.Errorf("invalid SHARDS endpoint: %#v", item)
	}
	nativeTLSPort := int64(0)
	if rawTLSPort := firstPresent(raw, "native_tls_port"); rawTLSPort != nil {
		nativeTLSPort, err = topologyInteger(rawTLSPort, "native_tls_port")
		if err != nil || nativeTLSPort <= 0 || nativeTLSPort > 65535 {
			return RoutingEndpoint{}, fmt.Errorf("invalid SHARDS TLS endpoint: %#v", item)
		}
	}
	node, err := optionalTopologyText(firstPresent(raw, "node", "leader_node", "owner_node"))
	if err != nil {
		return RoutingEndpoint{}, fmt.Errorf("invalid SHARDS endpoint node: %#v", item)
	}
	if node == "" && nestedEndpoint {
		node, err = optionalTopologyText(firstPresent(item, "node", "leader_node", "owner_node"))
		if err != nil {
			return RoutingEndpoint{}, fmt.Errorf("invalid SHARDS endpoint node: %#v", item)
		}
	}
	if node == "" {
		node = host
	}
	return RoutingEndpoint{
		Node:          node,
		Host:          host,
		NativePort:    int(nativePort64),
		NativeTLSPort: int(nativeTLSPort),
	}, nil
}

func optionalTopologyText(value any) (string, error) {
	if value == nil {
		return "", nil
	}
	text, err := responseString(value, nil)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(text), nil
}

func topologyInteger(value any, field string) (int64, error) {
	if value == nil {
		return 0, fmt.Errorf("%s is required", field)
	}
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		if uint64(v) > math.MaxInt64 {
			break
		}
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			break
		}
		return int64(v), nil
	case float32:
		if float32(int64(v)) == v {
			return int64(v), nil
		}
	case float64:
		if !math.IsNaN(v) && !math.IsInf(v, 0) && v >= math.MinInt64 && v <= math.MaxInt64 && float64(int64(v)) == v {
			return int64(v), nil
		}
	default:
		rv := reflect.ValueOf(value)
		if rv.IsValid() {
			switch rv.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				return rv.Int(), nil
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				u := rv.Uint()
				if u <= math.MaxInt64 {
					return int64(u), nil
				}
			}
		}
	}
	return 0, fmt.Errorf("%s must be an exact integer, got %T", field, value)
}

func firstPresent(mapping map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := mapping[key]; ok {
			return value
		}
	}
	return nil
}
