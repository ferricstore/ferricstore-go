package ferricstore

import (
	"context"
	"errors"
	"net"
	"strings"
)

const nativeStatusReroute = 5

// topologyRouteErrorDisposition reports whether an error makes the current
// topology suspect and whether the server explicitly permits replaying the
// request. Retry permission is deliberately fail-closed: error text is not a
// protocol disposition and must never make a possibly-applied command replay.
func topologyRouteErrorDisposition(err error) (refresh, safeToRetry bool) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false, false
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, errNativeConnectionUnavailable) {
		return true, false
	}
	nativeErr, ok := topologyNativeError(err)
	if !ok || !nativeErrorIsReroute(nativeErr) {
		return false, false
	}
	safe, _ := nativeErrorField(nativeErr.Value, "safe_to_retry").(bool)
	return true, safe
}

func topologyNativeError(err error) (NativeError, bool) {
	var value NativeError
	if errors.As(err, &value) {
		return value, true
	}
	var pointer *NativeError
	if errors.As(err, &pointer) && pointer != nil {
		return *pointer, true
	}
	return NativeError{}, false
}

func nativeErrorIsReroute(err NativeError) bool {
	if err.Status == nativeStatusReroute || strings.EqualFold(err.Kind, "reroute") {
		return true
	}
	code := nativeErrorField(err.Value, "code")
	switch value := code.(type) {
	case string:
		return strings.EqualFold(value, "reroute")
	case []byte:
		return strings.EqualFold(string(value), "reroute")
	default:
		return false
	}
}

func nativeErrorField(value any, name string) any {
	switch mapping := value.(type) {
	case map[string]any:
		return mapping[name]
	case map[interface{}]interface{}:
		for key, field := range mapping {
			switch typed := key.(type) {
			case string:
				if typed == name {
					return field
				}
			case []byte:
				if string(typed) == name {
					return field
				}
			}
		}
	}
	return nil
}

func isRetryableRouteError(err error) bool {
	refresh, _ := topologyRouteErrorDisposition(err)
	return refresh
}

func (e *TopologyNativeExecutor) refreshAndCanRetrySafeReroute(ctx context.Context, err error, attempt int) bool {
	refresh, safeToRetry := topologyRouteErrorDisposition(err)
	if !refresh || attempt != 0 {
		return false
	}
	if refreshErr := e.RefreshTopology(ctx); refreshErr != nil {
		return false
	}
	return safeToRetry
}

func (e *TopologyNativeExecutor) doNativeCommandWithSafeReroute(
	ctx context.Context,
	key any,
	command nativeCommand,
	route RoutingRoute,
	snapshot topologyRoutingSnapshot,
) (any, error) {
	for rerouteAttempt := 0; ; rerouteAttempt++ {
		adapter, err := e.adapterForTopologyRoute(route, snapshot)
		if err != nil {
			return nil, err
		}
		value, err := adapter.doNativeCommandOnLane(ctx, command, route.LaneID)
		if err == nil || !e.refreshAndCanRetrySafeReroute(ctx, err, rerouteAttempt) {
			return value, err
		}
		route, snapshot, err = e.routeWithRefreshSnapshot(ctx, key)
		if err != nil {
			return nil, err
		}
	}
}

func topologyPipelineRouteDisposition(items []pipelineItemResult) (routeErr error, safeToRetryAll bool) {
	safeToRetryAll = len(items) > 0
	for _, item := range items {
		refresh, safeToRetry := topologyRouteErrorDisposition(item.err)
		if refresh && routeErr == nil {
			routeErr = item.err
		}
		if !refresh || !safeToRetry {
			safeToRetryAll = false
		}
	}
	return routeErr, safeToRetryAll
}
