package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"
)

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
	disposition := nativeServerRetryDisposition(err)
	if !disposition.reroute {
		return false, false
	}
	return true, disposition.retryable
}

func isRetryableRouteError(err error) bool {
	refresh, _ := topologyRouteErrorDisposition(err)
	return refresh
}

func (e *TopologyNativeExecutor) refreshAndCanRetrySafeReroute(ctx context.Context, err error, attempt int) (bool, error) {
	refresh, safeToRetry := topologyRouteErrorDisposition(err)
	if !refresh || attempt != 0 {
		return false, nil
	}
	if disposition := nativeServerRetryDisposition(err); safeToRetry {
		if waitErr := waitNativeRetry(ctx, disposition.retryAfter); waitErr != nil {
			return false, waitErr
		}
	}
	if refreshErr := e.RefreshTopology(ctx); refreshErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, ctxErr
		}
		return false, nil
	}
	return safeToRetry, nil
}

func (e *TopologyNativeExecutor) refreshRouteWithoutReplay(ctx context.Context, err error, attempt int) {
	refresh, _ := topologyRouteErrorDisposition(err)
	if !refresh || attempt != 0 {
		return
	}
	// Preserve the original unknown/stale mutation outcome while refreshing
	// routing state for later independent requests.
	_ = e.RefreshTopology(ctx)
}

func (e *TopologyNativeExecutor) doNativeCommandWithSafeReroute(
	ctx context.Context,
	key any,
	command nativeCommand,
	route RoutingRoute,
	snapshot topologyRoutingSnapshot,
) (any, error) {
	var operationCtx context.Context
	var cancel context.CancelFunc
	for rerouteAttempt := 0; ; rerouteAttempt++ {
		adapter, err := e.adapterForTopologyRoute(route, snapshot)
		if err != nil {
			return nil, err
		}
		if operationCtx == nil {
			operationCtx, cancel = nativeContextWithBudget(ctx, adapter.opts.Timeout, command.budget)
			if cancel != nil {
				defer cancel()
			}
		}
		value, err := adapter.doNativeCommandOnLane(operationCtx, command, route.LaneID)
		if err == nil {
			return value, err
		}
		if command.replayPolicy == nativeReplayNever {
			e.refreshRouteWithoutReplay(operationCtx, err, rerouteAttempt)
			return value, err
		}
		retry, retryErr := e.refreshAndCanRetrySafeReroute(operationCtx, err, rerouteAttempt)
		if retryErr != nil {
			return nil, fmt.Errorf("%s retry backoff: %w", command.name, retryErr)
		}
		if !retry {
			return value, err
		}
		route, snapshot, err = e.routeWithRefreshSnapshot(operationCtx, key)
		if err != nil {
			return nil, err
		}
	}
}

func topologyPipelineRouteDisposition(items []pipelineItemResult) (routeErr error, safeToRetryAll bool) {
	safeToRetryAll = len(items) > 0
	var retryAfter time.Duration
	for _, item := range items {
		refresh, safeToRetry := topologyRouteErrorDisposition(item.err)
		if refresh {
			delay := nativeServerRetryDisposition(item.err).retryAfter
			if routeErr == nil || delay > retryAfter {
				routeErr = item.err
				retryAfter = delay
			}
		}
		if !refresh || !safeToRetry {
			safeToRetryAll = false
		}
	}
	return routeErr, safeToRetryAll
}
