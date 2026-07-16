package ferricstore

import (
	"context"
	"errors"
	"fmt"
)

func (e *TopologyNativeExecutor) RefreshTopology(ctx context.Context) error {
	return e.refreshTopology(ctx, nil)
}

func (e *TopologyNativeExecutor) refreshTopologyAtVersion(ctx context.Context, version uint64) error {
	return e.refreshTopology(ctx, &version)
}

func (e *TopologyNativeExecutor) refreshTopology(ctx context.Context, expectedVersion *uint64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for {
		e.refreshMu.Lock()
		if expectedVersion != nil {
			e.mu.Lock()
			current := e.topologyVersion
			e.mu.Unlock()
			if current != *expectedVersion {
				e.refreshMu.Unlock()
				return nil
			}
		}
		flight := e.refreshInFlight
		if flight != nil && flight.abandoned {
			done := flight.done
			e.refreshMu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		startFlight := false
		var refreshCtx context.Context
		if flight == nil {
			refreshCtx = e.eventContext
			if refreshCtx == nil {
				refreshCtx = context.Background()
			}
			var cancel context.CancelFunc
			refreshCtx, cancel = context.WithCancel(refreshCtx)
			flight = &topologyRefresh{done: make(chan struct{}), cancel: cancel}
			e.refreshInFlight = flight
			startFlight = true
		}
		flight.waiters++
		e.refreshMu.Unlock()
		if startFlight {
			go e.runTopologyRefresh(refreshCtx, flight)
		}

		completed := false
		select {
		case <-flight.done:
			completed = true
		case <-ctx.Done():
		}
		e.releaseTopologyRefreshWaiter(flight)
		if err := ctx.Err(); err != nil {
			return err
		}
		if completed {
			return flight.err
		}
	}
}

func (e *TopologyNativeExecutor) releaseTopologyRefreshWaiter(flight *topologyRefresh) {
	var cancel context.CancelFunc
	e.refreshMu.Lock()
	if flight.waiters > 0 {
		flight.waiters--
	}
	if flight.waiters == 0 && e.refreshInFlight == flight {
		flight.abandoned = true
		cancel = flight.cancel
	}
	e.refreshMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (e *TopologyNativeExecutor) runTopologyRefresh(ctx context.Context, flight *topologyRefresh) {
	err := e.refreshTopologyOnce(ctx)
	flight.cancel()
	e.refreshMu.Lock()
	flight.err = err
	if e.refreshInFlight == flight {
		e.refreshInFlight = nil
	}
	close(flight.done)
	e.refreshMu.Unlock()
}

func (e *TopologyNativeExecutor) refreshTopologyOnce(ctx context.Context) error {
	if err := e.assertOpen(); err != nil {
		return err
	}
	var lastErr error
	for _, candidate := range e.refreshCandidateURLs() {
		adapter, err := e.adapterForURL(candidate)
		if err != nil {
			lastErr = err
			continue
		}
		value, err := adapter.Do(ctx, "SHARDS")
		if err != nil {
			lastErr = err
			continue
		}
		topology, err := buildRoutingTopology(value)
		if err != nil {
			lastErr = err
			continue
		}
		if err := e.installTopology(topology); err != nil {
			return err
		}
		e.mu.Lock()
		e.lastSuccessfulURL = candidate
		e.mu.Unlock()
		if e.warmConnections {
			e.warmTopologyConnections(ctx, topology)
		}
		return nil
	}
	if lastErr == nil {
		lastErr = errors.New("no topology endpoints available")
	}
	return fmt.Errorf("no FerricStore topology endpoint reachable: %w", lastErr)
}

func (e *TopologyNativeExecutor) warmTopologyConnections(ctx context.Context, topology *RoutingTopology) {
	const maxConcurrentWarmups = 8
	tasks := make([]func(), 0, len(topology.endpoints))
	for _, endpoint := range topology.endpoints {
		endpoint := endpoint
		tasks = append(tasks, func() {
			if ctx.Err() != nil {
				return
			}
			adapter, err := e.adapterForEndpoint(endpoint)
			if err == nil {
				_ = adapter.ensureConnectedLocked(ctx)
			}
		})
	}
	runBoundedTopologyTasks(maxConcurrentWarmups, tasks)
}
