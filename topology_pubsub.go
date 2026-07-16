package ferricstore

import (
	"context"
	"errors"
)

// pubSubControlAdapter selects only currently active topology endpoints and
// retained seeds. It intentionally excludes lastSuccessfulURL when that URL is
// no longer part of either set, preventing a long-lived PubSub view from
// recreating an adapter that the latest topology retired.
func (e *TopologyNativeExecutor) pubSubControlAdapter(
	ctx context.Context,
	failed *NativeExecutor,
) (*NativeExecutor, error) {
	if err := e.assertOpen(); err != nil {
		return nil, err
	}
	var lastErr error
	for _, candidate := range e.activePubSubCandidateURLs() {
		adapter, err := e.adapterForURL(candidate)
		if err == nil && adapter == failed && nativeExecutorRetired(failed) {
			continue
		}
		if err == nil {
			err = adapter.ensureConnectedLocked(ctx)
		}
		if err == nil {
			adapter.enableEventDelivery()
			return adapter, nil
		}
		lastErr = err
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, errors.New("no active topology pubsub endpoint configured")
}

func nativeExecutorRetired(exec *NativeExecutor) bool {
	if exec == nil {
		return false
	}
	exec.mu.Lock()
	defer exec.mu.Unlock()
	return exec.retiring || exec.isClosed
}

func (e *TopologyNativeExecutor) activePubSubCandidateURLs() []string {
	e.mu.Lock()
	topology := e.topology
	seedURLs := append([]string(nil), e.seedURLs...)
	e.mu.Unlock()

	endpointCount := 0
	if topology != nil {
		endpointCount = len(topology.endpoints)
	}
	seen := make(map[string]struct{}, endpointCount+len(seedURLs))
	urls := make([]string, 0, endpointCount+len(seedURLs))
	add := func(candidate string) {
		if candidate == "" {
			return
		}
		if _, duplicate := seen[candidate]; duplicate {
			return
		}
		seen[candidate] = struct{}{}
		urls = append(urls, candidate)
	}
	if topology != nil {
		for _, endpoint := range topology.endpoints {
			if err := e.validateEndpoint(endpoint); err == nil {
				add(urlFromEndpoint(endpoint, e.tls))
			}
		}
	}
	for _, candidate := range seedURLs {
		add(candidate)
	}
	return urls
}
