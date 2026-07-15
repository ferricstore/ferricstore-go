package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

func (e *TopologyNativeExecutor) controlDo(ctx context.Context, args ...any) (any, error) {
	adapter, err := e.controlAdapter(ctx)
	if err != nil {
		return nil, err
	}
	return adapter.Do(ctx, args...)
}

func (e *TopologyNativeExecutor) controlAdapter(ctx context.Context) (*NativeExecutor, error) {
	if err := e.assertOpen(); err != nil {
		return nil, err
	}
	var lastErr error
	for _, candidate := range e.refreshCandidateURLs() {
		adapter, err := e.adapterForURL(candidate)
		if err == nil {
			err = adapter.ensureConnectedLocked(ctx)
		}
		if err == nil {
			return adapter, nil
		}
		lastErr = err
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, errors.New("no topology control endpoint configured")
}

func (e *TopologyNativeExecutor) acquireCommandSession(ctx context.Context, keys ...any) (commandSession, error) {
	if len(keys) == 0 {
		return nil, errors.New("topology transactions require at least one routing key; use Watch or TransactionForKeys")
	}
	for index, key := range keys {
		if !isRouteKey(key) {
			return nil, fmt.Errorf("transaction key %d has unsupported type %T", index, key)
		}
	}
	key, sameSlot := singleShardKey(keys)
	if !sameSlot {
		return nil, errors.New("topology transaction keys must use one hash slot")
	}
	route, snapshot, err := e.routeWithRefreshSnapshot(ctx, key)
	if err != nil {
		return nil, err
	}
	adapter, err := e.adapterForTopologyRoute(route, snapshot)
	if err != nil {
		return nil, err
	}
	return adapter.acquireCommandSessionOnLane(ctx, route.LaneID)
}

func (e *TopologyNativeExecutor) adapterForURL(rawurl string) (*NativeExecutor, error) {
	parsed, err := parseFerricURL(rawurl)
	if err != nil {
		return nil, err
	}
	endpoint := parsed.Endpoint()
	key := connectionKeyForEndpoint(endpoint, parsed.TLS)
	if _, seed := e.seedEndpointKeys[key]; !seed {
		if err := e.validateEndpoint(endpoint); err != nil {
			return nil, err
		}
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, errTopologyClosed
	}
	existing := e.adapters[key]
	e.mu.Unlock()
	if existing != nil {
		return existing, nil
	}
	adapter, err := NewNativeExecutorFromURL(parsed.URL(), e.nativeOptions...)
	if err != nil {
		return nil, err
	}
	if adapter.opts.TLS != e.tls {
		_ = adapter.Close()
		return nil, errTopologyTransportConflict
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		_ = adapter.Close()
		return nil, errTopologyClosed
	}
	if existing = e.adapters[key]; existing != nil {
		e.mu.Unlock()
		_ = adapter.Close()
		return existing, nil
	}
	e.adapters[key] = adapter
	e.mu.Unlock()
	return adapter, nil
}

func (e *TopologyNativeExecutor) adapterForEndpoint(endpoint RoutingEndpoint) (*NativeExecutor, error) {
	return e.adapterForEndpointAtSnapshot(endpoint, topologyRoutingSnapshot{})
}

func (e *TopologyNativeExecutor) adapterForTopologyRoute(
	route RoutingRoute,
	snapshot topologyRoutingSnapshot,
) (*NativeExecutor, error) {
	return e.adapterForEndpointAtSnapshot(route.Endpoint, snapshot)
}

func (e *TopologyNativeExecutor) adapterForEndpointAtSnapshot(
	endpoint RoutingEndpoint,
	snapshot topologyRoutingSnapshot,
) (*NativeExecutor, error) {
	if err := e.validateEndpoint(endpoint); err != nil {
		return nil, err
	}
	key := connectionKeyForEndpoint(endpoint, e.tls)
	e.mu.Lock()
	if err := e.routingSnapshotErrorLocked(snapshot); err != nil {
		e.mu.Unlock()
		return nil, err
	}
	seedURL := e.seedURLByKey[key]
	e.mu.Unlock()
	if seedURL != "" {
		adapter, err := e.adapterForURL(seedURL)
		if err != nil {
			return nil, err
		}
		e.mu.Lock()
		err = e.routingSnapshotErrorLocked(snapshot)
		e.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return adapter, nil
	}
	e.mu.Lock()
	if err := e.routingSnapshotErrorLocked(snapshot); err != nil {
		e.mu.Unlock()
		return nil, err
	}
	existing := e.adapters[key]
	e.mu.Unlock()
	if existing != nil {
		return existing, nil
	}
	adapter, err := NewNativeExecutorFromURL(urlFromEndpoint(endpoint, e.tls), e.nativeOptions...)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	if err := e.routingSnapshotErrorLocked(snapshot); err != nil {
		e.mu.Unlock()
		_ = adapter.Close()
		return nil, err
	}
	if existing = e.adapters[key]; existing != nil {
		e.mu.Unlock()
		_ = adapter.Close()
		return existing, nil
	}
	e.adapters[key] = adapter
	e.mu.Unlock()
	return adapter, nil
}

func (e *TopologyNativeExecutor) validateEndpoint(endpoint RoutingEndpoint) error {
	host := strings.ToLower(endpoint.Host)
	if host == "" || endpoint.NativePort <= 0 {
		return fmt.Errorf("invalid learned endpoint: %#v", endpoint)
	}
	allowed := false
	switch e.endpointPolicy {
	case "", EndpointPolicySeedHosts:
		_, seedOK := e.seedEndpointKeys[connectionKeyForEndpoint(endpoint, e.tls)]
		_, trustedOK := e.trustedHosts[host]
		allowed = seedOK || trustedOK
	case EndpointPolicyNone:
		_, allowed = e.seedEndpointKeys[connectionKeyForEndpoint(endpoint, e.tls)]
	case EndpointPolicyAny:
		allowed = true
	default:
		return fmt.Errorf("invalid endpoint policy %q", e.endpointPolicy)
	}
	if !allowed {
		return fmt.Errorf("unsafe learned endpoint: %#v", endpoint)
	}
	if e.endpointValidator != nil && !e.endpointValidator(endpoint) {
		return fmt.Errorf("unsafe learned endpoint: %#v", endpoint)
	}
	return nil
}

func (e *TopologyNativeExecutor) refreshCandidateURLs() []string {
	e.mu.Lock()
	topology := e.topology
	seedURLs := append([]string(nil), e.seedURLs...)
	lastSuccessful := e.lastSuccessfulURL
	e.mu.Unlock()

	endpointCount := 0
	if topology != nil {
		endpointCount = len(topology.endpoints)
	}
	seen := make(map[string]struct{}, len(seedURLs)+endpointCount)
	urls := make([]string, 0, len(seedURLs)+endpointCount)
	add := func(candidate string) {
		if candidate == "" {
			return
		}
		if _, ok := seen[candidate]; ok {
			return
		}
		seen[candidate] = struct{}{}
		urls = append(urls, candidate)
	}
	add(lastSuccessful)
	for _, candidate := range seedURLs {
		add(candidate)
	}
	if topology != nil {
		for _, endpoint := range topology.endpoints {
			if err := e.validateEndpoint(endpoint); err != nil {
				continue
			}
			add(urlFromEndpoint(endpoint, e.tls))
		}
	}
	return urls
}

func (e *TopologyNativeExecutor) assertOpen() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return errTopologyClosed
	}
	return nil
}

func (e *TopologyNativeExecutor) installTopology(topology *RoutingTopology) error {
	if topology == nil {
		return errors.New("cannot install nil routing topology")
	}
	for _, endpoint := range topology.endpoints {
		if err := e.validateEndpoint(endpoint); err != nil {
			return fmt.Errorf("reject routing topology endpoint: %w", err)
		}
	}
	active := make(map[string]struct{}, len(e.seedEndpointKeys)+len(topology.endpoints))
	for key := range e.seedEndpointKeys {
		active[key] = struct{}{}
	}
	for _, endpoint := range topology.endpoints {
		active[connectionKeyForEndpoint(endpoint, e.tls)] = struct{}{}
	}

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return errTopologyClosed
	}
	identical := routingTopologiesEqual(e.topology, topology)
	retired := make([]*NativeExecutor, 0)
	if e.retiringAdapters == nil {
		e.retiringAdapters = make(map[*NativeExecutor]struct{})
	}
	for key, adapter := range e.adapters {
		if _, keep := active[key]; keep {
			continue
		}
		delete(e.adapters, key)
		e.retiringAdapters[adapter] = struct{}{}
		retired = append(retired, adapter)
	}
	if !identical {
		e.topology = topology
		e.topologyVersion++
	}
	e.mu.Unlock()
	for _, adapter := range retired {
		adapter.retire()
		go e.forgetRetiredWhenClosed(adapter)
	}
	return nil
}

func (e *TopologyNativeExecutor) forgetRetiredWhenClosed(adapter *NativeExecutor) {
	adapter.mu.Lock()
	closed := adapter.closed
	adapter.mu.Unlock()
	<-closed
	e.mu.Lock()
	delete(e.retiringAdapters, adapter)
	e.mu.Unlock()
}
