package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

const routeSlotCount = 1024
const routeSlotMask = routeSlotCount - 1
const maxTopologyConcurrentTasks = 32

type EndpointPolicy string

const (
	EndpointPolicySeedHosts EndpointPolicy = "seed_hosts"
	EndpointPolicyAny       EndpointPolicy = "any"
	EndpointPolicyNone      EndpointPolicy = "none"
)

type RoutingEndpoint struct {
	Node          string
	Host          string
	NativePort    int
	NativeTLSPort int
}

type RoutingRoute struct {
	Shard       int
	LaneID      uint32
	EndpointKey string
	Endpoint    RoutingEndpoint
	LeaderNode  string
	Slot        int
}

type RoutingTopology struct {
	RouteEpoch int64
	ShardCount int

	slots     [routeSlotCount]*RoutingRoute
	endpoints map[string]RoutingEndpoint
}

func (t *RoutingTopology) RouteKey(key any) (RoutingRoute, error) {
	if t == nil {
		return RoutingRoute{}, errors.New("ferricstore topology is empty")
	}
	if !isRouteKey(key) {
		return RoutingRoute{}, fmt.Errorf("unsupported routing key type %T", key)
	}
	slot := routeSlotForKey(key)
	route := t.slots[slot]
	if route == nil {
		return RoutingRoute{}, fmt.Errorf("no route for slot %d", slot)
	}
	out := *route
	out.Slot = slot
	return out, nil
}

func (t *RoutingTopology) Endpoints() map[string]RoutingEndpoint {
	if t == nil || len(t.endpoints) == 0 {
		return map[string]RoutingEndpoint{}
	}
	out := make(map[string]RoutingEndpoint, len(t.endpoints))
	for key, endpoint := range t.endpoints {
		out[key] = endpoint
	}
	return out
}

func emptyRoutingTopology() *RoutingTopology {
	return &RoutingTopology{endpoints: make(map[string]RoutingEndpoint)}
}

type TopologyOption func(*topologyNativeOptions)

type topologyNativeOptions struct {
	endpointPolicy    EndpointPolicy
	endpointValidator func(RoutingEndpoint) bool
	nativeOptions     []NativeOption
	clientOptions     []ClientOption
	trustedHosts      []string
	warmConnections   bool
	crossShardWrites  CrossShardWritePolicy
}

// CrossShardWritePolicy controls destructive multi-key commands whose keys
// resolve to more than one shard. Rejecting them is the safe default because a
// server error can otherwise leave only some shards modified.
type CrossShardWritePolicy uint8

const (
	// CrossShardWriteReject prevents destructive commands from partially
	// succeeding across shards. It is the default policy.
	CrossShardWriteReject CrossShardWritePolicy = iota
	// CrossShardWritePerShard permits independent per-shard execution (or
	// per-slot execution when the server requires slot-local atomicity) and
	// reports any mixed outcome as TopologyPartialWriteError.
	CrossShardWritePerShard
)

// WithTopologyCrossShardWritePolicy explicitly opts into or rejects
// per-shard execution of destructive multi-key commands such as DEL and
// UNLINK, and per-slot execution of MSET.
func WithTopologyCrossShardWritePolicy(policy CrossShardWritePolicy) TopologyOption {
	return func(opts *topologyNativeOptions) {
		opts.crossShardWrites = policy
	}
}

func WithTopologyEndpointPolicy(policy EndpointPolicy) TopologyOption {
	return func(opts *topologyNativeOptions) {
		opts.endpointPolicy = policy
	}
}

func WithTopologyEndpointValidator(validator func(RoutingEndpoint) bool) TopologyOption {
	return func(opts *topologyNativeOptions) {
		opts.endpointValidator = validator
	}
}

func WithTopologyNativeOptions(opts ...NativeOption) TopologyOption {
	return func(options *topologyNativeOptions) {
		options.nativeOptions = append(options.nativeOptions, opts...)
	}
}

// WithTopologyClientOptions applies client-level options, such as codecs, when
// constructing an owning client with NewTopologyClientFromURLs.
func WithTopologyClientOptions(opts ...ClientOption) TopologyOption {
	return func(options *topologyNativeOptions) {
		options.clientOptions = append(options.clientOptions, opts...)
	}
}

func WithTopologyTrustedHosts(hosts ...string) TopologyOption {
	return func(opts *topologyNativeOptions) {
		opts.trustedHosts = append(opts.trustedHosts, hosts...)
	}
}

func WithTopologyWarmConnections(warm bool) TopologyOption {
	return func(opts *topologyNativeOptions) {
		opts.warmConnections = warm
	}
}

type TopologyNativeExecutor struct {
	mu        sync.Mutex
	refreshMu sync.Mutex
	eventWG   sync.WaitGroup

	adapters              map[string]*NativeExecutor
	retiringAdapters      map[*NativeExecutor]struct{}
	closed                bool
	endpointPolicy        EndpointPolicy
	endpointValidator     func(RoutingEndpoint) bool
	nativeOptions         []NativeOption
	clientOptions         []ClientOption
	seedEndpointKeys      map[string]struct{}
	seedURLByKey          map[string]string
	seedURLs              []string
	lastSuccessfulURL     string
	topologyVersion       uint64
	refreshInFlight       *topologyRefresh
	refreshSignals        chan struct{}
	eventContext          context.Context
	cancelEvents          context.CancelFunc
	tls                   bool
	topology              *RoutingTopology
	trustedHosts          map[string]struct{}
	warmConnections       bool
	crossShardWrites      CrossShardWritePolicy
	maxMSetPreflightBytes int
}

type topologyRefresh struct {
	done      chan struct{}
	cancel    context.CancelFunc
	err       error
	waiters   int
	abandoned bool
}

var errTopologyClosed = errors.New("ferricstore topology executor is closed")
var errTopologyTransportConflict = errors.New("ferricstore topology native TLS options conflict with the seed URL scheme; select transport with ferric:// or ferrics:// consistently")

func NewTopologyNativeExecutor(urls []string, opts ...TopologyOption) (*TopologyNativeExecutor, error) {
	if len(urls) == 0 {
		return nil, errors.New("ferricstore topology executor requires at least one seed URL")
	}
	options := topologyNativeOptions{endpointPolicy: EndpointPolicySeedHosts}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	switch options.endpointPolicy {
	case "", EndpointPolicySeedHosts, EndpointPolicyAny, EndpointPolicyNone:
	default:
		return nil, fmt.Errorf("invalid endpoint policy %q", options.endpointPolicy)
	}
	if options.crossShardWrites != CrossShardWriteReject && options.crossShardWrites != CrossShardWritePerShard {
		return nil, fmt.Errorf("invalid cross-shard write policy %d", options.crossShardWrites)
	}
	seedURLs := make([]string, 0, len(urls))
	seedEndpointKeys := make(map[string]struct{}, len(urls))
	seedURLByKey := make(map[string]string, len(urls))
	tlsEnabled := false
	for index, raw := range urls {
		parsed, err := parseFerricURL(raw)
		if err != nil {
			return nil, err
		}
		normalized := parsed.URL()
		seedURLs = append(seedURLs, normalized)
		if index == 0 {
			tlsEnabled = parsed.TLS
		} else if parsed.TLS != tlsEnabled {
			return nil, errors.New("ferricstore topology executor cannot mix ferric:// and ferrics:// seed URLs")
		}
		key := connectionKeyForEndpoint(parsed.Endpoint(), parsed.TLS)
		if existing, ok := seedURLByKey[key]; ok {
			previous, _ := parseFerricURL(existing)
			if previous.CredentialsSet != parsed.CredentialsSet || previous.Username != parsed.Username || previous.Password != parsed.Password {
				return nil, fmt.Errorf("conflicting credentials for topology seed %s", key)
			}
			continue
		}
		seedEndpointKeys[key] = struct{}{}
		seedURLByKey[key] = normalized
	}
	transportProbe := defaultNativeOptions("127.0.0.1", tlsEnabled)
	applyNativeOptions(&transportProbe, options.nativeOptions...)
	if transportProbe.TLS != tlsEnabled {
		return nil, errTopologyTransportConflict
	}
	exec := &TopologyNativeExecutor{
		adapters:              make(map[string]*NativeExecutor),
		retiringAdapters:      make(map[*NativeExecutor]struct{}),
		endpointPolicy:        options.endpointPolicy,
		endpointValidator:     options.endpointValidator,
		nativeOptions:         append([]NativeOption(nil), options.nativeOptions...),
		clientOptions:         append([]ClientOption(nil), options.clientOptions...),
		seedEndpointKeys:      seedEndpointKeys,
		seedURLByKey:          seedURLByKey,
		seedURLs:              seedURLs,
		tls:                   tlsEnabled,
		topology:              emptyRoutingTopology(),
		trustedHosts:          normalizedStringSet(options.trustedHosts),
		warmConnections:       options.warmConnections,
		crossShardWrites:      options.crossShardWrites,
		maxMSetPreflightBytes: nativeMaxFrameBytes,
		refreshSignals:        make(chan struct{}, 1),
	}
	exec.eventContext, exec.cancelEvents = context.WithCancel(context.Background())
	exec.nativeOptions = append(exec.nativeOptions, withNativeEventSubscription(
		[]string{"TOPOLOGY_CHANGED"}, exec.handleNativeManagementEvent,
	))
	exec.eventWG.Add(1)
	go exec.topologyEventRefreshLoop()
	return exec, nil
}

func NewTopologyClientFromURLs(urls []string, opts ...TopologyOption) (*Client, error) {
	exec, err := NewTopologyNativeExecutor(urls, opts...)
	if err != nil {
		return nil, err
	}
	client := NewClientWithExecutor(exec, exec.clientOptions...)
	client.closer = exec.Close
	return client, nil
}

func (e *TopologyNativeExecutor) Do(ctx context.Context, args ...any) (any, error) {
	return e.doUnlocked(ctx, args...)
}

func (e *TopologyNativeExecutor) doUnlocked(ctx context.Context, args ...any) (any, error) {
	if err := e.assertOpen(); err != nil {
		return nil, err
	}
	if name, mutates := connectionStateMutationCommand(args); mutates {
		return nil, fmt.Errorf("%s is connection-local and cannot be applied safely to a topology executor", name)
	}
	if name, keys, ok := safeScatterCommand(args); ok && len(keys) > 0 {
		requestContext, hasRequestContext := topologyRequestContext(args)
		return e.scatterCommandWithContext(ctx, name, keys, requestContext, hasRequestContext)
	}
	routeData, err := e.routeData(ctx, args)
	if err == errTopologyCrossSlotMSet {
		keys, values, requestContext, hasRequestContext, _, parseErr := topologyMSetCommand(args)
		if parseErr != nil {
			return nil, parseErr
		}
		return e.scatterMSet(ctx, keys, values, requestContext, hasRequestContext)
	}
	if err != nil {
		return nil, err
	}
	if routeData == nil {
		return e.controlDo(ctx, args...)
	}
	for rerouteAttempt := 0; ; rerouteAttempt++ {
		adapter, err := e.adapterForTopologyRoute(routeData.route, routeData.snapshot)
		if err != nil {
			return nil, err
		}
		value, err := adapter.doNativeCommandOnLane(ctx, routeData.command, routeData.route.LaneID)
		if err == nil || !e.refreshAndCanRetrySafeReroute(ctx, err, rerouteAttempt) {
			return value, err
		}
		routeData, err = e.routeData(ctx, args)
		if err != nil {
			return nil, err
		}
		if routeData == nil {
			return nil, errTopologyStaleRoute
		}
	}
}

func (e *TopologyNativeExecutor) routeWithRefresh(ctx context.Context, key any) (RoutingRoute, error) {
	route, _, err := e.routeWithRefreshSnapshot(ctx, key)
	return route, err
}

func (e *TopologyNativeExecutor) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	if e.cancelEvents != nil {
		e.cancelEvents()
	}
	adapterSet := make(map[*NativeExecutor]struct{}, len(e.adapters)+len(e.retiringAdapters))
	for _, adapter := range e.adapters {
		adapterSet[adapter] = struct{}{}
	}
	for adapter := range e.retiringAdapters {
		adapterSet[adapter] = struct{}{}
	}
	e.adapters = make(map[string]*NativeExecutor)
	e.retiringAdapters = make(map[*NativeExecutor]struct{})
	e.mu.Unlock()

	var firstErr error
	for adapter := range adapterSet {
		if err := adapter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	e.eventWG.Wait()
	return firstErr
}

func (e *TopologyNativeExecutor) handleNativeManagementEvent(value nativeServerEvent) {
	event, err := nativeEventFromServerValue(value)
	if err != nil || !strings.EqualFold(event.Name, "TOPOLOGY_CHANGED") {
		return
	}
	select {
	case e.refreshSignals <- struct{}{}:
	default:
	}
}

func (e *TopologyNativeExecutor) topologyEventRefreshLoop() {
	defer e.eventWG.Done()
	ctx := e.eventContext
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.refreshSignals:
		}

		// If an event arrives while a SHARDS request is already in flight, wait
		// for that snapshot and then fetch once more. The event can describe a
		// change newer than the response currently being installed.
		e.refreshMu.Lock()
		flight := e.refreshInFlight
		e.refreshMu.Unlock()
		if flight != nil {
			select {
			case <-flight.done:
			case <-ctx.Done():
				return
			}
		}
		if ctx.Err() == nil {
			_ = e.RefreshTopology(ctx)
		}
	}
}

func (e *TopologyNativeExecutor) Route(key any) (RoutingRoute, error) {
	for range maxTopologyPlanningAttempts {
		snapshot, err := e.captureRoutingTopology()
		if err != nil {
			return RoutingRoute{}, err
		}
		route, err := e.routeInSnapshot(snapshot, key)
		if err != nil {
			return RoutingRoute{}, err
		}
		if e.routingSnapshotCurrent(snapshot) {
			return route, nil
		}
	}
	return RoutingRoute{}, errTopologyStaleRoute
}

func (e *TopologyNativeExecutor) routeData(ctx context.Context, args []any) (*topologyRouteData, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	refreshed := false
	for range maxTopologyPlanningAttempts {
		snapshot, err := e.captureRoutingTopology()
		if err != nil {
			return nil, err
		}
		routeData, err := e.routeDataInSnapshot(args, snapshot)
		if err != nil {
			var lookupErr *topologyRouteLookupError
			if !errors.As(err, &lookupErr) || refreshed {
				return nil, err
			}
			if refreshErr := e.refreshTopologyAtVersion(ctx, snapshot.version); refreshErr != nil {
				return nil, refreshErr
			}
			refreshed = true
			continue
		}
		if e.routingSnapshotCurrent(snapshot) {
			return routeData, nil
		}
	}
	return nil, errTopologyStaleRoute
}
