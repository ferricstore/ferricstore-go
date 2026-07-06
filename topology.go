package ferricstore

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

const routeSlotCount = 1024
const routeSlotMask = routeSlotCount - 1

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
	trustedHosts      []string
	warmConnections   bool
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
	mu sync.Mutex

	adapters          map[string]*NativeExecutor
	endpointPolicy    EndpointPolicy
	endpointValidator func(RoutingEndpoint) bool
	nativeOptions     []NativeOption
	seedEndpointKeys  map[string]struct{}
	seedURLs          []string
	tls               bool
	topology          *RoutingTopology
	trustedHosts      map[string]struct{}
	warmConnections   bool
}

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
	seedURLs := make([]string, 0, len(urls))
	seedEndpointKeys := make(map[string]struct{}, len(urls))
	tlsEnabled := false
	for _, raw := range urls {
		parsed, err := parseFerricURL(raw)
		if err != nil {
			return nil, err
		}
		normalized := parsed.URL()
		seedURLs = append(seedURLs, normalized)
		seedEndpointKeys[endpointKey(parsed.Endpoint())] = struct{}{}
		tlsEnabled = tlsEnabled || parsed.TLS
	}
	nativeOptions := append(seedCredentialOptions(urls), options.nativeOptions...)
	exec := &TopologyNativeExecutor{
		adapters:          make(map[string]*NativeExecutor),
		endpointPolicy:    options.endpointPolicy,
		endpointValidator: options.endpointValidator,
		nativeOptions:     nativeOptions,
		seedEndpointKeys:  seedEndpointKeys,
		seedURLs:          seedURLs,
		tls:               tlsEnabled,
		topology:          emptyRoutingTopology(),
		trustedHosts:      normalizedStringSet(options.trustedHosts),
		warmConnections:   options.warmConnections,
	}
	return exec, nil
}

func NewTopologyClientFromURLs(urls []string, opts ...TopologyOption) (*Client, error) {
	exec, err := NewTopologyNativeExecutor(urls, opts...)
	if err != nil {
		return nil, err
	}
	client := NewClientWithExecutor(exec)
	client.closer = exec.Close
	return client, nil
}

func (e *TopologyNativeExecutor) Do(ctx context.Context, args ...any) (any, error) {
	routeData, err := e.routeData(ctx, args)
	if err != nil {
		return nil, err
	}
	if routeData == nil {
		return e.controlDo(ctx, args...)
	}
	adapter, err := e.adapterForEndpoint(routeData.route.Endpoint)
	if err != nil {
		return nil, err
	}
	value, err := adapter.doNativeCommandOnLane(ctx, routeData.command, routeData.route.LaneID)
	if err != nil && isRetryableRouteError(err) {
		_ = e.RefreshTopology(ctx)
	}
	return value, err
}

func (e *TopologyNativeExecutor) Pipeline(ctx context.Context, commands [][]any) ([]any, error) {
	if len(commands) == 0 {
		return nil, nil
	}
	route, ok, err := e.singleRouteForCommands(ctx, commands)
	if err != nil {
		return nil, err
	}
	if !ok {
		adapter, err := e.controlAdapter(ctx)
		if err != nil {
			return nil, err
		}
		return adapter.Pipeline(ctx, commands)
	}
	adapter, err := e.adapterForEndpoint(route.Endpoint)
	if err != nil {
		return nil, err
	}
	values, err := adapter.pipelineOnLane(ctx, commands, route.LaneID)
	if err != nil && isRetryableRouteError(err) {
		_ = e.RefreshTopology(ctx)
	}
	return values, err
}

func (e *TopologyNativeExecutor) Close() error {
	e.mu.Lock()
	adapters := make([]*NativeExecutor, 0, len(e.adapters))
	for _, adapter := range e.adapters {
		adapters = append(adapters, adapter)
	}
	e.adapters = make(map[string]*NativeExecutor)
	e.mu.Unlock()

	var firstErr error
	for _, adapter := range adapters {
		if err := adapter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (e *TopologyNativeExecutor) RefreshTopology(ctx context.Context) error {
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
		e.mu.Lock()
		e.topology = topology
		e.mu.Unlock()
		if e.warmConnections {
			for _, endpoint := range topology.endpoints {
				_, _ = e.adapterForEndpoint(endpoint)
			}
		}
		return nil
	}
	if lastErr == nil {
		lastErr = errors.New("no topology endpoints available")
	}
	return fmt.Errorf("no FerricStore topology endpoint reachable: %w", lastErr)
}

func (e *TopologyNativeExecutor) Route(key any) (RoutingRoute, error) {
	e.mu.Lock()
	topology := e.topology
	e.mu.Unlock()
	route, err := topology.RouteKey(key)
	if err != nil {
		return RoutingRoute{}, err
	}
	if err := e.validateEndpoint(route.Endpoint); err != nil {
		return RoutingRoute{}, err
	}
	return route, nil
}

func (e *TopologyNativeExecutor) routeData(ctx context.Context, args []any) (*topologyRouteData, error) {
	if len(args) == 0 {
		return nil, nil
	}
	command, err := buildNativeCommand(args)
	if err != nil {
		return nil, err
	}
	key, ok := routingKeyForBuiltCommand(args, command)
	if !ok {
		return nil, nil
	}
	route, err := e.Route(key)
	if err != nil {
		if refreshErr := e.RefreshTopology(ctx); refreshErr != nil {
			return nil, err
		}
		route, err = e.Route(key)
		if err != nil {
			return nil, err
		}
	}
	return &topologyRouteData{command: command, route: route}, nil
}

func (e *TopologyNativeExecutor) singleRouteForCommands(ctx context.Context, commands [][]any) (RoutingRoute, bool, error) {
	var route RoutingRoute
	hasRoute := false
	for _, command := range commands {
		routeData, err := e.routeData(ctx, command)
		if err != nil {
			return RoutingRoute{}, false, err
		}
		if routeData == nil || routeData.command.flags != 0 {
			return RoutingRoute{}, false, nil
		}
		if !hasRoute {
			route = routeData.route
			hasRoute = true
			continue
		}
		if route.EndpointKey != routeData.route.EndpointKey || route.LaneID != routeData.route.LaneID {
			return RoutingRoute{}, false, nil
		}
	}
	return route, hasRoute, nil
}

type topologyRouteData struct {
	command nativeCommand
	route   RoutingRoute
}

func (e *TopologyNativeExecutor) controlDo(ctx context.Context, args ...any) (any, error) {
	adapter, err := e.controlAdapter(ctx)
	if err != nil {
		return nil, err
	}
	return adapter.Do(ctx, args...)
}

func (e *TopologyNativeExecutor) controlAdapter(ctx context.Context) (*NativeExecutor, error) {
	var lastErr error
	for _, candidate := range e.refreshCandidateURLs() {
		adapter, err := e.adapterForURL(candidate)
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

func (e *TopologyNativeExecutor) adapterForURL(rawurl string) (*NativeExecutor, error) {
	parsed, err := parseFerricURL(rawurl)
	if err != nil {
		return nil, err
	}
	key := endpointKey(parsed.Endpoint())
	e.mu.Lock()
	existing := e.adapters[key]
	e.mu.Unlock()
	if existing != nil {
		return existing, nil
	}
	adapter, err := NewNativeExecutorFromURL(parsed.URL(), e.nativeOptions...)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
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
	if err := e.validateEndpoint(endpoint); err != nil {
		return nil, err
	}
	key := endpointKey(endpoint)
	e.mu.Lock()
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
		_, seedOK := e.seedEndpointKeys[endpointKey(endpoint)]
		_, trustedOK := e.trustedHosts[host]
		allowed = seedOK || trustedOK
	case EndpointPolicyAny, EndpointPolicyNone:
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
	e.mu.Unlock()

	endpointCount := 0
	if topology != nil {
		endpointCount = len(topology.endpoints)
	}
	seen := make(map[string]struct{}, len(seedURLs)+endpointCount)
	urls := make([]string, 0, len(seedURLs)+endpointCount)
	for _, candidate := range seedURLs {
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		urls = append(urls, candidate)
	}
	if topology != nil {
		for _, endpoint := range topology.endpoints {
			candidate := urlFromEndpoint(endpoint, e.tls)
			if _, ok := seen[candidate]; ok {
				continue
			}
			seen[candidate] = struct{}{}
			urls = append(urls, candidate)
		}
	}
	return urls
}

func buildRoutingTopology(value any) (*RoutingTopology, error) {
	payload, err := nativeMap(value)
	if err != nil {
		return nil, err
	}
	rawRanges, ok := normalizeAdminResponse(payload["ranges"]).([]any)
	if !ok {
		return nil, errors.New("invalid SHARDS topology payload")
	}
	topology := &RoutingTopology{
		RouteEpoch: asInt64(payload["route_epoch"]),
		ShardCount: int(asInt64(payload["shard_count"])),
		endpoints:  make(map[string]RoutingEndpoint),
	}
	for _, rawRange := range rawRanges {
		item, err := nativeMap(rawRange)
		if err != nil {
			return nil, err
		}
		if strings.EqualFold(asString(item["hint"]), "leader_unknown") {
			return nil, errors.New("SHARDS range has no leader")
		}
		first := int(asInt64(item["first_slot"]))
		last := int(asInt64(item["last_slot"]))
		shard := int(asInt64(item["shard"]))
		laneID := uint32(asInt64(item["lane_id"]))
		endpoint, err := endpointFromRange(item)
		if err != nil {
			return nil, err
		}
		if first < 0 || last < first || last >= routeSlotCount || laneID == 0 {
			return nil, fmt.Errorf("invalid SHARDS range: %#v", item)
		}
		key := endpointKey(endpoint)
		route := RoutingRoute{
			Shard:       shard,
			LaneID:      laneID,
			EndpointKey: key,
			Endpoint:    endpoint,
			LeaderNode:  endpoint.Node,
		}
		topology.endpoints[key] = endpoint
		for slot := first; slot <= last; slot++ {
			slotRoute := route
			slotRoute.Slot = slot
			topology.slots[slot] = &slotRoute
		}
	}
	return topology, nil
}

func endpointFromRange(item map[string]any) (RoutingEndpoint, error) {
	raw := item
	if endpointValue, ok := item["endpoint"]; ok && endpointValue != nil {
		endpointMap, err := nativeMap(endpointValue)
		if err != nil {
			return RoutingEndpoint{}, err
		}
		raw = endpointMap
	}
	host := asString(firstPresent(raw, "host", "native_host"))
	nativePort := int(asInt64(firstPresent(raw, "native_port")))
	if host == "" || nativePort <= 0 {
		return RoutingEndpoint{}, fmt.Errorf("invalid SHARDS endpoint: %#v", item)
	}
	node := asString(firstPresent(raw, "node", "leader_node", "owner_node"))
	if node == "" {
		node = host
	}
	return RoutingEndpoint{
		Node:          node,
		Host:          host,
		NativePort:    nativePort,
		NativeTLSPort: int(asInt64(firstPresent(raw, "native_tls_port"))),
	}, nil
}

func firstPresent(mapping map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := mapping[key]; ok {
			return value
		}
	}
	return nil
}

func routingKeyForCommand(args []any) (any, bool) {
	if len(args) == 0 {
		return nil, false
	}
	command, err := buildNativeCommand(args)
	if err != nil {
		return nil, false
	}
	return routingKeyForBuiltCommand(args, command)
}

func routingKeyForBuiltCommand(args []any, command nativeCommand) (any, bool) {
	name := commandName(args)
	if command.opcode < nativeOpCommandExec || name == "CLUSTER.KEYSLOT" || name == "SHARDS" || name == "ROUTE" {
		return nil, false
	}
	if key, ok := routingKeyFromArgs(name, args); ok {
		return key, true
	}
	mapping, ok := command.payload.(map[string]any)
	if !ok {
		return nil, false
	}
	for _, field := range []string{"key", "partition_key", "id", "owner_flow_id", "parent_id", "root_id", "correlation_id", "scope"} {
		value := mapping[field]
		if isRouteKey(value) {
			return value, true
		}
	}
	if keys, ok := mapping["keys"].([]any); ok {
		return singleShardKey(keys)
	}
	if pairs, ok := mapping["pairs"].([]any); ok {
		keys := make([]any, 0, len(pairs))
		for _, pair := range pairs {
			switch p := pair.(type) {
			case []any:
				if len(p) > 0 {
					keys = append(keys, p[0])
				}
			}
		}
		return singleShardKey(keys)
	}
	return nil, false
}

func routingKeyFromArgs(name string, args []any) (any, bool) {
	switch name {
	case "MGET", "DEL":
		return singleShardKey(args[1:])
	case "MSET":
		keys := make([]any, 0, len(args)/2)
		for idx := 1; idx < len(args); idx += 2 {
			keys = append(keys, args[idx])
		}
		return singleShardKey(keys)
	case "BITOP":
		if len(args) < 4 {
			return nil, false
		}
		return singleShardKey(args[2:])
	case "RENAME", "RENAMENX":
		return singleShardKey(args[1:minInt(len(args), 3)])
	case "XREAD", "XREADGROUP":
		return streamReadRoutingKey(args)
	default:
		if strings.HasPrefix(name, "FLOW.") {
			return flowRoutingKey(name, args)
		}
		if firstKeyCommands[name] && len(args) > 1 && isRouteKey(args[1]) {
			return args[1], true
		}
	}
	return nil, false
}

func flowRoutingKey(name string, args []any) (any, bool) {
	flowArgs := args[1:]
	if len(flowArgs) == 0 {
		return nil, false
	}
	if name == "FLOW.CLAIM_DUE" || name == "FLOW.RECLAIM" {
		return flowPartitionRoutingKey(flowArgs, 1)
	}
	switch name {
	case "FLOW.CREATE_MANY", "FLOW.COMPLETE_MANY", "FLOW.TRANSITION_MANY", "FLOW.RETRY_MANY", "FLOW.FAIL_MANY", "FLOW.CANCEL_MANY":
		partition := flowArgs[0]
		if !isRouteKey(partition) {
			return nil, false
		}
		part := strings.ToUpper(asString(partition))
		if part == "AUTO" || part == "MIXED" {
			return nil, false
		}
		return partition, true
	}
	if key, ok := flowPartitionRoutingKey(flowArgs, 1); ok {
		return key, true
	}
	if typeScopedFlowCommands[name] {
		return nil, false
	}
	id := flowArgs[0]
	if isRouteKey(id) {
		return id, true
	}
	return nil, false
}

func flowPartitionRoutingKey(args []any, start int) (any, bool) {
	for idx := start; idx < len(args); idx++ {
		token := commandPart(args[idx])
		switch token {
		case "PARTITION":
			if idx+1 < len(args) && isRouteKey(args[idx+1]) {
				return args[idx+1], true
			}
			return nil, false
		case "PARTITIONS":
			if idx+1 >= len(args) {
				return nil, false
			}
			count := int(asInt64(args[idx+1]))
			if count < 0 || idx+2+count > len(args) {
				return nil, false
			}
			return singleShardKey(args[idx+2 : idx+2+count])
		}
	}
	return nil, false
}

func streamReadRoutingKey(args []any) (any, bool) {
	streamsIndex := -1
	for idx := range args {
		if commandPart(args[idx]) == "STREAMS" {
			streamsIndex = idx
			break
		}
	}
	if streamsIndex < 0 {
		return nil, false
	}
	streamArgs := args[streamsIndex+1:]
	if len(streamArgs) == 0 || len(streamArgs)%2 != 0 {
		return nil, false
	}
	return singleShardKey(streamArgs[:len(streamArgs)/2])
}

func singleShardKey(keys []any) (any, bool) {
	usable := make([]any, 0, len(keys))
	for _, key := range keys {
		if isRouteKey(key) {
			usable = append(usable, key)
		}
	}
	if len(usable) == 0 {
		return nil, false
	}
	slot := routeSlotForKey(usable[0])
	for _, key := range usable[1:] {
		if routeSlotForKey(key) != slot {
			return nil, false
		}
	}
	return usable[0], true
}

func isRouteKey(value any) bool {
	switch value.(type) {
	case string, []byte:
		return true
	default:
		return false
	}
}

func commandName(args []any) string {
	first := commandPart(args[0])
	if first == "" {
		return ""
	}
	if len(args) > 1 && (first == "FLOW" || first == "CLIENT" || first == "CLUSTER") {
		second := commandPart(args[1])
		if second != "" {
			return first + "." + second
		}
	}
	return first
}

func commandPart(value any) string {
	return strings.ToUpper(asString(value))
}

func routeSlotForKey(key any) int {
	text := asString(key)
	var hashInput string
	if strings.HasPrefix(text, "f:{") {
		hashInput = flowHashTag(text[3:], text)
	} else if strings.HasPrefix(text, "X:f:{") {
		hashInput = flowHashTag(text[5:], text)
	} else {
		hashInput = hashTagOrKey(text)
	}
	return int(crc32.ChecksumIEEE([]byte(hashInput))) & routeSlotMask
}

func hashTagOrKey(key string) string {
	start := strings.IndexByte(key, '{')
	if start < 0 {
		return key
	}
	end := strings.IndexByte(key[start+1:], '}')
	if end <= 0 {
		return key
	}
	return key[start+1 : start+1+end]
}

func flowHashTag(rest string, fallbackKey string) string {
	end := strings.IndexByte(rest, '}')
	if end > 0 {
		return rest[:end]
	}
	return hashTagOrKey(fallbackKey)
}

type parsedFerricURL struct {
	Host     string
	Port     int
	RawURL   string
	Scheme   string
	TLS      bool
	Username string
	Password string
}

func parseFerricURL(raw string) (parsedFerricURL, error) {
	if !strings.Contains(raw, "://") {
		raw = "ferric://" + nativeAddressWithPort(raw, nativeDefaultPort)
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return parsedFerricURL{}, err
	}
	scheme := strings.ToLower(parsed.Scheme)
	tlsEnabled := false
	defaultPort := nativeDefaultPort
	switch scheme {
	case "ferric":
	case "ferrics":
		tlsEnabled = true
		defaultPort = nativeDefaultTLSPort
	default:
		return parsedFerricURL{}, fmt.Errorf("unsupported FerricStore URL scheme %q", parsed.Scheme)
	}
	host := parsed.Hostname()
	if host == "" {
		host = "127.0.0.1"
	}
	port := 0
	if parsed.Port() != "" {
		parsedPort, err := strconv.Atoi(parsed.Port())
		if err != nil {
			return parsedFerricURL{}, fmt.Errorf("invalid FerricStore URL port %q", parsed.Port())
		}
		port = parsedPort
	}
	if port <= 0 {
		defaultParsedPort, err := strconv.Atoi(defaultPort)
		if err != nil {
			return parsedFerricURL{}, fmt.Errorf("invalid default FerricStore URL port %q", defaultPort)
		}
		port = defaultParsedPort
	}
	password := ""
	if parsed.User != nil {
		password, _ = parsed.User.Password()
	}
	out := parsedFerricURL{
		Host:     host,
		Port:     port,
		Scheme:   scheme,
		TLS:      tlsEnabled,
		Username: "",
		Password: password,
	}
	if parsed.User != nil {
		out.Username = parsed.User.Username()
	}
	out.RawURL = out.URL()
	return out, nil
}

func (p parsedFerricURL) Endpoint() RoutingEndpoint {
	endpoint := RoutingEndpoint{Node: p.Host, Host: p.Host, NativePort: p.Port}
	if p.TLS {
		endpoint.NativeTLSPort = p.Port
	}
	return endpoint
}

func (p parsedFerricURL) URL() string {
	host := p.Host
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		host = "[" + host + "]"
	}
	user := ""
	if p.Username != "" || p.Password != "" {
		credentials := url.UserPassword(p.Username, p.Password).String()
		user = credentials + "@"
	}
	return fmt.Sprintf("%s://%s%s:%d", p.Scheme, user, host, p.Port)
}

func seedCredentialOptions(urls []string) []NativeOption {
	for _, raw := range urls {
		parsed, err := parseFerricURL(raw)
		if err != nil {
			continue
		}
		if parsed.Password != "" {
			username := parsed.Username
			if username == "" {
				username = "default"
			}
			return []NativeOption{WithNativeCredentials(username, parsed.Password)}
		}
	}
	return nil
}

func endpointKey(endpoint RoutingEndpoint) string {
	return fmt.Sprintf("%s:%d", strings.ToLower(endpoint.Host), endpoint.NativePort)
}

func urlFromEndpoint(endpoint RoutingEndpoint, useTLS bool) string {
	port := endpoint.NativePort
	scheme := "ferric"
	if useTLS {
		scheme = "ferrics"
		if endpoint.NativeTLSPort > 0 {
			port = endpoint.NativeTLSPort
		}
	}
	host := endpoint.Host
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		host = "[" + host + "]"
	}
	return fmt.Sprintf("%s://%s:%d", scheme, host, port)
}

func normalizedStringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func stringSet(values ...string) map[string]struct{} {
	return normalizedStringSet(values)
}

func isRetryableRouteError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, errNativeConnectionUnavailable) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "connection closed") ||
		strings.Contains(message, "shard not available") ||
		strings.Contains(message, "leader") ||
		strings.Contains(message, "route")
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

var firstKeyCommands = map[string]bool{
	"BITCOUNT":                true,
	"BITFIELD":                true,
	"BITPOS":                  true,
	"CAS":                     true,
	"EXISTS":                  true,
	"EXPIRE":                  true,
	"EXPIREAT":                true,
	"FETCH_OR_COMPUTE":        true,
	"FETCH_OR_COMPUTE_ERROR":  true,
	"FETCH_OR_COMPUTE_RESULT": true,
	"FERRICSTORE.KEY_INFO":    true,
	"GET":                     true,
	"GETBIT":                  true,
	"GETDEL":                  true,
	"GETEX":                   true,
	"HDEL":                    true,
	"HEXISTS":                 true,
	"HGET":                    true,
	"HGETALL":                 true,
	"HINCRBY":                 true,
	"HKEYS":                   true,
	"HLEN":                    true,
	"HMGET":                   true,
	"HMSET":                   true,
	"HSET":                    true,
	"HVALS":                   true,
	"LOCK":                    true,
	"LPOP":                    true,
	"LPUSH":                   true,
	"LRANGE":                  true,
	"LREM":                    true,
	"RATELIMIT.ADD":           true,
	"RPOP":                    true,
	"RPUSH":                   true,
	"SADD":                    true,
	"SCARD":                   true,
	"SISMEMBER":               true,
	"SMEMBERS":                true,
	"SREM":                    true,
	"SET":                     true,
	"SETBIT":                  true,
	"STRLEN":                  true,
	"TTL":                     true,
	"TYPE":                    true,
	"UNLINK":                  true,
	"UNLOCK":                  true,
	"XADD":                    true,
	"XLEN":                    true,
	"XRANGE":                  true,
	"ZADD":                    true,
	"ZCARD":                   true,
	"ZRANGE":                  true,
	"ZREM":                    true,
	"ZSCORE":                  true,
}

var typeScopedFlowCommands = map[string]bool{
	"FLOW.APPROVAL.LIST":       true,
	"FLOW.ATTRIBUTE_VALUES":    true,
	"FLOW.ATTRIBUTES":          true,
	"FLOW.BUDGET.LIST":         true,
	"FLOW.FAILURES":            true,
	"FLOW.GOVERNANCE.OVERVIEW": true,
	"FLOW.INFO":                true,
	"FLOW.LIMIT.LIST":          true,
	"FLOW.LIST":                true,
	"FLOW.POLICY.GET":          true,
	"FLOW.POLICY.SET":          true,
	"FLOW.RETENTION_CLEANUP":   true,
	"FLOW.SCHEDULE.FIRE_DUE":   true,
	"FLOW.SCHEDULE.LIST":       true,
	"FLOW.SEARCH":              true,
	"FLOW.STATS":               true,
	"FLOW.STUCK":               true,
	"FLOW.TERMINALS":           true,
}
