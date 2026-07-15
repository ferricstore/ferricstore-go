package ferricstore

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	nativeMagic                     = "FSNP"
	nativeRequestVersion            = 0x01
	nativeResponseVersion           = 0x81
	nativeHeaderLen                 = 24
	nativeDefaultPort               = "6388"
	nativeDefaultTLSPort            = "6389"
	nativeMaxFrameBytes             = 128 * 1024 * 1024
	nativeMaxContainerItems         = 1_000_000
	nativeAutoLaneID                = math.MaxUint32
	nativeDefaultGoAwayDrainTimeout = 30 * time.Second
	nativeEventBufferCapacity       = 4096
	nativeMaxBufferedEventBytes     = 16 * 1024 * 1024

	nativeFlagCompressed    = 0x08
	nativeFlagCustomPayload = 0x02
	nativeFlagMoreChunks    = 0x20

	nativeStatusOK = 0

	nativeOpAuth                   = 0x0002
	nativeOpPing                   = 0x0003
	nativeOpGoAway                 = 0x000A
	nativeOpStartup                = 0x000C
	nativeOpWindowUpdate           = 0x000D
	nativeOpPipeline               = 0x000E
	nativeOpEvent                  = 0x0010
	nativeOpSubscribeEvents        = 0x0011
	nativeOpUnsubscribeEvents      = 0x0012
	nativeOpCommandExec            = 0x0100
	nativeOpGet                    = 0x0101
	nativeOpSet                    = 0x0102
	nativeOpDel                    = 0x0103
	nativeOpMGet                   = 0x0104
	nativeOpMSet                   = 0x0105
	nativeOpFlowClaimDue           = 0x0203
	nativeOpFlowCreateMany         = 0x020F
	nativeOpFlowCompleteMany       = 0x0210
	nativeOpFlowPolicySet          = 0x021E
	nativeOpFlowPolicyGet          = 0x021F
	nativeOpFlowStepContinue       = 0x0222
	nativeOpFlowStartAndClaim      = 0x0223
	nativeOpFlowRunStepsMany       = 0x0224
	nativeOpFlowScheduleCreate     = 0x0225
	nativeOpFlowScheduleGet        = 0x0226
	nativeOpFlowScheduleDelete     = 0x0227
	nativeOpFlowScheduleFireDue    = 0x0228
	nativeOpFlowScheduleList       = 0x0229
	nativeOpFlowScheduleFire       = 0x022A
	nativeOpFlowSchedulePause      = 0x022B
	nativeOpFlowScheduleResume     = 0x022C
	nativeOpFlowEffectReserve      = 0x0240
	nativeOpFlowEffectConfirm      = 0x0241
	nativeOpFlowEffectFail         = 0x0242
	nativeOpFlowEffectCompensate   = 0x0243
	nativeOpFlowEffectGet          = 0x0244
	nativeOpFlowGovernanceLedger   = 0x0245
	nativeOpFlowApprovalRequest    = 0x0246
	nativeOpFlowApprovalApprove    = 0x0247
	nativeOpFlowApprovalReject     = 0x0248
	nativeOpFlowApprovalGet        = 0x0249
	nativeOpFlowCircuitOpen        = 0x024A
	nativeOpFlowCircuitClose       = 0x024B
	nativeOpFlowCircuitGet         = 0x024C
	nativeOpFlowBudgetReserve      = 0x024D
	nativeOpFlowBudgetGet          = 0x024E
	nativeOpFlowLimitLease         = 0x024F
	nativeOpFlowLimitSpend         = 0x0250
	nativeOpFlowLimitRelease       = 0x0251
	nativeOpFlowLimitGet           = 0x0252
	nativeOpFlowApprovalList       = 0x0253
	nativeOpFlowGovernanceOverview = 0x0254
	nativeOpFlowBudgetList         = 0x0255
	nativeOpFlowLimitList          = 0x0256
	nativeOpFlowBudgetCommit       = 0x0257
	nativeOpFlowBudgetRelease      = 0x0258

	nativeCompactFlowClaimJobs    = 0x80
	nativeCompactOKList           = 0x81
	nativeCompactKVGet            = 0x82
	nativeCompactKVMGet           = 0x83
	nativeCompactKVMGetFixed      = 0x89
	nativeCompactPipelineRequest  = 0x94
	nativeCompactPipelineResponse = 0x95
)

type nativeCompactOKCount int

type NativeOptions struct {
	Addr                string
	Username            string
	Password            string
	TLS                 bool
	TLSConfig           *tls.Config
	ClientName          string
	Timeout             time.Duration
	Dialer              *net.Dialer
	HeartbeatInterval   time.Duration
	HeartbeatTimeout    time.Duration
	GoAwayDrainTimeout  time.Duration
	ReconnectMaxRetries int
	ProtocolLanes       uint32
	MaxQueuedRequests   int
	startupEvents       []string
	eventHandler        func(nativeServerEvent)
	addressInput        string
	addressUsesDefault  bool
}

type NativeOption func(*NativeOptions)

func WithNativeCredentials(username, password string) NativeOption {
	return func(opts *NativeOptions) {
		opts.Username = username
		opts.Password = password
	}
}

func WithNativeTLS(config *tls.Config) NativeOption {
	return func(opts *NativeOptions) {
		opts.TLS = true
		opts.TLSConfig = config
	}
}

func WithNativeTimeout(timeout time.Duration) NativeOption {
	return func(opts *NativeOptions) {
		if timeout < 0 {
			timeout = 0
		}
		opts.Timeout = timeout
		if opts.Dialer == nil {
			opts.Dialer = &net.Dialer{}
		}
		opts.Dialer.Timeout = timeout
	}
}

func WithNativeClientName(name string) NativeOption {
	return func(opts *NativeOptions) {
		opts.ClientName = name
	}
}

func WithNativeHeartbeat(interval, timeout time.Duration) NativeOption {
	return func(opts *NativeOptions) {
		opts.HeartbeatInterval = interval
		opts.HeartbeatTimeout = timeout
	}
}

func WithNativeReconnect(maxRetries int) NativeOption {
	return func(opts *NativeOptions) {
		if maxRetries < 0 {
			maxRetries = 0
		}
		opts.ReconnectMaxRetries = maxRetries
	}
}

// WithNativeGoAwayDrainTimeout bounds how long an old connection may retain
// live requests after it stops admitting new work. This is especially
// important when another request on the connection blocks indefinitely.
func WithNativeGoAwayDrainTimeout(timeout time.Duration) NativeOption {
	return func(opts *NativeOptions) {
		if timeout <= 0 {
			timeout = nativeDefaultGoAwayDrainTimeout
		}
		opts.GoAwayDrainTimeout = timeout
	}
}

// WithNativeLanes caps automatic data-lane use. The server-advertised STARTUP
// limit may reduce this value further.
func WithNativeLanes(lanes uint32) NativeOption {
	return func(opts *NativeOptions) {
		if lanes == 0 {
			lanes = 1
		}
		opts.ProtocolLanes = lanes
	}
}

// WithNativeMaxQueuedRequests bounds requests waiting for server-advertised
// native flow-control credits. A zero limit rejects instead of queueing.
func WithNativeMaxQueuedRequests(limit int) NativeOption {
	return func(opts *NativeOptions) {
		if limit < 0 {
			limit = 0
		}
		opts.MaxQueuedRequests = limit
	}
}

func withNativeEventSubscription(events []string, handler func(nativeServerEvent)) NativeOption {
	return func(opts *NativeOptions) {
		opts.startupEvents = append([]string(nil), events...)
		opts.eventHandler = handler
	}
}

type NativeExecutor struct {
	opts NativeOptions

	sessionGate          sessionGate
	mu                   sync.Mutex
	writeEncodeMu        contextMutex
	writeMu              contextMutex
	conn                 net.Conn
	reader               *bufio.Reader
	writer               *bufio.Writer
	nextID               uint64
	nextLane             atomic.Uint32
	maxRequestFrameBytes int
	maxPipelineCommands  int
	maxDataLanes         uint32
	flow                 *nativeFlowController
	replayWindowUpdate   map[string]any
	connectInFlight      *nativeConnectAttempt
	connectionDone       chan struct{}
	connectionGeneration uint64
	goAway               bool
	goAwayDone           chan struct{}
	isClosed             bool
	retiring             bool
	activeRequests       int
	closed               chan struct{}
	heartbeatStop        chan struct{}
	lastActivityUnixNano atomic.Int64
	pending              map[uint64]*nativePendingRequest
	events               chan nativeQueuedEvent
	eventDeliveryEnabled bool
	eventBufferedBytes   int
	droppedEvents        atomic.Uint64
}

type nativePendingRequest struct {
	responseCh chan nativeResponse
	opcode     uint16
	laneID     uint32
	flowCredit *nativeFlowController
	abandoned  bool
}

type nativeConnectAttempt struct {
	done    chan struct{}
	cancel  context.CancelFunc
	err     error
	waiters int
}

type nativeConnectedTransport struct {
	conn            net.Conn
	reader          *bufio.Reader
	writer          *bufio.Writer
	startupResponse any
	windowResponse  any
}

type NativeError struct {
	Status uint16
	Kind   string
	Value  any
}

var (
	errNativeConnectionUnavailable = errors.New("ferricstore native connection unavailable")
	errNativeGoAway                = errors.New("ferricstore native connection is draining after GOAWAY")
)

func (e NativeError) Error() string {
	if message := nativeErrorMessage(e.Value); message != "" {
		return message
	}
	if e.Kind != "" {
		return fmt.Sprintf("ferricstore native %s status %d", e.Kind, e.Status)
	}
	return fmt.Sprintf("ferricstore native error status %d", e.Status)
}

func NewNativeExecutor(addr string, opts ...NativeOption) *NativeExecutor {
	options := defaultNativeOptions(addr, false)
	applyNativeOptions(&options, opts...)
	return newNativeExecutor(options)
}

func NewNativeExecutorFromURL(rawurl string, opts ...NativeOption) (*NativeExecutor, error) {
	parsed, err := parseFerricURL(rawurl)
	if err != nil {
		return nil, err
	}

	address := parsed.Host
	if parsed.ExplicitPort {
		address = parsed.Address()
	}
	options := defaultNativeOptions(address, parsed.TLS)
	options.Username = parsed.Username
	options.Password = parsed.Password
	if parsed.HasTimeout {
		options.Timeout = parsed.Timeout
		options.Dialer.Timeout = parsed.Timeout
	}
	applyNativeOptions(&options, opts...)
	return newNativeExecutor(options), nil
}

func newNativeExecutor(options NativeOptions) *NativeExecutor {
	return &NativeExecutor{
		opts:                 options,
		closed:               make(chan struct{}),
		maxRequestFrameBytes: nativeDefaultRequestFrameBytes,
		maxPipelineCommands:  nativeDefaultPipelineCommands,
		maxDataLanes:         1,
		flow:                 newNativeFlowController(nativeDefaultConnectionCredits, nativeDefaultLaneCredits, nativeDefaultLaneQueue, options.MaxQueuedRequests),
		eventDeliveryEnabled: options.eventHandler == nil,
	}
}

func (e *NativeExecutor) Do(ctx context.Context, args ...any) (any, error) {
	return e.command(ctx, args...)
}

func (e *NativeExecutor) Close() error {
	e.mu.Lock()
	if e.isClosed {
		e.mu.Unlock()
		return nil
	}
	e.markClosedLocked()
	pending := e.takePendingLocked()
	err := e.closeConnLocked()
	e.mu.Unlock()
	failNativePending(pending, net.ErrClosed)
	return err
}

func (e *NativeExecutor) DroppedEvents() uint64 {
	if e == nil {
		return 0
	}
	return e.droppedEvents.Load()
}

func (e *NativeExecutor) command(ctx context.Context, args ...any) (any, error) {
	if name, stateful := connectionStateCommand(args); stateful {
		return nil, fmt.Errorf("%s requires a connection-affine Client transaction helper", name)
	}
	if name, mutates := connectionStateMutationCommand(args); mutates && name != "CLIENT.SETNAME" && name != "WINDOW_UPDATE" {
		return nil, fmt.Errorf("%s is connection-local; configure it with NativeOptions or a dedicated helper", name)
	}
	command, err := buildNativeCommand(args)
	if err != nil {
		return nil, err
	}
	command.budget = blockingCommandBudget(args)
	if command.laneID != 0 {
		command.laneID = nativeAutoLaneID
	}
	value, err := e.doNativeCommand(ctx, command)
	if err == nil {
		e.rememberConnectionState(args, command)
	}
	return value, err
}

func (e *NativeExecutor) rememberConnectionState(args []any, command nativeCommand) {
	if len(args) == 0 {
		return
	}
	name := strings.ToUpper(asString(args[0]))
	if name == "COMMAND_EXEC" && len(args) > 1 {
		name = strings.ToUpper(asString(args[1]))
		args = args[1:]
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	switch {
	case name == "CLIENT" && len(args) > 2 && strings.EqualFold(asString(args[1]), "SETNAME"):
		e.opts.ClientName = asString(args[2])
	case name == "WINDOW_UPDATE":
		payload, ok := command.payload.(map[string]any)
		if !ok {
			return
		}
		e.replayWindowUpdate = make(map[string]any, len(payload))
		for key, value := range payload {
			e.replayWindowUpdate[key] = value
		}
	}
}

func (e *NativeExecutor) doNativeCommand(ctx context.Context, command nativeCommand) (any, error) {
	value, err := e.requestWithBudget(ctx, command.opcode, command.laneID, command.payload, command.flags, command.budget)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", command.name, err)
	}
	return value, nil
}

func (e *NativeExecutor) doNativeCommandOnLane(ctx context.Context, command nativeCommand, laneID uint32) (any, error) {
	command.laneID = laneID
	return e.doNativeCommand(ctx, command)
}

func (e *NativeExecutor) Pipeline(ctx context.Context, commands [][]any) ([]any, error) {
	return e.pipelineOnLane(ctx, commands, nativeAutoLaneID)
}

func (e *NativeExecutor) pipelineOnLane(ctx context.Context, commands [][]any, laneID uint32) ([]any, error) {
	results, err := e.pipelineDetailedOnLane(ctx, commands, laneID)
	if err != nil {
		return nil, err
	}
	return pipelineResultValues(results)
}

func (e *NativeExecutor) pipelineDetailed(ctx context.Context, commands [][]any) ([]pipelineItemResult, error) {
	return e.pipelineDetailedOnLane(ctx, commands, nativeAutoLaneID)
}

func (e *NativeExecutor) pipelineDetailedOnLane(ctx context.Context, commands [][]any, laneID uint32) ([]pipelineItemResult, error) {
	if len(commands) == 0 {
		return nil, nil
	}
	for _, command := range commands {
		if name, mutates := connectionStateMutationCommand(command); mutates {
			return nil, fmt.Errorf("%s is connection-local and cannot be used in a pipeline", name)
		}
	}
	ctx, cancel := nativeContextWithBudget(ctx, e.opts.Timeout, pipelineBlockingBudget(commands))
	if cancel != nil {
		defer cancel()
	}
	if err := e.sessionGate.readLock(ctx); err != nil {
		return nil, err
	}
	defer e.sessionGate.readUnlock()
	if err := e.ensureConnectedLocked(ctx); err != nil {
		return nil, err
	}
	if laneID == nativeAutoLaneID {
		laneID = e.nextDataLane()
	}
	maxFrameBytes, maxCommands := e.negotiatedPipelineLimits()
	if maxCommands == 0 {
		return e.pipelineSequentialWithoutGate(ctx, commands, laneID)
	}
	results := make([]pipelineItemResult, len(commands))
	for start := 0; start < len(commands); {
		end := start + maxCommands
		if end > len(commands) {
			end = len(commands)
		}
		chunkResults, err := e.pipelineChunkWithoutGate(ctx, commands[start:end], laneID, maxFrameBytes)
		if err != nil {
			var buildErr *pipelineCommandBuildError
			if errors.As(err, &buildErr) {
				if buildErr.index < 0 || buildErr.index >= end-start {
					return nil, fmt.Errorf("PIPELINE reported invalid build-error index %d for a %d-command chunk", buildErr.index, end-start)
				}
				failureIndex := start + buildErr.index
				if failureIndex > start {
					prefixResults, prefixErr := e.pipelineChunkWithoutGate(ctx, commands[start:failureIndex], laneID, maxFrameBytes)
					if prefixErr != nil {
						if len(prefixResults) > failureIndex-start {
							return nil, fmt.Errorf("PIPELINE returned %d partial results for a %d-command prefix", len(prefixResults), failureIndex-start)
						}
						copy(results[start:start+len(prefixResults)], prefixResults)
						failureStart := start + len(prefixResults)
						failureEnd := min(failureIndex, failureStart+pipelineChunkAffectedCommands(prefixErr, failureIndex-failureStart))
						for index := failureStart; index < failureEnd; index++ {
							results[index].err = prefixErr
						}
						markPipelineNotExecuted(results[failureEnd:], prefixErr)
						return results, nil
					}
					copy(results[start:failureIndex], prefixResults)
				}
				results[failureIndex].err = buildErr.cause
				start = failureIndex + 1
				continue
			}
			if len(chunkResults) > end-start {
				return nil, fmt.Errorf("PIPELINE returned %d partial results for a %d-command chunk", len(chunkResults), end-start)
			}
			copy(results[start:start+len(chunkResults)], chunkResults)
			failureStart := start + len(chunkResults)
			failureEnd := min(end, failureStart+pipelineChunkAffectedCommands(err, end-failureStart))
			for index := failureStart; index < failureEnd; index++ {
				results[index].err = err
			}
			markPipelineNotExecuted(results[failureEnd:], err)
			return results, nil
		}
		copy(results[start:end], chunkResults)
		start = end
	}
	return results, nil
}

func (e *NativeExecutor) pipelineChunkWithoutGate(ctx context.Context, commands [][]any, laneID uint32, maxFrameBytes int) ([]pipelineItemResult, error) {
	payload, flags, err := nativePipelinePayload(commands, laneID, maxFrameBytes)
	if err != nil {
		var limitErr nativeEncodeLimitError
		if !errors.As(err, &limitErr) {
			return nil, err
		}
		return e.splitOversizedPipelineChunk(ctx, commands, laneID, maxFrameBytes)
	}
	budget := pipelineBlockingBudget(commands)
	value, err := e.requestWithoutSessionGate(ctx, nativeOpPipeline, laneID, payload, flags, budget)
	if err != nil {
		var limitErr nativeEncodeLimitError
		if errors.As(err, &limitErr) {
			return e.splitOversizedPipelineChunk(ctx, commands, laneID, maxFrameBytes)
		}
		return nil, &pipelineChunkExecutionError{
			cause:    fmt.Errorf("PIPELINE: %w", err),
			affected: len(commands),
		}
	}
	return pipelineItemResults(value, len(commands))
}

func (e *NativeExecutor) splitOversizedPipelineChunk(ctx context.Context, commands [][]any, laneID uint32, maxFrameBytes int) ([]pipelineItemResult, error) {
	if len(commands) == 1 {
		return nil, &pipelineChunkExecutionError{
			cause:    fmt.Errorf("PIPELINE command exceeds server-advertised %d-byte frame limit", maxFrameBytes),
			affected: 1,
		}
	}
	middle := len(commands) / 2
	left, err := e.pipelineChunkWithoutGate(ctx, commands[:middle], laneID, maxFrameBytes)
	if err != nil {
		return left, err
	}
	right, err := e.pipelineChunkWithoutGate(ctx, commands[middle:], laneID, maxFrameBytes)
	if err != nil {
		return append(left, right...), err
	}
	return append(left, right...), nil
}

func nativePipelinePayload(commands [][]any, laneID uint32, maxFrameBytes int) (any, byte, error) {
	if payload, ok, err := compactPipelinePlanWithLimit(commands, maxFrameBytes); ok || err != nil {
		return payload, nativeFlagCustomPayload, err
	}
	items := make([]any, 0, len(commands))
	for idx, args := range commands {
		command, err := buildNativeCommand(args)
		if err != nil {
			return nil, 0, &pipelineCommandBuildError{index: idx, cause: err}
		}
		if command.flags != 0 {
			// Typed PIPELINE items require map bodies. Rebuild compact/custom
			// commands through COMMAND_EXEC, matching an ordinary command's wire
			// semantics without embedding an opaque body the server cannot parse.
			command, err = commandExecNativeCommand(strings.ToUpper(asString(args[0])), args[1:])
			if err != nil {
				return nil, 0, &pipelineCommandBuildError{index: idx, cause: err}
			}
		}
		items = append(items, map[string]any{
			"opcode":     int64(command.opcode),
			"lane_id":    int64(laneID),
			"request_id": int64(idx + 1),
			"body":       command.payload,
		})
	}
	return map[string]any{
		"atomicity": "none",
		"commands":  items,
		"return":    "compact",
	}, 0, nil
}

func pipelineBlockingBudget(commands [][]any) nativeRequestBudget {
	var out nativeRequestBudget
	for _, args := range commands {
		budget := blockingCommandBudget(args)
		if budget.disableDefault {
			return budget
		}
		if budget.extension > time.Duration(1<<63-1)-out.extension {
			return nativeRequestBudget{disableDefault: true}
		}
		out.extension += budget.extension
	}
	return out
}

func (e *NativeExecutor) pipelineSequentialWithoutGate(ctx context.Context, commands [][]any, laneID uint32) ([]pipelineItemResult, error) {
	results := make([]pipelineItemResult, len(commands))
	for index, args := range commands {
		command, err := buildNativeCommand(args)
		if err != nil {
			results[index].err = err
			continue
		}
		command.laneID = laneID
		command.budget = blockingCommandBudget(args)
		value, err := e.requestWithoutSessionGate(ctx, command.opcode, command.laneID, command.payload, command.flags, command.budget)
		results[index] = pipelineItemResult{value: value, err: err}
		if ctx != nil && ctx.Err() != nil {
			markPipelineNotExecuted(results[index+1:], ctx.Err())
			return results, nil
		}
	}
	return results, nil
}
