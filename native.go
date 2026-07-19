package ferricstore

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math"
	"net"
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
	nativeUnauthenticatedFrameBytes = 64 * 1024
	nativeMaxContainerItems         = 1_000_000
	nativeAutoLaneID                = math.MaxUint32
	nativeDefaultGoAwayDrainTimeout = 30 * time.Second
	nativeEventBufferCapacity       = 4096
	nativeMaxBufferedEventBytes     = 16 * 1024 * 1024

	nativeFlagTrace         = 0x01
	nativeFlagCustomPayload = 0x02
	nativeFlagWarning       = 0x04
	nativeFlagCompressed    = 0x08
	nativeFlagNoReply       = 0x10
	nativeFlagMoreChunks    = 0x20

	nativeResponseWireFlags = nativeFlagTrace | nativeFlagCustomPayload |
		nativeFlagWarning | nativeFlagCompressed | nativeFlagMoreChunks
	nativeStableChunkFlags = nativeFlagTrace | nativeFlagCustomPayload | nativeFlagWarning

	nativeStatusOK = 0

	nativeOpHello             = 0x0001
	nativeOpAuth              = 0x0002
	nativeOpPing              = 0x0003
	nativeOpShards            = 0x0007
	nativeOpGoAway            = 0x000A
	nativeOpStartup           = 0x000C
	nativeOpWindowUpdate      = 0x000D
	nativeOpPipeline          = 0x000E
	nativeOpEvent             = 0x0010
	nativeOpSubscribeEvents   = 0x0011
	nativeOpUnsubscribeEvents = 0x0012
	nativeOpCommandExec       = 0x0100
	nativeOpGet               = 0x0101
	nativeOpSet               = 0x0102
	nativeOpDel               = 0x0103
	nativeOpMGet              = 0x0104
	nativeOpMSet              = 0x0105

	nativeCompactFlowClaimJobs    = 0x80
	nativeCompactOKList           = 0x81
	nativeCompactKVGet            = 0x82
	nativeCompactKVMGet           = 0x83
	nativeCompactFlowRecord       = 0x84
	nativeCompactFlowRecordList   = 0x85
	nativeCompactBinaryListList   = 0x86
	nativeCompactBinaryMapList    = 0x87
	nativeCompactIntegerList      = 0x88
	nativeCompactKVMGetFixed      = 0x89
	nativeCompactPipelineRequest  = 0x94
	nativeCompactPipelineResponse = 0x95
)

type nativeCompactOKCount int

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
	maxResponseBytes     int
	maxPipelineCommands  int
	maxDataLanes         uint32
	responseCodecs       nativeResponseCodecs
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
	conn           net.Conn
	reader         *bufio.Reader
	writer         *bufio.Writer
	helloResponse  any
	windowResponse any
	contract       nativeHelloContract
}

type NativeError struct {
	Status uint16
	Kind   string
	Value  any

	// Keep NativeError non-comparable: Value can hold maps and slices, and a
	// nominally comparable error value can otherwise make errors.Is panic.
	_ [0]func()
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
	options.credentialsSet = parsed.CredentialsSet
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
		maxResponseBytes:     options.MaxResponseBytes,
		maxPipelineCommands:  nativeDefaultPipelineCommands,
		maxDataLanes:         1,
		flow:                 newNativeFlowController(nativeDefaultConnectionCredits, nativeDefaultLaneCredits, nativeDefaultLaneQueue, options.MaxQueuedRequests),
		eventDeliveryEnabled: nativeConfiguredEventHandler(options.eventSubscription) == nil,
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
	name := commandPart(args[0])
	if name == "COMMAND_EXEC" && len(args) > 1 {
		name = commandPart(args[1])
		args = args[1:]
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	switch {
	case name == "CLIENT" && len(args) > 2 && commandPart(args[1]) == "SETNAME":
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
	value, err := e.requestWithReplayPolicy(
		ctx, command.opcode, command.laneID, command.payload, command.flags,
		command.budget, command.replayPolicy,
	)
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

func nativePipelinePayload(commands [][]any, laneID uint32, maxFrameBytes int) (any, byte, error) {
	payload, flags, _, err := nativePipelinePayloadWithExecutionPolicy(commands, laneID, maxFrameBytes)
	return payload, flags, err
}

func nativePipelinePayloadWithReplayPolicy(
	commands [][]any,
	laneID uint32,
	maxFrameBytes int,
) (any, byte, nativeReplayPolicy, error) {
	payload, flags, policy, err := nativePipelinePayloadWithExecutionPolicy(
		commands, laneID, maxFrameBytes,
	)
	return payload, flags, policy.replayPolicy, err
}

func nativePipelinePayloadWithExecutionPolicy(
	commands [][]any,
	laneID uint32,
	maxFrameBytes int,
) (any, byte, nativeCommandExecutionPolicy, error) {
	if payload, ok, err := compactPipelinePlanWithLimit(commands, maxFrameBytes); ok || err != nil {
		return payload, nativeFlagCustomPayload, nativeCommandExecutionPolicy{}, err
	}
	items := make([]any, 0, len(commands))
	var policy nativeCommandExecutionPolicy
	for idx, args := range commands {
		command, err := buildNativeCommand(args)
		if err != nil {
			return nil, 0, nativeCommandExecutionPolicy{}, &pipelineCommandBuildError{index: idx, cause: err}
		}
		policy.add(command)
		if command.flags != 0 {
			if provider, ok := command.payload.(nativePipelineBodyProvider); ok {
				command.payload, err = provider.nativePipelineBody()
				command.flags = 0
			} else {
				// Typed PIPELINE items require map bodies. Rebuild other compact/custom
				// commands through COMMAND_EXEC because an opaque body cannot be nested.
				command, err = commandExecNativeCommand(commandPart(args[0]), args[1:])
			}
			if err != nil {
				return nil, 0, nativeCommandExecutionPolicy{}, &pipelineCommandBuildError{index: idx, cause: err}
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
	}, 0, policy, nil
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
		value, err := e.requestWithoutSessionGateWithReplayPolicy(
			ctx, command.opcode, command.laneID, command.payload, command.flags,
			command.budget, command.replayPolicy,
		)
		results[index] = pipelineItemResult{value: value, err: err}
		if ctx != nil && ctx.Err() != nil {
			markPipelineNotExecuted(results[index+1:], ctx.Err())
			return results, nil
		}
	}
	return results, nil
}
