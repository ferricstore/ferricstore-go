package ferricstore

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"sync"
)

const (
	nativeDefaultRequestFrameBytes = 16 * 1024 * 1024
	nativeDefaultPipelineCommands  = 1024
	nativeDefaultConnectionCredits = 4096
	nativeDefaultLaneCredits       = 1024
	nativeDefaultLaneQueue         = 1024
	nativeDefaultQueuedRequests    = 65_536
	nativeDefaultProtocolLanes     = 8
)

type nativeFlowController struct {
	mu sync.Mutex

	connectionLimit int
	laneWindowLimit int
	laneQueueLimit  int
	laneLimit       int
	maxQueued       int
	activeTotal     int
	activeByLane    map[uint32]int
	queues          map[uint32]*nativeFlowLaneQueue
	ready           list.List
	queuedTotal     int
	closedErr       error
}

type nativeFlowLaneQueue struct {
	head    *nativeFlowWaiter
	tail    *nativeFlowWaiter
	waiting int
	ready   *list.Element
}

type nativeFlowWaiter struct {
	ready    chan struct{}
	queue    *nativeFlowLaneQueue
	previous *nativeFlowWaiter
	next     *nativeFlowWaiter
	settled  bool
	granted  bool
	err      error
}

var (
	errNativeNoInflightCredit      = errors.New("ferricstore native server advertised no inflight request credit")
	errNativeServerLaneQueueClosed = errors.New("ferricstore native server lane queue is disabled")
	errNativeClientQueueFull       = errors.New("ferricstore native client request queue is full")
)

func newNativeFlowController(connectionLimit, laneLimit, queueLimit int, maxQueuedRequests ...int) *nativeFlowController {
	if connectionLimit < 0 {
		connectionLimit = nativeDefaultConnectionCredits
	}
	if laneLimit < 0 {
		laneLimit = nativeDefaultLaneCredits
	}
	if queueLimit < 0 {
		queueLimit = nativeDefaultLaneQueue
	}
	maxQueued := nativeDefaultQueuedRequests
	if len(maxQueuedRequests) > 0 {
		maxQueued = maxQueuedRequests[0]
		if maxQueued < 0 {
			maxQueued = 0
		}
	}
	return &nativeFlowController{
		connectionLimit: connectionLimit,
		laneWindowLimit: laneLimit,
		laneQueueLimit:  queueLimit,
		laneLimit:       min(laneLimit, queueLimit),
		maxQueued:       maxQueued,
		activeByLane:    make(map[uint32]int),
		queues:          make(map[uint32]*nativeFlowLaneQueue),
	}
}

func (f *nativeFlowController) acquire(ctx context.Context, lane uint32) error {
	if f == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	if f.closedErr != nil {
		err := f.closedErr
		f.mu.Unlock()
		return err
	}
	if f.laneQueueLimit == 0 {
		f.mu.Unlock()
		return errNativeServerLaneQueueClosed
	}
	if f.connectionLimit == 0 || f.laneLimit == 0 {
		f.mu.Unlock()
		return errNativeNoInflightCredit
	}
	if f.canAcquireLocked(lane) {
		f.activateLocked(lane)
		f.mu.Unlock()
		return nil
	}
	if f.queuedTotal >= f.maxQueued {
		f.mu.Unlock()
		return errNativeClientQueueFull
	}

	waiter := &nativeFlowWaiter{ready: make(chan struct{})}
	queue := f.queues[lane]
	if queue == nil {
		queue = &nativeFlowLaneQueue{}
		f.queues[lane] = queue
	}
	waiter.queue = queue
	waiter.previous = queue.tail
	if queue.tail == nil {
		queue.head = waiter
	} else {
		queue.tail.next = waiter
	}
	queue.tail = waiter
	queue.waiting++
	f.queuedTotal++
	if queue.ready == nil {
		queue.ready = f.ready.PushBack(lane)
	}
	f.mu.Unlock()

	select {
	case <-waiter.ready:
		f.mu.Lock()
		err := waiter.err
		granted := waiter.granted
		f.mu.Unlock()
		if err != nil {
			return err
		}
		if !granted {
			return errNativeConnectionUnavailable
		}
		return nil
	case <-ctx.Done():
		f.mu.Lock()
		if !waiter.settled {
			waiter.settled = true
			f.removeQueuedWaiterLocked(lane, waiter)
			f.mu.Unlock()
			return ctx.Err()
		}
		granted := waiter.granted
		f.mu.Unlock()
		if granted {
			f.release(lane)
		}
		return ctx.Err()
	}
}

func (f *nativeFlowController) canAcquireLocked(lane uint32) bool {
	return f.activeTotal < f.connectionLimit && f.activeByLane[lane] < f.laneLimit
}

func (f *nativeFlowController) activateLocked(lane uint32) {
	f.activeTotal++
	f.activeByLane[lane]++
}

func (f *nativeFlowController) release(lane uint32) {
	f.mu.Lock()
	active := f.activeByLane[lane]
	if active <= 0 {
		f.mu.Unlock()
		return
	}
	f.activeTotal--
	if active == 1 {
		delete(f.activeByLane, lane)
	} else {
		f.activeByLane[lane] = active - 1
	}
	if f.closedErr == nil {
		f.pumpLocked()
	}
	f.mu.Unlock()
}

func (f *nativeFlowController) removeQueuedWaiterLocked(lane uint32, waiter *nativeFlowWaiter) {
	queue := waiter.queue
	if queue == nil || queue != f.queues[lane] || queue.waiting <= 0 {
		return
	}
	unlinkNativeFlowWaiter(queue, waiter)
	queue.waiting--
	f.queuedTotal--
	if queue.waiting == 0 {
		f.removeLaneQueueLocked(lane, queue)
	}
}

func (f *nativeFlowController) removeLaneQueueLocked(lane uint32, queue *nativeFlowLaneQueue) {
	if queue.ready != nil {
		f.ready.Remove(queue.ready)
		queue.ready = nil
	}
	delete(f.queues, lane)
}

func (f *nativeFlowController) takeWaiterLocked(queue *nativeFlowLaneQueue) *nativeFlowWaiter {
	for queue.head != nil {
		waiter := queue.head
		unlinkNativeFlowWaiter(queue, waiter)
		if !waiter.settled {
			return waiter
		}
	}
	return nil
}

func unlinkNativeFlowWaiter(queue *nativeFlowLaneQueue, waiter *nativeFlowWaiter) {
	if waiter.previous == nil {
		queue.head = waiter.next
	} else {
		waiter.previous.next = waiter.next
	}
	if waiter.next == nil {
		queue.tail = waiter.previous
	} else {
		waiter.next.previous = waiter.previous
	}
	waiter.queue = nil
	waiter.previous = nil
	waiter.next = nil
}

func (f *nativeFlowController) pumpLocked() {
	for f.closedErr == nil && f.activeTotal < f.connectionLimit && f.ready.Len() > 0 {
		attempts := f.ready.Len()
		granted := false
		for attempts > 0 && f.activeTotal < f.connectionLimit {
			attempts--
			element := f.ready.Front()
			lane := element.Value.(uint32)
			queue := f.queues[lane]
			if queue == nil || queue.ready != element || queue.waiting == 0 {
				f.ready.Remove(element)
				continue
			}
			if f.activeByLane[lane] >= f.laneLimit {
				f.ready.MoveToBack(element)
				continue
			}
			waiter := f.takeWaiterLocked(queue)
			if waiter == nil {
				f.removeLaneQueueLocked(lane, queue)
				continue
			}
			queue.waiting--
			f.queuedTotal--
			if queue.waiting == 0 {
				f.removeLaneQueueLocked(lane, queue)
			} else {
				f.ready.MoveToBack(element)
			}
			f.activateLocked(lane)
			waiter.granted = true
			waiter.settled = true
			close(waiter.ready)
			granted = true
			break
		}
		if !granted {
			return
		}
	}
}

func (f *nativeFlowController) updateLimits(connectionLimit, laneLimit *int) {
	if f == nil {
		return
	}
	f.mu.Lock()
	if connectionLimit != nil && *connectionLimit >= 0 {
		f.connectionLimit = *connectionLimit
	}
	if laneLimit != nil && *laneLimit >= 0 {
		f.laneWindowLimit = *laneLimit
	}
	f.laneLimit = min(f.laneWindowLimit, f.laneQueueLimit)
	if f.closedErr == nil {
		f.pumpLocked()
	}
	f.mu.Unlock()
}

func (f *nativeFlowController) close(err error) {
	if f == nil {
		return
	}
	if err == nil {
		err = errNativeConnectionUnavailable
	}
	f.mu.Lock()
	if f.closedErr != nil {
		f.mu.Unlock()
		return
	}
	f.closedErr = err
	for _, queue := range f.queues {
		for waiter := queue.head; waiter != nil; {
			next := waiter.next
			if waiter.settled {
				waiter = next
				continue
			}
			waiter.settled = true
			waiter.err = err
			close(waiter.ready)
			waiter.queue = nil
			waiter.previous = nil
			waiter.next = nil
			waiter = next
		}
	}
	f.queues = make(map[uint32]*nativeFlowLaneQueue)
	f.ready.Init()
	f.queuedTotal = 0
	f.mu.Unlock()
}

func (e *NativeExecutor) applyStartupCapabilitiesLocked(value any) {
	frameBytes := nativeDefaultRequestFrameBytes
	pipelineCommands := nativeDefaultPipelineCommands
	lanes := uint32(1)
	connectionCredits := nativeDefaultConnectionCredits
	laneCredits := nativeDefaultLaneCredits
	laneQueue := nativeDefaultLaneQueue

	if n, ok := nativeCapabilityInteger(value, "max_frame_bytes", []string{"limits", "payload"}, true); ok {
		frameBytes = boundedNativeFrameBytes(n)
	}
	if n, ok := nativeCapabilityInteger(value, "max_pipeline_commands", []string{"limits", "payload"}, false); ok && n <= math.MaxInt {
		pipelineCommands = int(n)
	}
	if n, ok := nativeCapabilityInteger(value, "max_lanes_per_connection", []string{"limits", "multiplexing", "payload"}, true); ok && n <= math.MaxUint32 {
		lanes = uint32(n)
	}
	configured := e.opts.ProtocolLanes
	if configured == 0 {
		configured = nativeDefaultProtocolLanes
	}
	if lanes > configured {
		lanes = configured
	}
	if lanes == 0 {
		lanes = 1
	}
	if n, ok := nativeCapabilityInteger(value, "max_inflight_per_connection", []string{"flow_control", "limits", "payload"}, false); ok && n <= math.MaxInt {
		connectionCredits = int(n)
	}
	if n, ok := nativeCapabilityInteger(value, "max_inflight_per_lane", []string{"flow_control", "limits", "payload"}, false); ok && n <= math.MaxInt {
		laneCredits = int(n)
	}
	if n, ok := nativeCapabilityInteger(value, "max_lane_queue", []string{"flow_control", "limits", "multiplexing", "payload"}, false); ok && n <= math.MaxInt {
		laneQueue = int(n)
	}

	e.maxRequestFrameBytes = frameBytes
	e.maxPipelineCommands = pipelineCommands
	e.maxDataLanes = lanes
	e.nextLane.Store(0)
	e.flow = newNativeFlowController(connectionCredits, laneCredits, laneQueue, e.opts.MaxQueuedRequests)
}

func boundedNativeFrameBytes(value int64) int {
	if value > int64(nativeMaxFrameBytes) {
		return nativeMaxFrameBytes
	}
	return int(value)
}

func nativeCapabilityInteger(value any, field string, nestedKeys []string, positive bool) (int64, bool) {
	level := []any{value}
	for depth := 0; depth < 3 && len(level) > 0; depth++ {
		next := make([]any, 0)
		for _, candidate := range level {
			mapping, err := nativeMap(candidate)
			if err != nil {
				continue
			}
			if raw, exists := mapping[field]; exists {
				n, err := topologyInteger(raw, field)
				if err == nil && ((!positive && n >= 0) || (positive && n > 0)) {
					return n, true
				}
			}
			for _, key := range nestedKeys {
				if child, exists := mapping[key]; exists && child != nil {
					next = append(next, child)
				}
			}
		}
		level = next
	}
	return 0, false
}

func (e *NativeExecutor) applyFlowControlLimits(expected net.Conn, value any) {
	connectionLimit, laneLimit := nativeFlowControlLimits(value)
	if connectionLimit == nil && laneLimit == nil {
		return
	}
	e.mu.Lock()
	if e.conn != expected {
		e.mu.Unlock()
		return
	}
	flow := e.flow
	e.mu.Unlock()
	flow.updateLimits(connectionLimit, laneLimit)
}

func nativeFlowControlLimits(value any) (*int, *int) {
	var connectionLimit *int
	if n, ok := nativeCapabilityInteger(value, "max_inflight_per_connection", []string{"flow_control", "limits", "payload"}, false); ok && n <= math.MaxInt {
		limit := int(n)
		connectionLimit = &limit
	}
	var laneLimit *int
	if n, ok := nativeCapabilityInteger(value, "max_inflight_per_lane", []string{"flow_control", "limits", "payload"}, false); ok && n <= math.MaxInt {
		limit := int(n)
		laneLimit = &limit
	}
	return connectionLimit, laneLimit
}

func (e *NativeExecutor) nextDataLane() uint32 {
	e.mu.Lock()
	lanes := e.maxDataLanes
	e.mu.Unlock()
	if lanes == 0 {
		lanes = 1
	}
	return (e.nextLane.Add(1)-1)%lanes + 1
}

func (e *NativeExecutor) negotiatedPipelineLimits() (int, int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	frameBytes := e.maxRequestFrameBytes
	if frameBytes <= 0 {
		frameBytes = nativeDefaultRequestFrameBytes
	}
	pipelineCommands := e.maxPipelineCommands
	if pipelineCommands < 0 {
		pipelineCommands = nativeDefaultPipelineCommands
	}
	if e.flow == nil {
		e.flow = newNativeFlowController(nativeDefaultConnectionCredits, nativeDefaultLaneCredits, nativeDefaultLaneQueue, e.opts.MaxQueuedRequests)
	}
	return frameBytes, pipelineCommands
}

func (e *NativeExecutor) acquireFlowCredit(ctx context.Context, expected net.Conn, lane uint32) (*nativeFlowController, error) {
	e.mu.Lock()
	if e.conn != expected {
		e.mu.Unlock()
		return nil, fmt.Errorf("%w: connection changed", errNativeConnectionUnavailable)
	}
	flow := e.flow
	if flow == nil {
		flow = newNativeFlowController(nativeDefaultConnectionCredits, nativeDefaultLaneCredits, nativeDefaultLaneQueue, e.opts.MaxQueuedRequests)
		e.flow = flow
	}
	e.mu.Unlock()
	if err := flow.acquire(ctx, lane); err != nil {
		return nil, err
	}
	return flow, nil
}
