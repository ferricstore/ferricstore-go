package ferricstore

import (
	"context"
	"errors"
	"fmt"
)

func (e *AutoBatchExecutor) submitWithQueuePolicy(
	ctx context.Context,
	args []any,
	allowQueued bool,
) (<-chan autoBatchResult, error) {
	if e == nil || e.client == nil {
		return nil, errors.New("ferricstore autobatch executor requires a client")
	}
	if e.isClosed.Load() {
		return nil, errAutoBatchClosed
	}
	if err := e.reserveQueueSlot(ctx); err != nil {
		return nil, err
	}
	preparedArgs, err := materializeDeferredCodecValuesForExecutor(
		args,
		e.client.exec,
		&e.codecMu,
	)
	if err != nil {
		e.queueSlots <- struct{}{}
		return nil, fmt.Errorf("encode autobatch command: %w", err)
	}
	copiedArgs, err := snapshotDeferredCodecInputs(preparedArgs)
	if err != nil {
		e.queueSlots <- struct{}{}
		return nil, fmt.Errorf("snapshot autobatch command: %w", err)
	}
	request := autoBatchRequest{
		ctx:    ctx,
		args:   copiedArgs,
		result: make(chan autoBatchResult, 1),
	}
	if !allowQueued {
		request.control = autoBatchDisallowQueuedControl
	}
	if err := e.sendReserved(request); err != nil {
		return nil, err
	}
	return request.result, nil
}

// Blocking commands must not enter the shared autobatch pipeline. A server-side
// wait would otherwise consume the sole pipeline slot, delay unrelated work,
// and inherit FlushTimeout instead of the command's own blocking budget.
func (e *AutoBatchExecutor) executeBlockingCommandDirect(
	ctx context.Context,
	allowQueued bool,
	args []any,
) (any, bool, error) {
	if e == nil || e.client == nil {
		return nil, false, errors.New("ferricstore autobatch executor requires a client")
	}
	if e.isClosed.Load() {
		return nil, false, errAutoBatchClosed
	}
	prepared, err := materializeDeferredCodecValuesForExecutor(
		args,
		e.client.exec,
		&e.codecMu,
	)
	if err != nil {
		return nil, false, fmt.Errorf("encode autobatch blocking command: %w", err)
	}
	if e.isClosed.Load() {
		return nil, false, errAutoBatchClosed
	}
	return e.client.commandWithoutLegacyWithQueuePolicy(ctx, allowQueued, prepared...)
}

func (e *AutoBatchExecutor) reserveQueueSlot(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.isClosed.Load() {
		return errAutoBatchClosed
	}
	select {
	case <-e.queueSlots:
		if err := ctx.Err(); err != nil {
			e.queueSlots <- struct{}{}
			return err
		}
		if e.isClosed.Load() {
			e.queueSlots <- struct{}{}
			return errAutoBatchClosed
		}
		return nil
	case <-e.closed:
		return errAutoBatchClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *AutoBatchExecutor) sendReserved(request autoBatchRequest) error {
	e.submitMu.RLock()
	if e.isClosed.Load() {
		e.submitMu.RUnlock()
		e.queueSlots <- struct{}{}
		return errAutoBatchClosed
	}
	// The admission token reserves capacity, so this send cannot block while
	// holding the close-coordination lock.
	e.requests <- request
	e.submitMu.RUnlock()
	return nil
}

func (e *AutoBatchExecutor) flushPending(ctx context.Context) error {
	if err := e.reserveQueueSlot(ctx); err != nil {
		return err
	}
	flushed := make(chan autoBatchResult, 1)
	if err := e.sendReserved(autoBatchRequest{
		result: flushed, control: autoBatchFlushControl,
	}); err != nil {
		return err
	}
	select {
	case result := <-flushed:
		return result.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *AutoBatchExecutor) Pipeline(ctx context.Context, commands [][]any) ([]any, error) {
	results, err := e.pipelineDetailed(ctx, commands)
	if err != nil {
		return nil, err
	}
	return pipelineResultValues(results)
}

func (e *AutoBatchExecutor) pipelineDetailed(ctx context.Context, commands [][]any) ([]pipelineItemResult, error) {
	if e == nil || e.client == nil {
		return nil, errors.New("ferricstore autobatch executor requires a client")
	}
	if len(commands) == 0 {
		return nil, nil
	}
	if err := validatePipelineCommands(commands); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if pipelineContainsBlockingCommand(commands) {
		return e.executeBlockingPipelineDirect(ctx, commands)
	}
	results := make([]pipelineItemResult, len(commands))
	if err := e.reserveQueueSlot(ctx); err != nil {
		results[0].err = err
		markPipelineNotExecuted(results[1:], err)
		return results, nil
	}
	ownedCommands, accepted, snapshotErr := snapshotDeferredCodecCommands(commands, &e.codecMu)
	if snapshotErr != nil {
		snapshotErr = fmt.Errorf("snapshot autobatch command: %w", snapshotErr)
		results[accepted].err = snapshotErr
		markPipelineNotExecuted(results[accepted+1:], snapshotErr)
	}
	if accepted == 0 {
		e.queueSlots <- struct{}{}
		return results, nil
	}
	response := make(chan autoBatchResult, 1)
	request := autoBatchRequest{
		ctx:    ctx,
		result: response,
		control: &autoBatchRequestControl{
			pipelineCommands: ownedCommands[:accepted],
			allowQueued:      true,
		},
	}
	if err := e.sendReserved(request); err != nil {
		results[0].err = err
		markPipelineNotExecuted(results[1:accepted], err)
		return results, nil
	}
	_ = e.flushPending(ctx)
	select {
	case item := <-response:
		if item.err != nil {
			for index := range results[:accepted] {
				results[index].err = item.err
			}
			return results, nil
		}
		grouped, ok := item.value.(autoBatchExplicitPipelineResult)
		if !ok || len(grouped.items) != accepted {
			err := fmt.Errorf(
				"ferricstore autobatch returned %T with %d results for an explicit %d-command pipeline",
				item.value, len(grouped.items), accepted,
			)
			for index := range results[:accepted] {
				results[index].err = err
			}
			return results, nil
		}
		copy(results[:accepted], grouped.items)
	case <-ctx.Done():
		for index := range results[:accepted] {
			results[index].err = ctx.Err()
		}
	}
	return results, nil
}

func pipelineContainsBlockingCommand(commands [][]any) bool {
	for _, command := range commands {
		if isBlockingCommand(command) {
			return true
		}
	}
	return false
}

func (e *AutoBatchExecutor) executeBlockingPipelineDirect(
	ctx context.Context,
	commands [][]any,
) ([]pipelineItemResult, error) {
	results := make([]pipelineItemResult, len(commands))
	if e.isClosed.Load() {
		results[0].err = errAutoBatchClosed
		markPipelineNotExecuted(results[1:], errAutoBatchClosed)
		return results, nil
	}

	prepared := make([][]any, len(commands))
	accepted := len(commands)
	var prepareErr error
	for index, command := range commands {
		prepared[index], prepareErr = materializeDeferredCodecValuesForExecutor(
			command,
			e.client.exec,
			&e.codecMu,
		)
		if prepareErr != nil {
			accepted = index
			prepareErr = fmt.Errorf("encode autobatch command %d: %w", index, prepareErr)
			break
		}
	}

	if accepted > 0 {
		direct, err := e.client.pipelineDetailedWithQueuePolicy(
			ctx,
			prepared[:accepted],
			nil,
		)
		if err != nil {
			for index := range results[:accepted] {
				results[index].err = err
			}
		} else {
			copy(results[:accepted], direct)
		}
	}
	if prepareErr != nil {
		results[accepted].err = prepareErr
		markPipelineNotExecuted(results[accepted+1:], prepareErr)
	}
	return results, nil
}
