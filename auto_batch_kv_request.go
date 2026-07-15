package ferricstore

import (
	"context"
	"fmt"
)

type autoBatchTypedKVKind uint8

const (
	autoBatchTypedKVNone autoBatchTypedKVKind = iota
	autoBatchTypedKVMGet
	autoBatchTypedKVMSet
	autoBatchTypedKVMSetNX
	autoBatchTypedKVDel
	autoBatchTypedKVExists
)

type autoBatchRequestControl struct {
	typedKind        autoBatchTypedKVKind
	typedKeys        []string
	typedValues      []any
	pipelineCommands [][]any
	allowQueued      bool
	flush            bool
}

type autoBatchExplicitPipelineResult struct {
	items []pipelineItemResult
}

var (
	autoBatchDisallowQueuedControl = &autoBatchRequestControl{}
	autoBatchFlushControl          = &autoBatchRequestControl{flush: true}
)

func (r autoBatchRequest) isFlushRequest() bool {
	return r.control != nil && r.control.flush
}

func (r autoBatchRequest) queueAllowed() bool {
	return r.control == nil || r.control.allowQueued
}

func (r autoBatchRequest) isExplicitPipeline() bool {
	return r.control != nil && r.control.pipelineCommands != nil
}

func (e *AutoBatchExecutor) executeAutoBatchRequests(
	ctx context.Context,
	requests []autoBatchRequest,
) ([]pipelineItemResult, error) {
	if len(requests) == 1 && requests[0].canExecuteTypedKVDirect(e.client.exec) {
		return []pipelineItemResult{e.executeTypedKVDirect(ctx, requests[0])}, nil
	}
	hasExplicitPipeline := false
	commandCount := len(requests)
	for index := range requests {
		if !requests[index].isExplicitPipeline() {
			continue
		}
		hasExplicitPipeline = true
		commandCount += len(requests[index].control.pipelineCommands) - 1
	}
	if !hasExplicitPipeline {
		commands := make([][]any, len(requests))
		var allowQueued []bool
		for index := range requests {
			commands[index] = requests[index].commandArgs()
			if !requests[index].queueAllowed() {
				if allowQueued == nil {
					allowQueued = make([]bool, len(requests))
					for policyIndex := range allowQueued {
						allowQueued[policyIndex] = true
					}
				}
				allowQueued[index] = false
			}
		}
		return e.client.pipelineDetailedWithQueuePolicy(ctx, commands, allowQueued)
	}

	commands := make([][]any, 0, commandCount)
	var allowQueued []bool
	for index := range requests {
		request := requests[index]
		if request.isExplicitPipeline() {
			commands = append(commands, request.control.pipelineCommands...)
			continue
		}
		commandIndex := len(commands)
		commands = append(commands, request.commandArgs())
		if !request.queueAllowed() {
			if allowQueued == nil {
				allowQueued = make([]bool, commandCount)
				for policyIndex := range allowQueued {
					allowQueued[policyIndex] = true
				}
			}
			allowQueued[commandIndex] = false
		}
	}
	flatResults, err := e.client.pipelineDetailedWithQueuePolicy(ctx, commands, allowQueued)
	if err != nil {
		return nil, err
	}
	if len(flatResults) != commandCount {
		return nil, fmt.Errorf(
			"ferricstore autobatch pipeline returned %d results for %d commands",
			len(flatResults), commandCount,
		)
	}
	results := make([]pipelineItemResult, len(requests))
	commandIndex := 0
	for index := range requests {
		request := requests[index]
		if request.isExplicitPipeline() {
			count := len(request.control.pipelineCommands)
			results[index].value = autoBatchExplicitPipelineResult{
				items: flatResults[commandIndex : commandIndex+count],
			}
			commandIndex += count
			continue
		}
		results[index] = flatResults[commandIndex]
		commandIndex++
	}
	return results, nil
}

func (r autoBatchRequest) canExecuteTypedKVDirect(exec Executor) bool {
	if r.control == nil {
		return false
	}
	switch r.control.typedKind {
	case autoBatchTypedKVMGet, autoBatchTypedKVMSet:
		_, ok := exec.(keyValueBulkExecutor)
		return ok
	case autoBatchTypedKVMSetNX:
		_, ok := exec.(keyValueMSetNXExecutor)
		return ok
	case autoBatchTypedKVDel:
		_, ok := exec.(keyValueDelExecutor)
		return ok
	case autoBatchTypedKVExists:
		_, ok := exec.(keyValueExistsExecutor)
		return ok
	default:
		return false
	}
}

func (e *AutoBatchExecutor) executeTypedKVDirect(
	ctx context.Context,
	request autoBatchRequest,
) pipelineItemResult {
	value, queued, err := e.client.typedCommandWithState(
		ctx,
		request.queueAllowed(),
		func() (any, error) { return request.executeTypedKV(ctx, e.client.exec) },
		request.commandArgs,
	)
	return pipelineItemResult{value: wrapTypedCommandState(value, queued), err: err}
}

func (r autoBatchRequest) executeTypedKV(ctx context.Context, exec Executor) (any, error) {
	control := r.control
	switch control.typedKind {
	case autoBatchTypedKVMGet:
		return exec.(keyValueBulkExecutor).keyValueMGet(ctx, control.typedKeys)
	case autoBatchTypedKVMSet:
		return exec.(keyValueBulkExecutor).keyValueMSet(ctx, control.typedKeys, control.typedValues)
	case autoBatchTypedKVMSetNX:
		return exec.(keyValueMSetNXExecutor).keyValueMSetNX(ctx, control.typedKeys, control.typedValues)
	case autoBatchTypedKVDel:
		return exec.(keyValueDelExecutor).keyValueDel(ctx, control.typedKeys)
	case autoBatchTypedKVExists:
		return exec.(keyValueExistsExecutor).keyValueExists(ctx, control.typedKeys)
	default:
		panic("ferricstore: invalid typed autobatch request")
	}
}

func (r autoBatchRequest) commandArgs() []any {
	if r.control == nil {
		return r.args
	}
	control := r.control
	switch control.typedKind {
	case autoBatchTypedKVMGet:
		return keyListCommandArgs("MGET", control.typedKeys)
	case autoBatchTypedKVMSet:
		return keyValuePairCommandArgs("MSET", control.typedKeys, control.typedValues)
	case autoBatchTypedKVMSetNX:
		return keyValuePairCommandArgs("MSETNX", control.typedKeys, control.typedValues)
	case autoBatchTypedKVDel:
		return keyListCommandArgs("DEL", control.typedKeys)
	case autoBatchTypedKVExists:
		return keyListCommandArgs("EXISTS", control.typedKeys)
	default:
		return r.args
	}
}
