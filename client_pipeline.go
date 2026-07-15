package ferricstore

import (
	"context"
	"fmt"
)

func (c *Client) Pipeline(ctx context.Context, commands [][]any) ([]any, error) {
	if err := validatePipelineCommands(commands); err != nil {
		return nil, err
	}
	for _, command := range commands {
		if name, stateful := connectionStateCommand(command); stateful {
			return nil, fmt.Errorf("%s cannot be used in a non-affine pipeline", name)
		}
	}
	if err := c.legacyGate.readLock(ctx); err != nil {
		return nil, err
	}
	defer c.legacyGate.readUnlock()
	if session, multi := c.currentLegacySessionState(); session != nil {
		results := make([]pipelineItemResult, len(commands))
		for index, command := range commands {
			value, err := session.Do(ctx, affineCommandArgs(command)...)
			results[index] = pipelineItemResult{value: wrapTypedCommandState(value, multi), err: err}
			if err != nil {
				markPipelineNotExecuted(results[index+1:], err)
				return pipelineResultValues(results)
			}
		}
		return pipelineResultValues(results)
	}
	if _, ownsSessions := c.exec.(commandSessionProvider); ownsSessions {
		return c.pipelineUnlocked(ctx, commands)
	}
	if err := c.sessionGate.readLock(ctx); err != nil {
		return nil, err
	}
	defer c.sessionGate.readUnlock()
	return c.pipelineUnlocked(ctx, commands)
}

func (c *Client) pipelineUnlocked(ctx context.Context, commands [][]any) ([]any, error) {
	if len(commands) == 0 {
		return nil, nil
	}
	results, err := c.pipelineDetailedUnlocked(ctx, commands)
	if err != nil {
		return nil, err
	}
	return pipelineResultValues(results)
}

func (c *Client) pipelineDetailed(ctx context.Context, commands [][]any) ([]pipelineItemResult, error) {
	return c.pipelineDetailedWithQueuePolicy(ctx, commands, nil)
}

func (c *Client) pipelineDetailedWithQueuePolicy(
	ctx context.Context,
	commands [][]any,
	allowQueued []bool,
) ([]pipelineItemResult, error) {
	if allowQueued != nil && len(allowQueued) != len(commands) {
		return nil, fmt.Errorf("ferricstore pipeline received %d queue policies for %d commands", len(allowQueued), len(commands))
	}
	if err := validatePipelineCommands(commands); err != nil {
		return nil, err
	}
	for _, command := range commands {
		if name, stateful := connectionStateCommand(command); stateful {
			return nil, fmt.Errorf("%s cannot be used in a non-affine pipeline", name)
		}
	}
	if err := c.legacyGate.readLock(ctx); err != nil {
		return nil, err
	}
	defer c.legacyGate.readUnlock()
	if session, multi := c.currentLegacySessionState(); session != nil {
		results := make([]pipelineItemResult, len(commands))
		for index, command := range commands {
			if multi && allowQueued != nil && !allowQueued[index] {
				results[index].err = ErrTypedReplyInTransaction
				continue
			}
			value, err := session.Do(ctx, affineCommandArgs(command)...)
			results[index] = pipelineItemResult{value: wrapTypedCommandState(value, multi), err: err}
			if err != nil {
				markPipelineNotExecuted(results[index+1:], err)
				break
			}
		}
		return results, nil
	}
	if _, ownsSessions := c.exec.(commandSessionProvider); ownsSessions {
		return c.pipelineDetailedUnlocked(ctx, commands)
	}
	if err := c.sessionGate.readLock(ctx); err != nil {
		return nil, err
	}
	defer c.sessionGate.readUnlock()
	return c.pipelineDetailedUnlocked(ctx, commands)
}

func (c *Client) pipelineDetailedUnlocked(ctx context.Context, commands [][]any) ([]pipelineItemResult, error) {
	if len(commands) == 0 {
		return nil, nil
	}
	commands = commandBatchArgsForExecutor(c.exec, commands)
	if exec, ok := c.exec.(detailedPipelineExecutor); ok {
		results, err := exec.pipelineDetailed(ctx, commands)
		if err != nil {
			return nil, err
		}
		if len(results) != len(commands) {
			return nil, fmt.Errorf("ferricstore detailed pipeline returned %d results for %d commands", len(results), len(commands))
		}
		return results, nil
	}
	if exec, ok := c.exec.(pipelineExecutor); ok {
		values, err := exec.Pipeline(ctx, commands)
		if len(values) != len(commands) {
			if err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("ferricstore pipeline returned %d results for %d commands", len(values), len(commands))
		}
		results := make([]pipelineItemResult, len(values))
		hasItemError := false
		for i, value := range values {
			if itemErr, ok := value.(error); ok {
				results[i].err = itemErr
				hasItemError = true
			} else {
				results[i].value = value
			}
		}
		// Pipeline implementations following Client.Pipeline's contract return
		// both the complete outcome slice and an aggregate error. Once failures
		// are represented at their exact positions, the client can rebuild the
		// aggregate without discarding successful sibling results.
		if err != nil && !hasItemError {
			return nil, err
		}
		return results, nil
	}
	results := make([]pipelineItemResult, 0, len(commands))
	for _, command := range commands {
		value, err := c.commandUnlocked(ctx, command...)
		results = append(results, pipelineItemResult{value: value, err: err})
	}
	return results, nil
}
