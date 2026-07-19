package ferricstore

import "time"

// canonicalCommandArgs removes transport-only COMMAND_EXEC and request-context
// metadata so command routing, retry, timeout, and validation policies all see
// the same command grammar.
func canonicalCommandArgs(args []any) []any {
	for len(args) > 1 && commandPart(args[0]) == "COMMAND_EXEC" {
		args = args[1:]
	}
	payload, _, _ := splitNativeRequestContext(args)
	return payload
}

func wrappedNativeReplayPolicy(args []any) nativeReplayPolicy {
	if len(args) < 2 || commandPart(args[0]) != "COMMAND_EXEC" {
		return nativeReplayDefault
	}
	canonical := canonicalCommandArgs(args)
	if len(canonical) == 0 {
		return nativeReplayDefault
	}
	name := commandName(canonical)
	command, ok, err := buildFlowNativeCommand(name, canonical[1:])
	if err != nil || !ok {
		return nativeReplayDefault
	}
	return command.replayPolicy
}

func isBlockingCommand(args []any) bool {
	budget := blockingCommandBudget(args)
	return budget.disableDefault || budget.extension > 0
}

// nativeCommandExecutionPolicy carries transport behavior that must survive
// command wrapping and pipeline encoding. Keeping it next to command parsing
// prevents routing, timeout, and retry paths from developing independent
// interpretations of the command grammar.
type nativeCommandExecutionPolicy struct {
	budget       nativeRequestBudget
	replayPolicy nativeReplayPolicy
}

func (p *nativeCommandExecutionPolicy) add(command nativeCommand) {
	if command.replayPolicy == nativeReplayNever {
		p.replayPolicy = nativeReplayNever
	}
	if p.budget.disableDefault {
		return
	}
	if command.budget.disableDefault ||
		command.budget.extension > time.Duration(1<<63-1)-p.budget.extension {
		p.budget = nativeRequestBudget{disableDefault: true}
		return
	}
	p.budget.extension += command.budget.extension
}
