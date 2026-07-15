package ferricstore

import (
	"errors"
	"fmt"
)

func validateCommandArgs(args []any) error {
	if len(args) == 0 {
		return errors.New("ferricstore command requires at least a command name")
	}
	if asString(args[0]) == "" {
		return errors.New("ferricstore command name is empty")
	}
	return nil
}

func validatePipelineCommands(commands [][]any) error {
	for index, command := range commands {
		if err := validateCommandArgs(command); err != nil {
			return fmt.Errorf("ferricstore pipeline command %d: %w", index, err)
		}
	}
	return nil
}
