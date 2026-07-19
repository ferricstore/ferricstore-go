package ferricstore

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unicode"
)

func validateCommandArgs(args []any) error {
	if len(args) == 0 {
		return errors.New("ferricstore command requires at least a command name")
	}
	if _, err := validatedCommandName(args[0]); err != nil {
		return err
	}
	if err := validatePublicCommandKeys(args); err != nil {
		return err
	}
	if err := validateSETCommandArgs(args); err != nil {
		return err
	}
	if err := validateV080FlowSignalArgs(args); err != nil {
		return err
	}
	return validateAtomicMSetSlots(args)
}

func validateV080FlowSignalArgs(args []any) error {
	args = canonicalCommandArgs(args)
	if len(args) == 0 || commandName(args) != "FLOW.SIGNAL" {
		return nil
	}
	if len(args) < 2 {
		return errors.New("FLOW.SIGNAL requires id and SIGNAL")
	}
	for index := 2; index < len(args); {
		token := commandPart(args[index])
		switch token {
		case "VALUE", "VALUE_REF":
			if index+2 >= len(args) {
				return errors.New("FLOW.SIGNAL named value option requires name and value")
			}
			index += 3
		case "DROP_VALUE", "OVERRIDE_VALUE":
			if index+1 >= len(args) {
				return errors.New("FLOW.SIGNAL value option requires a name")
			}
			index += 2
		case "PARTITION", "SIGNAL", "IDEMPOTENCY", "IDEMPOTENCY_KEY", "IF_STATE", "TRANSITION_TO", "RUN_AT", "NOW":
			if index+1 >= len(args) {
				return fmt.Errorf("FLOW.SIGNAL %s requires a value", token)
			}
			index += 2
		default:
			return fmt.Errorf("FLOW.SIGNAL option %q is unsupported by FerricStore 0.8", token)
		}
	}
	return nil
}

func validateAtomicMSetSlots(args []any) error {
	args = canonicalCommandArgs(args)
	if len(args) == 0 {
		return nil
	}
	name := commandName(args)
	if name != "MSET" && name != "MSETNX" {
		return nil
	}
	keys, _, ok := topologyPolicyKeys(name, args)
	if !ok {
		return nil
	}
	if _, sameSlot := singleShardKey(keys); !sameSlot {
		return fmt.Errorf("%s requires keys in one hash slot", name)
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

func validatedCommandName(value any) (string, error) {
	name, ok := commandText(value)
	if !ok {
		return "", fmt.Errorf("ferricstore command name must be string or []byte, got %T", value)
	}
	if name == "" {
		return "", errors.New("ferricstore command name is empty")
	}
	if strings.IndexFunc(name, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	}) >= 0 {
		return "", fmt.Errorf("ferricstore command name %q contains whitespace or control characters", name)
	}
	return name, nil
}

func commandText(value any) (string, bool) {
	switch text := value.(type) {
	case string:
		return text, true
	case []byte:
		return string(text), true
	}
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() {
		return "", false
	}
	switch reflected.Kind() {
	case reflect.String:
		return reflected.String(), true
	case reflect.Slice:
		if reflected.Type().Elem().Kind() == reflect.Uint8 {
			return string(reflected.Bytes()), true
		}
	}
	return "", false
}
