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
	_, err := validatedCommandName(args[0])
	return err
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
