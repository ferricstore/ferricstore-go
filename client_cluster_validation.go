package ferricstore

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func validateHotnessArgs(args []any) error {
	if len(args)%2 != 0 {
		return errors.New("FERRICSTORE.HOTNESS options must be name/value pairs")
	}
	var topSeen, windowSeen bool
	for index := 0; index < len(args); index += 2 {
		option, ok := commandText(args[index])
		if !ok {
			return fmt.Errorf("FERRICSTORE.HOTNESS option %d must be text", index/2)
		}
		switch strings.ToUpper(option) {
		case "TOP":
			if topSeen {
				return errors.New("FERRICSTORE.HOTNESS TOP option is duplicated")
			}
			topSeen = true
		case "WINDOW":
			if windowSeen {
				return errors.New("FERRICSTORE.HOTNESS WINDOW option is duplicated")
			}
			windowSeen = true
		default:
			return fmt.Errorf("FERRICSTORE.HOTNESS has unsupported option %q", option)
		}
		if number, err := strconv.ParseInt(asString(args[index+1]), 10, 64); err != nil || number <= 0 {
			return fmt.Errorf("FERRICSTORE.HOTNESS %s must be a positive integer", strings.ToUpper(option))
		}
	}
	return nil
}

func validateMetricsArgs(args []any) error {
	if len(args) != 0 {
		return errors.New("FERRICSTORE.METRICS accepts no arguments")
	}
	return nil
}
