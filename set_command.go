package ferricstore

import (
	"errors"
	"fmt"
	"strconv"
)

type setCommandOptions struct {
	ttlMS, exat, pxat        int64
	ttlSet, exatSet, pxatSet bool
	nx, xx, get, keepTTL     bool
}

func validateSETCommandArgs(args []any) error {
	args = canonicalCommandArgs(args)
	if len(args) == 0 || commandName(args) != "SET" {
		return nil
	}
	if len(args) < 3 {
		return errors.New("SET requires key and value")
	}
	_, err := parseSETCommandOptions(args[3:])
	return err
}

func parseSETCommandOptions(args []any) (setCommandOptions, error) {
	var options setCommandOptions
	expiryMode := ""
	for index := 0; index < len(args); {
		token := commandPart(args[index])
		if token == "" {
			return options, fmt.Errorf("SET option at position %d must be a string", index+1)
		}
		switch token {
		case "NX":
			if options.nx {
				return options, errors.New("SET option NX is duplicated")
			}
			options.nx = true
			index++
		case "XX":
			if options.xx {
				return options, errors.New("SET option XX is duplicated")
			}
			options.xx = true
			index++
		case "GET":
			if options.get {
				return options, errors.New("SET option GET is duplicated")
			}
			options.get = true
			index++
		case "KEEPTTL":
			if options.keepTTL {
				return options, errors.New("SET option KEEPTTL is duplicated")
			}
			options.keepTTL = true
			index++
		case "EX", "PX", "EXAT", "PXAT":
			if expiryMode != "" {
				return options, errors.New("SET accepts only one expiration option")
			}
			expiryMode = token
			if index+1 >= len(args) {
				return options, fmt.Errorf("SET %s requires an expiration value", token)
			}
			value, err := setCommandPositiveInteger(args[index+1], token)
			if err != nil {
				return options, err
			}
			switch token {
			case "EX":
				options.ttlMS, options.ttlSet = value*1000, true
			case "PX":
				options.ttlMS, options.ttlSet = value, true
			case "EXAT":
				options.exat, options.exatSet = value, true
			case "PXAT":
				options.pxat, options.pxatSet = value, true
			}
			index += 2
		default:
			return options, fmt.Errorf("SET option %q is unsupported by FerricStore 0.8", token)
		}
	}
	if options.nx && options.xx {
		return options, errors.New("SET NX and XX options are mutually exclusive")
	}
	expiryModes := boolInt(options.ttlSet) + boolInt(options.exatSet) + boolInt(options.pxatSet)
	if expiryModes > 1 {
		return options, errors.New("SET accepts only one expiration option")
	}
	if options.keepTTL && expiryModes != 0 {
		return options, errors.New("SET KEEPTTL and expiration options are mutually exclusive")
	}
	return options, nil
}

func setCommandPositiveInteger(value any, option string) (int64, error) {
	context := "SET expiration"
	switch option {
	case "EX":
		context = "SET EX"
	case "PX":
		context = "SET PX"
	case "EXAT":
		context = "SET EXAT"
	case "PXAT":
		context = "SET PXAT"
	}
	integer, err := topologyInteger(value, context)
	if err != nil {
		if text, ok := commandText(value); ok {
			integer, err = strconv.ParseInt(text, 10, 64)
		}
	}
	if err != nil || integer <= 0 {
		return 0, fmt.Errorf("SET %s expiration must be a positive integer", option)
	}
	var bounds [4]*int64
	switch option {
	case "EX":
		bounds[0] = &integer
	case "PX":
		bounds[1] = &integer
	case "EXAT":
		bounds[2] = &integer
	case "PXAT":
		bounds[3] = &integer
	}
	if err := validateExpiryOptionBounds("SET", bounds[0], bounds[1], bounds[2], bounds[3]); err != nil {
		return 0, err
	}
	return integer, nil
}

func buildNativeSETCommand(args []any) (nativeCommand, error) {
	if len(args) < 2 {
		return nativeCommand{}, errors.New("SET requires key and value")
	}
	options, err := parseSETCommandOptions(args[2:])
	if err != nil {
		return nativeCommand{}, err
	}
	payload := map[string]any{"key": args[0], "value": args[1]}
	if options.ttlSet {
		payload["ttl"] = options.ttlMS
	}
	if options.exatSet {
		payload["exat"] = options.exat
	}
	if options.pxatSet {
		payload["pxat"] = options.pxat
	}
	if options.nx {
		payload["nx"] = true
	}
	if options.xx {
		payload["xx"] = true
	}
	if options.get {
		payload["get"] = true
	}
	if options.keepTTL {
		payload["keepttl"] = true
	}
	return nativeCommand{name: "SET", opcode: nativeOpSet, laneID: 1, payload: payload}, nil
}
