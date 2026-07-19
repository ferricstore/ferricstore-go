package ferricstore

import (
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
)

const maxStringKeyBytesV080 = 65_535

var (
	errReservedInternalKey   = errors.New("ferricstore: access to internal keys is not allowed")
	errEmptyStringKeyV080    = errors.New("ferricstore: empty key")
	errStringKeyTooLargeV080 = errors.New("ferricstore: key too large (maximum 65535 bytes)")
)

var internalCompoundKeyPrefixes = [...]string{
	"H:", "L:", "S:", "Z:", "X:", "XM:", "XG:", "T:",
	"V:", "VM:", "PM:", "LM:", "FC:",
}

func validatePublicCommandKeys(args []any) error {
	args = topologyCommandArgs(args)
	if len(args) == 0 {
		return nil
	}
	name := commandName(args)
	if strings.HasPrefix(name, "FLOW.") {
		return nil
	}
	keys, _, ok := topologyPolicyKeys(name, args)
	validateStringKey := ok && v080ValidatesStringCommandKeys(name)
	if !ok {
		keys, ok = publicIntrospectionKeys(name, args)
	}
	if !ok {
		return nil
	}
	for _, key := range keys {
		text, textKey := commandText(key)
		if textKey && reservedInternalKeyString(text) {
			return errReservedInternalKey
		}
		if textKey && validateStringKey {
			if err := validateStringKeyV080(text); err != nil {
				return err
			}
		}
	}
	return nil
}

func v080ValidatesStringCommandKeys(name string) bool {
	switch name {
	case "GET", "SET", "MSET", "MSETNX":
		return true
	default:
		return false
	}
}

func publicIntrospectionKeys(name string, args []any) ([]any, bool) {
	for name == "COMMAND" && len(args) > 2 && commandPart(args[1]) == "GETKEYS" {
		args = topologyCommandArgs(args[2:])
		if len(args) == 0 {
			return nil, false
		}
		name = commandName(args)
		if keys, _, ok := topologyPolicyKeys(name, args); ok {
			return keys, true
		}
	}
	switch name {
	case "CLUSTER.KEYSLOT", "ROUTE":
		if len(args) > 1 {
			return args[1:2], true
		}
	case "ROUTE_BATCH":
		if len(args) > 1 {
			return args[1:], true
		}
	}
	return nil, false
}

func validatePublicStringKeys(keys []string) error {
	for _, key := range keys {
		if err := validatePublicStringKey(key); err != nil {
			return err
		}
	}
	return nil
}

func validatePublicStringKey(key string) error {
	if reservedInternalKeyString(key) {
		return errReservedInternalKey
	}
	return nil
}

func validatePublicV080StringKey(key string) error {
	if err := validatePublicStringKey(key); err != nil {
		return err
	}
	return validateStringKeyV080(key)
}

func validatePublicV080StringKeys(keys []string) error {
	for _, key := range keys {
		if err := validatePublicV080StringKey(key); err != nil {
			return err
		}
	}
	return nil
}

func validateStringKeyV080(key string) error {
	switch {
	case key == "":
		return errEmptyStringKeyV080
	case len(key) > maxStringKeyBytesV080:
		return errStringKeyTooLargeV080
	default:
		return nil
	}
}

func reservedInternalKey(value any) bool {
	key, ok := commandText(value)
	return ok && reservedInternalKeyString(key)
}

func reservedInternalKeyString(key string) bool {
	if strings.HasPrefix(key, "f:{__server__}:catalog:") || reservedFlowInternalKey(key) {
		return true
	}
	for _, prefix := range internalCompoundKeyPrefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func reservedFlowInternalKey(key string) bool {
	if !strings.HasPrefix(key, "f:{") {
		return false
	}
	rest := key[3:]
	end := strings.IndexByte(rest, '}')
	if end <= 0 {
		return false
	}
	tag, suffix := rest[:end], rest[end+1:]
	if suffix != "" && !strings.HasPrefix(suffix, ":") {
		return false
	}
	return reservedFlowTag(tag)
}

func reservedFlowTag(tag string) bool {
	switch tag {
	case "f", "flow-governance":
		return true
	}
	if bucketText, ok := strings.CutPrefix(tag, "fa:"); ok {
		bucket, err := strconv.Atoi(bucketText)
		return err == nil && bucket >= 0 && bucket < 256 && strconv.Itoa(bucket) == bucketText
	}
	for _, prefix := range []string{"f:", "fgc:"} {
		digest, ok := strings.CutPrefix(tag, prefix)
		if !ok || len(digest) != 43 {
			continue
		}
		decoded, err := base64.RawURLEncoding.DecodeString(digest)
		if err == nil && len(decoded) == 32 && base64.RawURLEncoding.EncodeToString(decoded) == digest {
			return true
		}
	}
	return false
}
