package ferricstore

import (
	"bytes"
	"fmt"
	"math"
)

func parseResponseIntBytes(value []byte) (int64, bool) {
	text := bytes.TrimSpace(value)
	if len(text) == 0 {
		return 0, false
	}
	negative := false
	if text[0] == '+' || text[0] == '-' {
		negative = text[0] == '-'
		text = text[1:]
		if len(text) == 0 {
			return 0, false
		}
	}
	limit := uint64(math.MaxInt64)
	if negative {
		limit++
	}
	var result uint64
	for _, digit := range text {
		if digit < '0' || digit > '9' {
			return 0, false
		}
		value := uint64(digit - '0')
		if result > (limit-value)/10 {
			return 0, false
		}
		result = result*10 + value
	}
	if !negative {
		return int64(result), true
	}
	if result == uint64(math.MaxInt64)+1 {
		return math.MinInt64, true
	}
	return -int64(result), true
}

func validateStringResponse(value any, err error) error {
	if err != nil {
		return err
	}
	switch value.(type) {
	case string, []byte:
		return nil
	case nil:
		return ErrNil
	default:
		return fmt.Errorf("expected string response, got %T", value)
	}
}

func responseTextEqual(value any, expected string) bool {
	switch text := value.(type) {
	case string:
		return text == expected
	case []byte:
		return string(text) == expected
	default:
		return false
	}
}
