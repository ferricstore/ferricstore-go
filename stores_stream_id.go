package ferricstore

import "fmt"

func streamIDStringResponse(command string, value any, err error) (string, error) {
	id, err := responseString(value, err)
	if err != nil {
		return "", err
	}
	milliseconds, sequence, ok := parseStreamIDText(id)
	if !ok || milliseconds == 0 && sequence == 0 {
		return "", fmt.Errorf("%s returned invalid stream ID %q", command, id)
	}
	return id, nil
}

func parseStreamIDResponse(value any) (uint64, uint64, bool) {
	switch id := value.(type) {
	case string:
		return parseStreamIDText(id)
	case []byte:
		return parseStreamIDText(id)
	default:
		return 0, 0, false
	}
}

func parseStreamIDText[T ~string | ~[]byte](id T) (uint64, uint64, bool) {
	separator := -1
	for index := range len(id) {
		if id[index] != '-' {
			continue
		}
		if separator >= 0 {
			return 0, 0, false
		}
		separator = index
	}
	if separator <= 0 || separator >= len(id)-1 {
		return 0, 0, false
	}
	milliseconds, ok := parseStreamIDPart(id, 0, separator)
	if !ok {
		return 0, 0, false
	}
	sequence, ok := parseStreamIDPart(id, separator+1, len(id))
	if !ok {
		return 0, 0, false
	}
	return milliseconds, sequence, true
}

func parseStreamIDPart[T ~string | ~[]byte](id T, start, end int) (uint64, bool) {
	var number uint64
	for index := start; index < end; index++ {
		digit := id[index]
		if digit < '0' || digit > '9' {
			return 0, false
		}
		value := uint64(digit - '0')
		if number > (^uint64(0)-value)/10 {
			return 0, false
		}
		number = number*10 + value
	}
	return number, true
}

func compareStreamIDs(leftMS, leftSequence, rightMS, rightSequence uint64) int {
	if leftMS < rightMS || leftMS == rightMS && leftSequence < rightSequence {
		return -1
	}
	if leftMS == rightMS && leftSequence == rightSequence {
		return 0
	}
	return 1
}
