package ferricstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

type nativeCompactPipelineCandidate struct {
	value any
	next  int
	cost  int
}

type nativeCompactPipelineCandidates struct {
	items [4]nativeCompactPipelineCandidate
	count int
}

func (c *nativeCompactPipelineCandidates) add(value any, next, cost int) {
	c.items[c.count] = nativeCompactPipelineCandidate{value: value, next: next, cost: cost}
	c.count++
}

type nativeCompactPipelineDecision struct {
	item       int
	offset     int
	candidates nativeCompactPipelineCandidates
	next       int
	remaining  int
}

type nativeCompactPipelineState struct {
	item      int
	offset    int
	remaining int
}

func decodeNativeCompactPipelineResponse(data []byte) ([]any, error) {
	if len(data) < 5 || data[0] != nativeCompactPipelineResponse {
		return nil, errors.New("ferricstore native compact pipeline response is truncated")
	}
	count, err := nativeBoundedItemCount(
		"compact pipeline response", binary.BigEndian.Uint32(data[1:5]), len(data)-5, 2,
	)
	if err != nil {
		return nil, err
	}

	items := make([]any, count)
	decisions := make([]nativeCompactPipelineDecision, 0)
	failed := make(map[nativeCompactPipelineState]struct{})
	item, offset := 0, 5
	remaining := nativeMaxContainerItems - count
	attempts, maxAttempts := 0, 8*(count+1)+64
	var parseErr error

	for {
		state := nativeCompactPipelineState{item: item, offset: offset, remaining: remaining}
		if item == count {
			if offset == len(data) {
				return items, nil
			}
			parseErr = errors.New("ferricstore native compact pipeline response has trailing bytes")
			failed[state] = struct{}{}
		} else if _, knownBad := failed[state]; !knownBad {
			attempts++
			if attempts > maxAttempts {
				return nil, errors.New("ferricstore native compact pipeline response exceeds ambiguity budget")
			}
			candidates, candidateErr := decodeNativeCompactPipelineItem(data, offset)
			if candidateErr == nil {
				candidate, nextCandidate, affordable := nextNativeCompactPipelineCandidate(candidates, 0, remaining)
				if !affordable {
					parseErr = errors.New("ferricstore native compact pipeline response exceeds aggregate item limit")
					failed[state] = struct{}{}
				} else {
					items[item] = candidate.value
					if nextCandidate < candidates.count {
						decisions = append(decisions, nativeCompactPipelineDecision{
							item: item, offset: offset, candidates: candidates, next: nextCandidate,
							remaining: remaining,
						})
					}
					offset = candidate.next
					remaining -= candidate.cost
					item++
					continue
				}
			}
			if candidateErr != nil {
				parseErr = candidateErr
				failed[state] = struct{}{}
			}
		}

		resumed := false
		for len(decisions) > 0 {
			last := &decisions[len(decisions)-1]
			for {
				candidate, nextCandidate, affordable := nextNativeCompactPipelineCandidate(last.candidates, last.next, last.remaining)
				last.next = nextCandidate
				if !affordable {
					break
				}
				nextState := nativeCompactPipelineState{
					item: last.item + 1, offset: candidate.next, remaining: last.remaining - candidate.cost,
				}
				if _, knownBad := failed[nextState]; knownBad {
					continue
				}
				attempts++
				if attempts > maxAttempts {
					return nil, errors.New("ferricstore native compact pipeline response exceeds ambiguity budget")
				}
				items[last.item] = candidate.value
				item, offset, remaining = nextState.item, nextState.offset, nextState.remaining
				resumed = true
				break
			}
			if resumed {
				break
			}
			failed[nativeCompactPipelineState{
				item: last.item, offset: last.offset, remaining: last.remaining,
			}] = struct{}{}
			decisions = decisions[:len(decisions)-1]
		}
		if resumed {
			continue
		}
		if parseErr == nil {
			parseErr = errors.New("ferricstore native compact pipeline response is invalid")
		}
		return nil, parseErr
	}
}

func nextNativeCompactPipelineCandidate(candidates nativeCompactPipelineCandidates, start, remaining int) (nativeCompactPipelineCandidate, int, bool) {
	for index := start; index < candidates.count; index++ {
		candidate := candidates.items[index]
		if candidate.cost <= remaining {
			return candidate, index + 1, true
		}
	}
	return nativeCompactPipelineCandidate{}, candidates.count, false
}

func decodeNativeCompactPipelineItem(data []byte, offset int) (nativeCompactPipelineCandidates, error) {
	var candidates nativeCompactPipelineCandidates
	if offset >= len(data) {
		return candidates, errors.New("ferricstore native compact pipeline item is truncated")
	}
	status := data[offset]
	offset++
	if status == 0 {
		if offset >= len(data) {
			return candidates, errors.New("ferricstore native compact pipeline ok item is truncated")
		}
		present := data[offset]
		values, err := decodeNativeCompactPipelineOKValues(present, data, offset+1)
		if err != nil {
			return candidates, err
		}
		for index := 0; index < values.count; index++ {
			candidate := values.items[index]
			candidates.add([]any{"ok", candidate.value}, candidate.next, candidate.cost)
		}
		return candidates, nil
	}
	if status != 1 && status != 2 {
		return candidates, fmt.Errorf("ferricstore native compact pipeline status %d is unsupported", status)
	}
	reason, next, err := readNativeCompactBinary(data, offset)
	if err != nil {
		return candidates, err
	}
	label := "error"
	if status == 1 {
		label = "busy"
	}
	candidates.add([]any{label, reason}, next, 0)
	return candidates, nil
}

func decodeNativeCompactPipelineOKValues(present byte, data []byte, offset int) (nativeCompactPipelineCandidates, error) {
	var candidates nativeCompactPipelineCandidates
	switch present {
	case 0:
		candidates.add(nil, offset, 0)
	case 1:
		value, next, err := readNativeCompactBinary(data, offset)
		if err != nil {
			return candidates, err
		}
		candidates.add(value, next, 0)
	case 2:
		budget := newNativeCompactFlowRecordBudget()
		value, next, err := takeNativeCompactFlowRecordWithBudget(data, offset, budget)
		if err != nil {
			return candidates, err
		}
		candidates.add(value, next, nativeMaxContainerItems-budget.remaining)
	case 3:
		budget := newNativeCompactFlowRecordBudget()
		value, next, err := takeNativeCompactFlowRecordListWithBudget(data, offset, budget)
		if err != nil {
			return candidates, err
		}
		candidates.add(value, next, nativeMaxContainerItems-budget.remaining)
	case 4:
		return decodeNativeCompactPipelineClaim(data, offset)
	case 5:
		value, next, err := takeNativeCompactPipelineValueRef(data, offset)
		if err != nil {
			return candidates, err
		}
		candidates.add(value, next, 0)
	case 6:
		value, next, cost, err := takeNativeCompactPipelineBinaryList(data, offset)
		if err != nil {
			return candidates, err
		}
		candidates.add(value, next, cost)
	case 7:
		value, next, cost, err := takeNativeCompactPipelineBinaryMap(data, offset)
		if err != nil {
			return candidates, err
		}
		candidates.add(value, next, cost)
	default:
		return candidates, fmt.Errorf("ferricstore native compact pipeline value tag %d is unsupported", present)
	}
	return candidates, nil
}

func decodeNativeCompactPipelineClaim(data []byte, offset int) (nativeCompactPipelineCandidates, error) {
	var candidates nativeCompactPipelineCandidates
	id, next, err := readNativeCompactBinary(data, offset)
	if err != nil {
		return candidates, err
	}
	partition, next, err := readNativeCompactOptionalBinary(data, next)
	if err != nil {
		return candidates, err
	}
	lease, next, err := readNativeCompactBinary(data, next)
	if err != nil {
		return candidates, err
	}
	if len(data)-next < 8 {
		return candidates, errors.New("ferricstore native compact pipeline claimed job fencing token is truncated")
	}
	rawFencing := binary.BigEndian.Uint64(data[next : next+8])
	if rawFencing > math.MaxInt64 {
		return candidates, errors.New("ferricstore native compact pipeline claimed job fencing token is negative")
	}
	next += 8
	base := []any{id, partition, lease, int64(rawFencing)}

	if runState, stateNext, stateErr := readNativeCompactOptionalBinary(data, next); stateErr == nil {
		if attributes, attrsNext, cost, ok := takeNativeCompactPipelineAttributes(data, stateNext); ok {
			candidates.add(appendClaimFields(base, runState, attributes), attrsNext, cost)
		}
	}
	if attributes, attrsNext, cost, ok := takeNativeCompactPipelineAttributes(data, next); ok {
		candidates.add(appendClaimFields(base, attributes), attrsNext, cost)
	}
	if runState, stateNext, stateErr := readNativeCompactOptionalBinary(data, next); stateErr == nil {
		candidates.add(appendClaimFields(base, runState), stateNext, 0)
	}
	candidates.add(base, next, 0)
	return candidates, nil
}

func takeNativeCompactPipelineAttributes(data []byte, offset int) (map[string]any, int, int, bool) {
	budget := newNativeCompactFlowRecordBudget()
	value, rest, err := decodeNativeValueBudget(data[offset:], budget, 0)
	if err != nil {
		return nil, offset, 0, false
	}
	attributes, ok := value.(map[string]any)
	if !ok {
		return nil, offset, 0, false
	}
	return attributes, len(data) - len(rest), nativeMaxContainerItems - budget.remaining, true
}

func appendClaimFields(base []any, fields ...any) []any {
	item := make([]any, len(base), len(base)+len(fields))
	copy(item, base)
	return append(item, fields...)
}

func takeNativeCompactPipelineValueRef(data []byte, offset int) (map[string]any, int, error) {
	ref, next, err := readNativeCompactBinary(data, offset)
	if err != nil {
		return nil, offset, err
	}
	partition, next, err := readNativeCompactOptionalBinary(data, next)
	if err != nil {
		return nil, offset, err
	}
	owner, next, err := readNativeCompactOptionalBinary(data, next)
	if err != nil {
		return nil, offset, err
	}
	value := map[string]any{"ref": ref}
	if partition != nil {
		value["partition_key"] = partition
	}
	if owner != nil {
		value["owner_flow_id"] = owner
	}
	return value, next, nil
}

func takeNativeCompactPipelineBinaryList(data []byte, offset int) ([]any, int, int, error) {
	budget := nativeMaxContainerItems
	value, next, err := takeNativeCompactBinaryList(data, offset, &budget)
	return value, next, nativeMaxContainerItems - budget, err
}

func takeNativeCompactPipelineBinaryMap(data []byte, offset int) (map[string]any, int, int, error) {
	budget := nativeMaxContainerItems
	value, next, err := takeNativeCompactBinaryMap(data, offset, &budget)
	return value, next, nativeMaxContainerItems - budget, err
}
