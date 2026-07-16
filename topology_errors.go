package ferricstore

import (
	"errors"
	"fmt"
	"sort"
)

// TopologyWriteFailure identifies the route and keys affected by one failed
// shard operation within a partially completed cross-shard write.
type TopologyWriteFailure struct {
	Route RoutingRoute
	Keys  []string
	Err   error
}

func (e *TopologyWriteFailure) Error() string {
	if e == nil {
		return ""
	}
	endpoint := e.Route.EndpointKey
	if endpoint == "" {
		endpoint = endpointKey(e.Route.Endpoint)
	}
	return fmt.Sprintf(
		"topology write to shard %d lane %d endpoint %s failed for %d keys: %v",
		e.Route.Shard, e.Route.LaneID, endpoint, len(e.Keys), e.Err,
	)
}

func (e *TopologyWriteFailure) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func topologyStringKeyWriteFailure(
	attributed bool,
	route RoutingRoute,
	keys []string,
	err error,
) error {
	if !attributed || err == nil {
		return err
	}
	return &TopologyWriteFailure{
		Route: route,
		Keys:  append([]string(nil), keys...),
		Err:   err,
	}
}

func topologyKeyWriteFailure(
	attributed bool,
	route RoutingRoute,
	keys []any,
	err error,
) error {
	if !attributed || err == nil {
		return err
	}
	stringKeys := make([]string, len(keys))
	for index, key := range keys {
		stringKeys[index] = asString(key)
	}
	return &TopologyWriteFailure{Route: route, Keys: stringKeys, Err: err}
}

// TopologyPartialWriteError reports the observable result of an explicitly
// enabled cross-shard destructive command when one or more shards fail.
type TopologyPartialWriteError struct {
	Command   string
	Succeeded int64
	Failures  []error
}

func newTopologyPartialWriteError(command string, succeeded int64, failures []error) *TopologyPartialWriteError {
	ordered := append([]error(nil), failures...)
	sort.SliceStable(ordered, func(left, right int) bool {
		return topologyFailureLess(ordered[left], ordered[right])
	})
	return &TopologyPartialWriteError{Command: command, Succeeded: succeeded, Failures: ordered}
}

func topologyFailureLess(left, right error) bool {
	var leftFailure, rightFailure *TopologyWriteFailure
	leftAttributed := errors.As(left, &leftFailure)
	rightAttributed := errors.As(right, &rightFailure)
	if leftAttributed != rightAttributed {
		return leftAttributed
	}
	if !leftAttributed {
		return topologyErrorText(left) < topologyErrorText(right)
	}
	leftEndpoint := leftFailure.Route.EndpointKey
	if leftEndpoint == "" {
		leftEndpoint = endpointKey(leftFailure.Route.Endpoint)
	}
	rightEndpoint := rightFailure.Route.EndpointKey
	if rightEndpoint == "" {
		rightEndpoint = endpointKey(rightFailure.Route.Endpoint)
	}
	if leftEndpoint != rightEndpoint {
		return leftEndpoint < rightEndpoint
	}
	if leftFailure.Route.Shard != rightFailure.Route.Shard {
		return leftFailure.Route.Shard < rightFailure.Route.Shard
	}
	if leftFailure.Route.LaneID != rightFailure.Route.LaneID {
		return leftFailure.Route.LaneID < rightFailure.Route.LaneID
	}
	if comparison := compareTopologyFailureKeys(leftFailure.Keys, rightFailure.Keys); comparison != 0 {
		return comparison < 0
	}
	return topologyErrorText(leftFailure.Err) < topologyErrorText(rightFailure.Err)
}

func compareTopologyFailureKeys(left, right []string) int {
	for index := 0; index < len(left) && index < len(right); index++ {
		if left[index] < right[index] {
			return -1
		}
		if left[index] > right[index] {
			return 1
		}
	}
	if len(left) < len(right) {
		return -1
	}
	if len(left) > len(right) {
		return 1
	}
	return 0
}

func topologyErrorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (e *TopologyPartialWriteError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("cross-shard %s partially completed: %d successful mutations, %d shard failures", e.Command, e.Succeeded, len(e.Failures))
}

func (e *TopologyPartialWriteError) Unwrap() []error {
	if e == nil {
		return nil
	}
	return e.Failures
}
