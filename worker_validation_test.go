package ferricstore

import (
	"math"
	"strconv"
	"sync/atomic"
	"testing"
)

func TestFirstDuplicateStringLargeInput(t *testing.T) {
	values := make([]string, 64)
	for index := range values {
		values[index] = "value-" + strconv.Itoa(index)
	}
	values[63] = values[31]

	duplicate, found := firstDuplicateString(values)
	if !found || duplicate != values[31] {
		t.Fatalf("duplicate = %q, %t; want %q, true", duplicate, found, values[31])
	}
}

func TestRunConcurrentBoundsSemaphoreToAvailableJobs(t *testing.T) {
	jobs := []FlowRecord{{ID: "first"}, {ID: "second"}}
	var calls atomic.Int64

	runConcurrent(jobs, math.MaxInt, func(FlowRecord) {
		calls.Add(1)
	})

	if got := calls.Load(); got != int64(len(jobs)) {
		t.Fatalf("handler calls = %d; want %d", got, len(jobs))
	}
}
