package main

import (
	"reflect"
	"testing"
	"time"
)

func TestPartitionWakeCoordinatorNextPartitionsDrainsPending(t *testing.T) {
	coord := newPartitionWakeCoordinator(2, 8)
	coord.notifyPartition(0)
	coord.notifyPartition(2)
	coord.notifyPartition(4)
	coord.notifyPartition(1)

	got, ok := coord.nextPartitions(0, time.Millisecond, 2)
	if !ok {
		t.Fatal("expected partitions")
	}
	if !reflect.DeepEqual(got, []int{0, 2}) {
		t.Fatalf("unexpected first drain: %#v", got)
	}

	got, ok = coord.nextPartitions(0, time.Millisecond, 2)
	if !ok {
		t.Fatal("expected second partition drain")
	}
	if !reflect.DeepEqual(got, []int{4}) {
		t.Fatalf("unexpected second drain: %#v", got)
	}
}

func TestRemoveIntsRetiresExhaustedPartitions(t *testing.T) {
	got := removeInts([]int{0, 2, 4, 6}, []int{2, 6})
	if !reflect.DeepEqual(got, []int{0, 4}) {
		t.Fatalf("unexpected remaining partitions: %#v", got)
	}

	got = removeInts(got, []int{0, 4})
	if len(got) != 0 {
		t.Fatalf("expected all partitions retired, got %#v", got)
	}
}
