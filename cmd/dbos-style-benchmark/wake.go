package main

import (
	"sync"
	"sync/atomic"
	"time"
)

type partitionWakeCoordinator struct {
	workers       int
	partitions    int
	chans         []chan int
	pending       []map[int]struct{}
	locks         []sync.Mutex
	notifications atomic.Int64
}

func newPartitionWakeCoordinator(workers, partitions int) *partitionWakeCoordinator {
	c := &partitionWakeCoordinator{
		workers:    workers,
		partitions: partitions,
		chans:      make([]chan int, workers),
		pending:    make([]map[int]struct{}, workers),
		locks:      make([]sync.Mutex, workers),
	}
	for i := range c.chans {
		c.chans[i] = make(chan int, partitions)
		c.pending[i] = make(map[int]struct{})
	}
	return c
}

func (c *partitionWakeCoordinator) ownerFor(partition int) int {
	return partition % c.workers
}

func (c *partitionWakeCoordinator) notifyPartition(partition int) {
	owner := c.ownerFor(partition)
	c.locks[owner].Lock()
	if _, ok := c.pending[owner][partition]; ok {
		c.locks[owner].Unlock()
		return
	}
	c.pending[owner][partition] = struct{}{}
	c.notifications.Add(1)
	c.locks[owner].Unlock()
	c.chans[owner] <- partition
}

func (c *partitionWakeCoordinator) nextPartitions(worker int, timeout time.Duration, limit int) ([]int, bool) {
	if limit <= 0 {
		limit = 1
	}
	select {
	case partition := <-c.chans[worker]:
		c.locks[worker].Lock()
		delete(c.pending[worker], partition)
		c.locks[worker].Unlock()
		partitions := []int{partition}
		for len(partitions) < limit {
			select {
			case partition := <-c.chans[worker]:
				c.locks[worker].Lock()
				delete(c.pending[worker], partition)
				c.locks[worker].Unlock()
				partitions = append(partitions, partition)
			default:
				return partitions, true
			}
		}
		return partitions, true
	case <-time.After(timeout):
		return nil, false
	}
}
