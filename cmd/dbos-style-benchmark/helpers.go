package main

import (
	"math"
	"strconv"
)

func chunks(values []int, size int) [][]int {
	if size <= 0 {
		size = 1
	}
	out := make([][]int, 0, (len(values)+size-1)/size)
	for start := 0; start < len(values); start += size {
		end := start + size
		if end > len(values) {
			end = len(values)
		}
		out = append(out, values[start:end])
	}
	return out
}

func partitionFor(index, partitions int, prefix string) string {
	if partitions <= 0 {
		partitions = 1
	}
	return prefix + ":partition:" + strconv.Itoa(index%partitions)
}

func flowID(prefix string, index int) string {
	return prefix + ":flow:" + strconv.Itoa(index)
}

func workerID(prefix string, index int) string {
	return prefix + ":worker:" + strconv.Itoa(index)
}

func partitionKeysFor(indices []int, partitions int, prefix string) []string {
	keys := make([]string, 0, len(indices))
	for _, index := range indices {
		keys = append(keys, partitionFor(index, partitions, prefix))
	}
	return keys
}

func removeInts(values []int, remove []int) []int {
	if len(values) == 0 || len(remove) == 0 {
		return values
	}
	removeSet := make(map[int]struct{}, len(remove))
	for _, value := range remove {
		removeSet[value] = struct{}{}
	}
	out := values[:0]
	for _, value := range values {
		if _, ok := removeSet[value]; ok {
			continue
		}
		out = append(out, value)
	}
	return out
}

func partitionIndexForClaim(workerIndex, workerCount, partitions, claimRound int) int {
	if partitions <= 0 {
		return 0
	}
	if workerCount >= partitions {
		return workerIndex % partitions
	}
	return (workerIndex + claimRound*workerCount) % partitions
}

func sumStats(stats []phaseStats, fn func(phaseStats) int64) int64 {
	var total int64
	for _, stat := range stats {
		total += fn(stat)
	}
	return total
}

func maxStats(stats []phaseStats, fn func(phaseStats) int64) int64 {
	var max int64
	for _, stat := range stats {
		if value := fn(stat); value > max {
			max = value
		}
	}
	return max
}

func wakeNotifications(wake *partitionWakeCoordinator) int64 {
	if wake == nil {
		return 0
	}
	return wake.notifications.Load()
}

func avg(numerator, denominator int64) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func rate(count int64, seconds float64) float64 {
	if seconds <= 0 {
		return 0
	}
	return float64(count) / seconds
}

func avgFloat(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var total float64
	for _, value := range values {
		total += value
	}
	return total / float64(len(values))
}

func percentile(values []float64, pct float64) float64 {
	if len(values) == 0 {
		return 0
	}
	index := int(math.Ceil((pct/100.0)*float64(len(values)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}
