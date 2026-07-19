#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

stress_count="${FERRICSTORE_STRESS_COUNT:-10}"
benchmark_time="${FERRICSTORE_BENCHTIME:-50x}"
stress_pattern='Test(NativeFlowController|AutoBatch|Buffered|Topology.*(Close|Refresh|Snapshot|Pipeline|Route)|Native.*(Reconnect|Cancellation|Canceled|Pending|Close|Generation|Write))'

run_go() {
  if command -v go >/dev/null 2>&1; then
    go "$@"
  elif command -v mise >/dev/null 2>&1; then
    mise exec -- go "$@"
  else
    echo "go is not available" >&2
    return 127
  fi
}

run_go test -race . -run "$stress_pattern" -count "$stress_count" -timeout 10m
run_go test . -run 'Allocation|BoundedAllocation|ResourceBound' -count 1
run_go test . -run '^$' -bench '.' -benchmem -benchtime "$benchmark_time" -count 1
./scripts/benchmark-gate.sh
