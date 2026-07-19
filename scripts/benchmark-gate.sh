#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

benchmark_time="${FERRICSTORE_BENCH_GATE_TIME:-100ms}"
benchmark_count="${FERRICSTORE_BENCH_GATE_COUNT:-3}"
runtime_scale="${FERRICSTORE_BENCH_GATE_RUNTIME_SCALE:-1}"
benchmark_pattern='^(BenchmarkAutoBatchExplicitPipeline100|BenchmarkValidateAppliedPolicySnapshot|BenchmarkNativeFlowControllerUncontended|BenchmarkNativeResponseAssemblerSingleFrame|BenchmarkNativeResponseAssemblerChunkedBinary|BenchmarkKeyValueMGet100|BenchmarkKeyValueMSet100|BenchmarkTopologySameRoutePlan100|BenchmarkTopologyRouteGrouping1000)$'

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

results="$(mktemp)"
trap 'rm -f "$results"' EXIT
run_go test . -run '^$' -bench "$benchmark_pattern" -benchmem \
  -benchtime "$benchmark_time" -count "$benchmark_count" | tee "$results"

awk -v scale="$runtime_scale" '
function add(name, ns, bytes, allocs) {
  max_ns[name] = ns * scale
  max_bytes[name] = bytes
  max_allocs[name] = allocs
}
function median_ns(name, count,    left, right, swap, result) {
  for (left = 1; left <= count; left++) {
    ordered[left] = samples[name SUBSEP left]
  }
  for (left = 1; left <= count; left++) {
    for (right = left + 1; right <= count; right++) {
      if (ordered[right] < ordered[left]) {
        swap = ordered[left]
        ordered[left] = ordered[right]
        ordered[right] = swap
      }
    }
  }
  result = ordered[int(count / 2) + 1]
  for (left = 1; left <= count; left++) delete ordered[left]
  return result
}
BEGIN {
  if (scale <= 0) {
    print "benchmark gate scale must be positive" > "/dev/stderr"
    invalid_scale = 1
    exit
  }
  # Runtime limits deliberately allow slower shared CI hosts while catching
  # order-of-magnitude regressions. Allocation limits guard the stable shape.
  add("BenchmarkAutoBatchExplicitPipeline100", 500000, 65536, 400)
  add("BenchmarkValidateAppliedPolicySnapshot", 5000, 0, 0)
  add("BenchmarkNativeFlowControllerUncontended", 5000, 0, 0)
  add("BenchmarkNativeResponseAssemblerSingleFrame", 2000, 128, 2)
  add("BenchmarkNativeResponseAssemblerChunkedBinary", 10000000, 1100000, 12)
  add("BenchmarkKeyValueMGet100", 50000, 2048, 2)
  add("BenchmarkKeyValueMSet100", 75000, 4096, 2)
  add("BenchmarkTopologySameRoutePlan100", 25000, 0, 0)
  add("BenchmarkTopologyRouteGrouping1000", 250000, 1024, 4)
}
$1 ~ /^Benchmark/ {
  name = $1
  sub(/-[0-9]+$/, "", name)
  if (!(name in max_ns)) {
    next
  }
  ns = $3 + 0
  bytes = -1
  allocs = -1
  for (field = 4; field <= NF; field++) {
    if ($field == "B/op") {
      bytes = $(field - 1) + 0
    } else if ($field == "allocs/op") {
      allocs = $(field - 1) + 0
    }
  }
  seen[name]++
  samples[name SUBSEP seen[name]] = ns
  if (bytes < 0 || allocs < 0) malformed[name] = 1
  if (bytes > worst_bytes[name]) worst_bytes[name] = bytes
  if (allocs > worst_allocs[name]) worst_allocs[name] = allocs
}
END {
  if (invalid_scale) exit 2
  failed = 0
  for (name in max_ns) {
    if (!seen[name]) {
      printf "FAIL %s: benchmark result missing\n", name > "/dev/stderr"
      failed = 1
      continue
    }
    if (malformed[name]) {
      printf "FAIL %s: benchmark memory metrics missing\n", name > "/dev/stderr"
      failed = 1
      continue
    }
    runtime = median_ns(name, seen[name])
    printf "GATE %s: median %.2f ns/op, max %.0f B/op, max %.0f allocs/op\n", \
      name, runtime, worst_bytes[name], worst_allocs[name]
    if (runtime > max_ns[name] ||
        worst_bytes[name] > max_bytes[name] ||
        worst_allocs[name] > max_allocs[name]) {
      printf "FAIL %s exceeds median/max limits %.2f ns/op, %.0f B/op, %.0f allocs/op\n", \
        name, max_ns[name], max_bytes[name], max_allocs[name] > "/dev/stderr"
      failed = 1
    }
  }
  exit failed
}
' "$results"
