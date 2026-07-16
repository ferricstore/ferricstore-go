#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

fuzz_time="${FERRICSTORE_FUZZ_TIME:-3s}"
fuzz_parallel="${FERRICSTORE_FUZZ_PARALLEL:-4}"
targets=(
  FuzzDecodeNativeValueBounded
  FuzzDecodeNativeCompactResponses
  FuzzNativeValueRoundTrip
  FuzzDecodedProtocolSurfaces
  FuzzParseFerricURLRoundTrip
)

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

for target in "${targets[@]}"; do
  run_go test . \
    -run '^$' \
    -fuzz "^${target}$" \
    -fuzztime "$fuzz_time" \
    -parallel "$fuzz_parallel"
done
