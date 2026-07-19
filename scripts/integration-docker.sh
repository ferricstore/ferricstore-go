#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

image="${FERRICSTORE_IMAGE:-ghcr.io/ferricstore/ferricstore:0.8.0}"
name="${FERRICSTORE_TEST_CONTAINER:-ferricstore-go-integration-$$}"

cleanup() {
  docker rm -f "$name" >/dev/null 2>&1 || true
  if [[ -n "${ready_log:-}" ]]; then
    rm -f "$ready_log"
  fi
}
trap cleanup EXIT

docker run -d --rm \
  --name "$name" \
  -e FERRICSTORE_PROTECTED_MODE=false \
  -p 127.0.0.1::6388 \
  "$image" >/dev/null

host_port="$(docker port "$name" 6388/tcp | awk -F: 'NR == 1 {print $NF}')"
if [[ -z "$host_port" ]]; then
  echo "failed to resolve mapped FerricStore native port" >&2
  exit 1
fi

run_go_test() {
  if command -v mise >/dev/null 2>&1; then
    mise exec -- go test "$@"
  else
    go test "$@"
  fi
}

ready_log="$(mktemp)"
ready=0
for _ in $(seq 1 60); do
  if FERRICSTORE_ADDR="127.0.0.1:${host_port}" run_go_test -tags=integration -run '^TestIntegrationKVAndFlowRoundTrip$' . >"$ready_log" 2>&1; then
    ready=1
    break
  fi
  sleep 1
done

export FERRICSTORE_ADDR="127.0.0.1:${host_port}"
if [[ "$ready" != 1 ]]; then
  cat "$ready_log" >&2
	exit 1
fi
export FERRICSTORE_STRICT_COMMAND_COVERAGE=1
run_go_test -tags=integration ./...
