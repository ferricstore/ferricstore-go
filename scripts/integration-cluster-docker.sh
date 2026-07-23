#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

image="${FERRICSTORE_IMAGE:-ghcr.io/ferricstore/ferricstore:0.10.1@sha256:198cffba8e2df2f5f66db9e6bbef83131f4841d4b90c65ee8091ac463ec6715d}"
suffix="$$-$RANDOM"
network="ferricstore-go-cluster-$suffix"
ready_log="$(mktemp)"
cookie="sdk-cluster-$suffix"
containers=("fs-go-node1-$suffix" "fs-go-node2-$suffix" "fs-go-node3-$suffix")
hosts=("fs-node1-$suffix" "fs-node2-$suffix" "fs-node3-$suffix")
nodes=("ferricstore@${hosts[0]}" "ferricstore@${hosts[1]}" "ferricstore@${hosts[2]}")

read -r port1 port2 port3 < <(python3 - <<'PY'
import socket
sockets = []
for _ in range(3):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sockets.append(sock)
print(*(sock.getsockname()[1] for sock in sockets))
for sock in sockets:
    sock.close()
PY
)
ports=("$port1" "$port2" "$port3")

cleanup() {
  local status=$?
  if [[ "$status" != 0 ]]; then
    for container in "${containers[@]}"; do
      docker logs --tail 200 "$container" >&2 2>/dev/null || true
    done
  fi
  docker rm -f "${containers[@]}" >/dev/null 2>&1 || true
  docker network rm "$network" >/dev/null 2>&1 || true
  rm -f "$ready_log"
}
trap cleanup EXIT

run_go_test() {
  if command -v mise >/dev/null 2>&1; then
    mise exec -- go test "$@"
  else
    go test "$@"
  fi
}

start_node() {
  local index="$1"
  local discovery="$2"
  local cluster_nodes="$3"
  local auto_join="$4"
  docker run -d \
    --name "${containers[$index]}" \
    --hostname "${hosts[$index]}" \
    --network "$network" \
    -e "FERRICSTORE_NODE_NAME=${nodes[$index]}" \
    -e "FERRICSTORE_COOKIE=$cookie" \
    -e "FERRICSTORE_DISCOVERY=$discovery" \
    -e "FERRICSTORE_CLUSTER_NODES=$cluster_nodes" \
    -e "FERRICSTORE_CLUSTER_AUTO_JOIN=$auto_join" \
    -e FERRICSTORE_CLUSTER_REMOVE_DELAY_MS=1000 \
    -e FERRICSTORE_PROTECTED_MODE=false \
    -e FERRICSTORE_SHARD_COUNT=4 \
    -e "FERRICSTORE_NATIVE_PORT=${ports[$index]}" \
    -e FERRICSTORE_NATIVE_ADVERTISE_HOST=127.0.0.1 \
    -e "FERRICSTORE_NATIVE_ADVERTISE_PORT=${ports[$index]}" \
    -e FERRICSTORE_HEALTH_PORT=0 \
    -e FERRICSTORE_HEALTH_PROBE_PORT=0 \
    -p "127.0.0.1:${ports[$index]}:${ports[$index]}" \
    "$image" >/dev/null
}

docker network create "$network" >/dev/null
start_node 0 none "" false
sleep 2
start_node 1 epmd "${nodes[0]}" true
sleep 2
start_node 2 epmd "${nodes[0]},${nodes[1]}" true

urls="ferric://127.0.0.1:${ports[0]},ferric://127.0.0.1:${ports[1]},ferric://127.0.0.1:${ports[2]}"
node_list="${nodes[0]},${nodes[1]},${nodes[2]}"
export FERRICSTORE_CLUSTER_URLS="$urls"
export FERRICSTORE_CLUSTER_NODES="$node_list"
export FERRICSTORE_CLUSTER_FAILURE_CONTAINER="${containers[2]}"
export FERRICSTORE_CLUSTER_READY=1

ready=0
for _ in $(seq 1 90); do
  if run_go_test -tags=integration -run '^TestIntegrationClusterReady$' -timeout 15s . >"$ready_log" 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [[ "$ready" != 1 ]]; then
  cat "$ready_log" >&2
  exit 1
fi
unset FERRICSTORE_CLUSTER_READY

export FERRICSTORE_CLUSTER_TEST=1
run_go_test -tags=integration -run '^TestIntegrationClusterTopologyRoutingAndFailover$' -timeout 120s .
