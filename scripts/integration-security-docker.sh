#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

image="${FERRICSTORE_IMAGE:-ghcr.io/ferricstore/ferricstore:0.10.2@sha256:e6116d6f6c2c701e7c12076ed55233f4305e5fd6ff627cc3ed4ab7f828940cf3}"
suffix="$$-$RANDOM"
bootstrap_name="ferricstore-go-security-bootstrap-$suffix"
server_name="ferricstore-go-security-$suffix"
volume_name="ferricstore-go-security-$suffix"
node_hostname="ferricstore-security-$suffix"
cert_dir="$(mktemp -d "$PWD/.ferricstore-security.XXXXXX")"
ready_log="$(mktemp)"

cleanup() {
  local status=$?
  if [[ "$status" != 0 ]]; then
    docker logs --tail 200 "$bootstrap_name" >&2 2>/dev/null || true
    docker logs --tail 200 "$server_name" >&2 2>/dev/null || true
  fi
  docker rm -f "$bootstrap_name" "$server_name" >/dev/null 2>&1 || true
  docker volume rm -f "$volume_name" >/dev/null 2>&1 || true
  rm -rf "$cert_dir"
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

wait_for_test() {
  local pattern="$1"
  for _ in $(seq 1 60); do
    if run_go_test -tags=integration -run "$pattern" . >"$ready_log" 2>&1; then
      return 0
    fi
    sleep 1
  done
  cat "$ready_log" >&2
  return 1
}

openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 2 \
  -subj "/CN=FerricStore Go SDK Test CA" \
  -keyout "$cert_dir/ca-key.pem" -out "$cert_dir/ca-cert.pem" >/dev/null 2>&1
openssl req -new -newkey rsa:2048 -nodes -sha256 \
  -subj "/CN=localhost" \
  -keyout "$cert_dir/server-key.pem" -out "$cert_dir/server.csr" >/dev/null 2>&1
openssl x509 -req -sha256 -days 2 \
  -in "$cert_dir/server.csr" -CA "$cert_dir/ca-cert.pem" -CAkey "$cert_dir/ca-key.pem" -CAcreateserial \
  -out "$cert_dir/server-cert.pem" \
  -extfile <(printf '%s\n' 'basicConstraints=CA:FALSE' 'keyUsage=digitalSignature,keyEncipherment' 'extendedKeyUsage=serverAuth' 'subjectAltName=DNS:localhost,IP:127.0.0.1') >/dev/null 2>&1
openssl req -new -newkey rsa:2048 -nodes -sha256 \
  -subj "/CN=ferricstore-go-integration" \
  -keyout "$cert_dir/client-key.pem" -out "$cert_dir/client.csr" >/dev/null 2>&1
openssl x509 -req -sha256 -days 2 \
  -in "$cert_dir/client.csr" -CA "$cert_dir/ca-cert.pem" -CAkey "$cert_dir/ca-key.pem" -CAcreateserial \
  -out "$cert_dir/client-cert.pem" \
  -extfile <(printf '%s\n' 'basicConstraints=CA:FALSE' 'keyUsage=digitalSignature,keyEncipherment' 'extendedKeyUsage=clientAuth') >/dev/null 2>&1
chmod 0755 "$cert_dir"
chmod 0600 "$cert_dir/ca-key.pem" "$cert_dir/client-key.pem"
chmod 0644 "$cert_dir/ca-cert.pem" "$cert_dir/server-cert.pem" "$cert_dir/server-key.pem" "$cert_dir/client-cert.pem"

docker volume create "$volume_name" >/dev/null
docker run -d \
  --name "$bootstrap_name" \
  --hostname "$node_hostname" \
  -e "FERRICSTORE_NODE_NAME=ferricstore@${node_hostname}" \
  -e FERRICSTORE_PROTECTED_MODE=false \
  -v "$volume_name:/data" \
  -p 127.0.0.1::6388 \
  "$image" >/dev/null

bootstrap_port="$(docker port "$bootstrap_name" 6388/tcp | awk -F: 'NR == 1 {print $NF}')"
if [[ -z "$bootstrap_port" ]]; then
  echo "failed to resolve bootstrap native port" >&2
  exit 1
fi

export FERRICSTORE_ADDR="127.0.0.1:${bootstrap_port}"
export FERRICSTORE_SECURITY_BOOTSTRAP=1
export FERRICSTORE_SECURITY_ADMIN_PASSWORD="sdk-admin-$suffix"
export FERRICSTORE_SECURITY_READER_USER="sdk_reader"
export FERRICSTORE_SECURITY_READER_PASSWORD="sdk-reader-$suffix"
wait_for_test '^TestIntegrationSecurityBootstrap$'
unset FERRICSTORE_SECURITY_BOOTSTRAP

docker stop --time 30 "$bootstrap_name" >/dev/null
docker rm "$bootstrap_name" >/dev/null
docker run -d \
  --name "$server_name" \
  --hostname "$node_hostname" \
  -e "FERRICSTORE_NODE_NAME=ferricstore@${node_hostname}" \
  -e FERRICSTORE_PROTECTED_MODE=true \
  -e FERRICSTORE_REQUIRE_TLS=true \
  -e FERRICSTORE_NATIVE_TLS_PORT=6389 \
  -e FERRICSTORE_NATIVE_TLS_CERT_FILE=/tls/server-cert.pem \
  -e FERRICSTORE_NATIVE_TLS_KEY_FILE=/tls/server-key.pem \
  -e FERRICSTORE_NATIVE_TLS_CA_CERT_FILE=/tls/ca-cert.pem \
  -v "$volume_name:/data" \
  -v "$cert_dir:/tls:ro" \
  -p 127.0.0.1::6388 \
  -p 127.0.0.1::6389 \
  "$image" >/dev/null

plain_port="$(docker port "$server_name" 6388/tcp | awk -F: 'NR == 1 {print $NF}')"
tls_port="$(docker port "$server_name" 6389/tcp | awk -F: 'NR == 1 {print $NF}')"
if [[ -z "$plain_port" || -z "$tls_port" ]]; then
  echo "failed to resolve protected-mode ports" >&2
  exit 1
fi

export FERRICSTORE_ADDR="127.0.0.1:${plain_port}"
export FERRICSTORE_TLS_ADDR="127.0.0.1:${tls_port}"
export FERRICSTORE_SECURITY_CA_CERT="$cert_dir/ca-cert.pem"
export FERRICSTORE_SECURITY_CLIENT_CERT="$cert_dir/client-cert.pem"
export FERRICSTORE_SECURITY_CLIENT_KEY="$cert_dir/client-key.pem"
export FERRICSTORE_SECURITY_READY=1
wait_for_test '^TestIntegrationSecurityReady$'
unset FERRICSTORE_SECURITY_READY

export FERRICSTORE_SECURITY_TEST=1
run_go_test -tags=integration -run '^TestIntegrationSecurity(AuthenticationAndACL|TLSVerificationAndEnforcement)$' .
