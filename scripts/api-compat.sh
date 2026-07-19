#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

module="github.com/ferricstore/ferricstore-go"
baseline="${FERRICSTORE_API_BASELINE:-$(tr -d '[:space:]' < .api-baseline)}"
allowed_breaks_file=".api-allowed-breaks"
apidiff_version="v0.0.0-20260709172345-9ea1abe57597"
tmp_dir="$(mktemp -d)"

cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

if command -v go >/dev/null 2>&1; then
  go_cmd=(go)
elif command -v mise >/dev/null 2>&1; then
  go_bin="$(mise which go)"
  if [[ ! -x "$go_bin" ]]; then
    echo "mise did not provide an executable go toolchain" >&2
    exit 127
  fi
  export PATH="$(dirname "$go_bin"):$PATH"
  go_cmd=(go)
else
  echo "go is not available" >&2
  exit 127
fi

case "$baseline" in
  v[0-9]*.[0-9]*.[0-9]*) ;;
  *)
    echo "invalid API baseline: $baseline" >&2
    exit 2
    ;;
esac

GOBIN="$tmp_dir/bin" "${go_cmd[@]}" install "golang.org/x/exp/cmd/apidiff@${apidiff_version}"
"${go_cmd[@]}" mod download "${module}@${baseline}"
old_dir="$("${go_cmd[@]}" list -m -f '{{.Dir}}' "${module}@${baseline}")"

(
  cd "$old_dir"
  "$tmp_dir/bin/apidiff" -m -w "$tmp_dir/old.api" "$module"
)
"$tmp_dir/bin/apidiff" -m -w "$tmp_dir/new.api" "$module"

incompatible="$($tmp_dir/bin/apidiff -m -incompatible "$tmp_dir/old.api" "$tmp_dir/new.api")"
if [[ -f "$allowed_breaks_file" ]]; then
  if ! diff -u "$allowed_breaks_file" <(printf '%s\n' "$incompatible"); then
    echo "exported API break set differs from the audited v0.8 transition:" >&2
    exit 1
  fi
  exit 0
fi
if [[ -n "$incompatible" ]]; then
  echo "exported API is incompatible with ${module}@${baseline}:" >&2
  echo "$incompatible" >&2
  exit 1
fi
