#!/usr/bin/env bash
set -euo pipefail

version="${1:-}"
target="${2:-}"

if [[ ! "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]]; then
  echo "release version must be a semantic v-prefixed tag" >&2
  exit 1
fi
if [[ -z "$target" ]]; then
  echo "release target commit is required" >&2
  exit 1
fi

target_sha="$(git rev-parse "${target}^{commit}")"
git fetch --quiet origin main --tags
if ! git merge-base --is-ancestor "$target_sha" origin/main; then
  echo "release target $target_sha is not on origin/main" >&2
  exit 1
fi

tag_refs="$(git ls-remote --tags origin "refs/tags/${version}" "refs/tags/${version}^{}")"
tag_sha="$(awk -v peeled="refs/tags/${version}^{}" '$2 == peeled {print $1}' <<<"$tag_refs")"
if [[ -z "$tag_sha" ]]; then
  tag_sha="$(awk -v direct="refs/tags/${version}" '$2 == direct {print $1}' <<<"$tag_refs")"
fi
if [[ -n "$tag_sha" && "$tag_sha" != "$target_sha" ]]; then
  echo "release tag $version already targets $tag_sha, not $target_sha" >&2
  exit 1
fi

if [[ "${FERRICSTORE_SKIP_PROXY_CHECK:-0}" != "1" ]]; then
  response_file="$(mktemp)"
  trap 'rm -f "$response_file"' EXIT
  proxy_url="https://proxy.golang.org/github.com/ferricstore/ferricstore-go/@v/list"
  proxy_status="$(curl --silent --show-error --output "$response_file" --write-out '%{http_code}' "$proxy_url")"
  case "$proxy_status" in
    200)
      if grep --fixed-strings --line-regexp -- "$version" "$response_file" >/dev/null &&
        [[ -z "$tag_sha" ]]; then
        echo "Go proxy already contains immutable version $version but origin has no matching tag" >&2
        exit 1
      fi
      ;;
    404|410)
      ;;
    *)
      echo "could not prove Go proxy version availability (HTTP $proxy_status)" >&2
      exit 1
      ;;
  esac
fi

if [[ -n "$tag_sha" ]]; then
  echo "release $version can safely resume at $target_sha"
else
  echo "release $version is unpublished and ready at $target_sha"
fi
