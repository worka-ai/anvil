#!/usr/bin/env bash
set -euo pipefail

image="${1:?usage: resolve-docker-image-id.sh IMAGE}"

if image_id="$(docker image inspect --format '{{.Id}}' "$image" 2>/dev/null)"; then
  printf '%s\n' "$image_id"
  exit 0
fi

# Docker Desktop can list a repository/tag while its tag resolver temporarily
# returns NoSuchImage. Resolve the immutable ID from the listing and verify it.
image_id="$(
  docker image ls --no-trunc --format '{{.Repository}}:{{.Tag}}\t{{.ID}}' \
    | awk -F '\t' -v image="$image" '$1 == image { print $2; exit }'
)"
if [[ "$image_id" != sha256:* ]]; then
  echo "Docker image '$image' is not available" >&2
  exit 1
fi

docker image inspect "$image_id" >/dev/null
printf '%s\n' "$image_id"
