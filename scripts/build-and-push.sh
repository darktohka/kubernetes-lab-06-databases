#!/usr/bin/env sh
# Build the three demo applications for both cluster architectures and push
# them to the shared registry.
#
#   ./scripts/build-and-push.sh [registry/namespace] [tag]
#
# Defaults:
#   registry/namespace  lighthouse.tohka.us/example-projects
#   tag                 latest
#
# The cluster has both amd64 and arm64 nodes, so a single-architecture image
# fails with "exec format error" on at least one of them.

set -eu

REGISTRY="${1:-lighthouse.tohka.us/example-projects}"
TAG="${2:-latest}"

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

if ! docker buildx version >/dev/null 2>&1; then
  echo "docker buildx is required" >&2
  exit 1
fi

build() {
  name="$1"
  context="$2"
  image="$REGISTRY/$name:$TAG"

  echo "==> building $image for linux/amd64 + linux/arm64"
  docker buildx build \
    --platform linux/amd64,linux/arm64 \
    --tag "$image" \
    --push \
    "$context"
}

build lab6-postgres-demo "$ROOT/apps/postgres-demo"
build lab6-mongo-demo "$ROOT/apps/mongo-demo"
build lab6-redis-demo "$ROOT/apps/redis-demo"

echo
echo "==> manifests"
for name in lab6-postgres-demo lab6-mongo-demo lab6-redis-demo; do
  docker buildx imagetools inspect "$REGISTRY/$name:$TAG" | head -8
done
