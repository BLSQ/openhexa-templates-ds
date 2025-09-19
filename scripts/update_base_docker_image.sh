#!/usr/bin/env bash
set -euo pipefail

# Create a new builder (if not exists) and use it
docker buildx create --name multi-builder --use || true
docker buildx inspect --bootstrap

# Build and push multi-platform image
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t dalmasbluesquarehub/templates-ci:latest \
  -f Dockerfile.base \
  --push .
