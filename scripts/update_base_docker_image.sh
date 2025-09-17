docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t dalmasbluesquarehub/templates-ci:latest \
  -f Dockerfile.base \
  --push .