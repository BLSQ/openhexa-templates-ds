#!/usr/bin/env bash

# Run tests inside Docker, mounting the current directory
docker run --rm -t \
  -v "$(pwd)":/app \
  -w /app \
  blsq/openhexa-blsq-environment:latest \
  pytest --cov --cov-report json

status_of_previous_command=$?
if [[ $status_of_previous_command -ne 0 ]]; then
    echo "❌ Unit tests failed"
    exit 1
fi

# expected template assets
./scripts/pipeline_template_contents.sh
status_of_previous_command=$?
if [[ $status_of_previous_command -ne 0 ]]; then
    echo "❌ Template content check failed"
    exit 1
fi
