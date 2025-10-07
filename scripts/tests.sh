#!/usr/bin/env bash
set -euo pipefail

# Ensure output dir exists
mkdir -p coverage_output

# Run tests inside Docker, mounting both source and output dirs
docker run --rm -t \
  -v "$(pwd)":/app \
  -v "$(pwd)/coverage_output":/coverage_output \
  -w /app \
  blsq/openhexa-blsq-environment:latest \
  bash -c "pytest --cov --cov-report=json:/coverage_output/coverage.json"

status_of_previous_command=$?
if [[ $status_of_previous_command -ne 0 ]]; then
    echo "❌ Unit tests failed"
    exit 1
fi

# Run template structure validation
./scripts/pipeline_template_contents.sh
status_of_previous_command=$?
if [[ $status_of_previous_command -ne 0 ]]; then
    echo '❌ Template content check failed'
    exit 1
fi
