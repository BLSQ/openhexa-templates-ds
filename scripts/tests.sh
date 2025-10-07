#!/usr/bin/env bash
set -euo pipefail

# Define paths for report outputs
HOST_DIR="$(pwd)"
COVERAGE_FILE="$HOST_DIR/pytest-coverage.txt"
JUNIT_FILE="$HOST_DIR/pytest.xml"

# Run tests inside Docker, mounting the current directory
docker run --rm -t \
  -v "$(pwd)":/app \
  -w /app \
  blsq/openhexa-blsq-environment:latest \
  pytest --junitxml="$JUNIT_FILE" \
         --cov-report=term-missing:skip-covered \
         --cov=. tests/ | tee "$COVERAGE_FILE"

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
