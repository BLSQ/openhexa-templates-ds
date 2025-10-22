#!/usr/bin/env bash
set -euo pipefail

# Run template structure validation
./scripts/pipeline_template_contents.sh
status_of_previous_command=$?
if [[ $status_of_previous_command -ne 0 ]]; then
    echo '❌ Template content check failed'
    exit 1
fi
