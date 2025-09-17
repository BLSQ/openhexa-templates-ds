#!/usr/bin/env bash

# unit and feature tests written using pytest
docker build -t tests_image_ci -f Dockerfile .
docker run -t tests_image_ci:latest pytest 
status_of_previous_command=$?
if [[ $status_of_previous_command -ne 0 ]]; then
    # unit tests have failed
    exit 1
fi

# expected template assets
./scripts/pipeline_template_contents.sh
status_of_previous_command=$?
if [[ $status_of_previous_command -ne 0 ]]; then
    # unit tests have failed
    exit 1
fi
