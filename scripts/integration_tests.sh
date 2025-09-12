#!/usr/bin/env bash

cd $(dirname "$0")/..

export PYTHONPATH=../

if [ -d "venv" ]; then
    source venv/bin/activate
fi

# unit and feature tests written using pytest
pytest  --maxfail=3 --disable-warnings
status_of_previous_command=$?
if [[ $status_of_previous_command -ne 0 ]]; then
    # unit tests have failed
    exit 1
fi

# expected pipeline template contents
./scripts/pipeline_template_contents.sh
status_of_previous_command=$?
if [[ $status_of_previous_command -ne 0 ]]; then
    # unit tests have failed
    exit 1
fi
