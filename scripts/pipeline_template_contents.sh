#!/usr/bin/env bash
set -euo pipefail

# Define required files
required_files=("pipeline.py" "README.md" "requirements.txt" "expectations.yml")

# Define excluded folders
excluded_folders=("scripts" "tests" "venv")

# Get repo root (script's parent directory)
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Checking folders under $repo_root..."

# Track failing folders
failing_folders=()

# Loop through top-level directories
for dir in "$repo_root"/*/; do
    folder_name=$(basename "$dir")

    # Skip excluded and hidden folders
    if [[ " ${excluded_folders[*]} " =~ " $folder_name " ]] || [[ $folder_name == .* ]]; then
        continue
    fi

    echo "📂 Checking $folder_name..."

    missing=0
    for file in "${required_files[@]}"; do
        if [[ ! -f "$dir/$file" ]]; then
            echo "   ❌ Missing: $file"
            missing=1
        else
            echo "   ✅ Found: $file"
        fi
    done

    if [[ $missing -eq 0 ]]; then
        echo "   ✔ All required files are present."
    else
        failing_folders+=("$folder_name")
    fi

    echo
done

# Final result
if [[ ${#failing_folders[@]} -gt 0 ]]; then
    echo "❌ The following folders are missing required files:"
    for f in "${failing_folders[@]}"; do
        echo "   - $f"
    done
    exit 1
else
    echo "🎉 All folders passed the check."
    exit 0
fi
