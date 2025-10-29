#!/usr/bin/env bash
set -euo pipefail

# Define required files (reference names only, case-insensitive check will be done)
required_files=("pipeline.py" "README.md")

# Get repo root (script's parent directory)
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Checking folders under $repo_root..."

# Track failing folders
failing_folders=()

# Loop through top-level directories
for dir in "$repo_root"/*/; do
    folder_name=$(basename "$dir")

    # Skip hidden folders (e.g., .git, .venv)
    if [[ $folder_name == .* ]]; then
        continue
    fi

    # Skip if the folder does not contain pipeline.py (case-insensitive)
    if ! find "$dir" -maxdepth 1 -type f -iname "pipeline.py" | grep -q .; then
        echo "⏭️  Skipping $folder_name (no pipeline.py found)"
        continue
    fi

    echo "📂 Checking $folder_name..."

    missing=0
    for file in "${required_files[@]}"; do
        # Use find with -iname for case-insensitive matching
        if ! find "$dir" -maxdepth 1 -type f -iname "$file" | grep -q .; then
            echo "   ❌ Missing: $file"
            missing=1
        else
            echo "   ✅ Found (case-insensitive): $file"
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
    echo "🎉 All checked folders passed the check."
    exit 0
fi
