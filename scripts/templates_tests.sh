set -e

# Mark the repo as safe for Git operations
git config --global --add safe.directory "$(pwd)"

echo "🔍 Determining directories to test..."

# Identify the current branch name
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo "Current branch: $CURRENT_BRANCH"

# Fetch main branch for comparison
git fetch origin main

if [ "$CURRENT_BRANCH" = "main" ]; then
echo "📘 On main branch — testing all top-level directories..."
changed_dirs=$(find . -mindepth 1 -maxdepth 1 -type d ! -name '.git' | sed 's|^\./||')
else
echo "📂 On feature branch — testing only changed directories..."

# Try to find a merge base safely
if git merge-base --is-ancestor origin/main HEAD 2>/dev/null || git merge-base origin/main HEAD >/dev/null 2>&1; then
    changed_dirs=$(git diff --name-only origin/main...HEAD | awk -F/ '{print $1}' | sort -u)
else
    echo "⚠️ No merge base found — defaulting to all directories."
    changed_dirs=$(find . -mindepth 1 -maxdepth 1 -type d ! -name '.git' | sed 's|^\./||')
fi
fi

if [ -z "$changed_dirs" ]; then
    echo "✅ No relevant changes detected. Exiting..."
    exit 0
fi

echo "📁 Directories to test:"
echo "$changed_dirs"

for dir in $changed_dirs; do
    if [ -d "$dir" ]; then
        echo "-----------------------------"
        echo "📂 Processing directory: $dir"
        
        # Install dependencies if requirements.txt exists
        if [ -f "$dir/requirements.txt" ]; then
        echo "📦 Installing dependencies for $dir..."
        pip install -r "$dir/requirements.txt"
        else
        echo "⚠️ No requirements.txt found in $dir, skipping dependency installation."
        fi

        # Run pytest if tests folder exists
        if [ -d "$dir/tests" ]; then
        echo "🧪 Running tests for $dir..."
        cd "$dir"
        export PYTHONPATH=`pwd`
        pytest --cov=. --cov-report=term
        cd -
        else
        echo "⚠️ No tests directory found in $dir, skipping tests."
        fi
    fi
done