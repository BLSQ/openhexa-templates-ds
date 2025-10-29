# OpenHEXA Pipeline Templates

[![CI](https://github.com/BLSQ/openhexa-templates-ds/actions/workflows/ci.yaml/badge.svg)](https://github.com/BLSQ/openhexa-templates-ds/actions/workflows/ci.yaml)

A collection of **OpenHEXA pipeline templates** created and maintained by the **Bluesquare Data Services** team.

---

## Continuous Integration (CI)

This repository uses **GitHub Actions** to automatically validate and test all OpenHEXA pipeline templates.

### Workflow: `ci.yaml`

**File:** [`.github/workflows/ci.yaml`](.github/workflows/ci.yaml)  
**Triggers:** on every **push** and **pull request**

#### What the CI does

1. **Checks out** the repository.
2. **Runs asset validation** via [`scripts/pipeline_template_contents.sh`](./scripts/pipeline_template_contents.sh):
   - Ensures each valid pipeline folder contains:
     - `pipeline.py`
     - `README.md`
   - Skips hidden directories and folders without a `pipeline.py`.
3. **Runs tests with coverage** via [`scripts/templates_tests.sh`](./scripts/templates_tests.sh):
   - Detects the current branch and compares it against `origin/main`.
   - On feature branches, it **tests only directories that have changed**.
   - On `main`, it **tests all directories**.
   - For each directory:
     - Installs dependencies from `requirements.txt` (if available).
     - Runs all tests in the `tests/` subdirectory (if available).
     - Reports **coverage results** directly in the workflow logs.

---

## Scripts Overview

### `scripts/pipeline_template_contents.sh`

Ensures all pipeline template folders meet minimum structural requirements.

**Checks performed:**
- `pipeline.py` exists (case-insensitive)
- `README.md` exists (case-insensitive)
- Skips hidden directories and those without a `pipeline.py`.

If any required file is missing, the script lists all failing folders and exits with an error code, causing the CI to fail.

---

### `scripts/templates_tests.sh`

Runs **pytest with coverage** for changed or relevant pipeline directories.

#### How it works

1. Detects the **current branch** name.
2. Fetches the `main` branch to compare differences.
3. If on `main`:
   - Runs tests for **all** top-level directories.
4. If on a feature branch:
   - Tries to find a **merge base** between `origin/main` and `HEAD`.
   - If found → tests only changed directories.
   - If no merge base → falls back to testing all directories.
5. For each target directory:
   - Installs dependencies (`requirements.txt`) if present.
   - Runs `pytest` and shows coverage report (`--cov-report=term`).
   - Skips directories without tests gracefully.

#### Example output

```

📂 On feature branch — testing only changed directories...
📁 Directories to test:
template_a
template_b
----------

📂 Processing directory: template_a
📦 Installing dependencies for template_a...
🧪 Running tests for template_a...

```

If no directories have relevant changes:
```

✅ No relevant changes detected. Exiting...

````

---

## Running Locally

You can reproduce the CI checks on your local machine:

```bash
# Validate pipeline structure
./scripts/pipeline_template_contents.sh

# Run selective or full test suite (depending on branch)
./scripts/templates_tests.sh
````

You can also run tests manually for a single template:

```bash
cd template_a
pytest --cov=. --cov-report=term
```

---

## Repository Structure

A typical layout of the repository:

```
openhexa-templates-ds/
├── template_a/
│   ├── pipeline.py
│   ├── requirements.txt
│   ├── tests/
│   └── README.md
├── template_b/
│   ├── pipeline.py
│   ├── requirements.txt
│   └── README.md
├── scripts/
│   ├── pipeline_template_contents.sh
│   └── templates_tests.sh
└── .github/
    └── workflows/
        └── ci.yaml
```

---

## CI Summary

| Step                | Description                                                                           |
| ------------------- | ------------------------------------------------------------------------------------- |
| **Run Assets**      | Validates that all template folders have required files (`pipeline.py`, `README.md`). |
| **Run Tests**       | Executes `pytest` for changed or all directories, depending on the branch.            |
| **Coverage Report** | Prints coverage results in CI logs for each tested directory.                         |

All steps run automatically on every push or pull request to ensure template quality and maintainability.

---

**Maintained by:** [Bluesquare Data Services Team](https://www.bluesquarehub.com/)
**Environment:** [`blsq/openhexa-blsq-environment:latest`](https://hub.docker.com/r/blsq/openhexa-blsq-environment)
