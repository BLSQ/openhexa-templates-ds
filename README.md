
# OpenHEXA Pipeline Templates

[![CI](https://github.com/BLSQ/openhexa-templates-ds/actions/workflows/ci.yaml/badge.svg)](https://github.com/BLSQ/openhexa-templates-ds/actions/workflows/ci.yaml)
[![Cov](https://BLSQ.github.io/openhexa-templates-ds/badges/coverage.svg)](https://github.com/BLSQ/openhexa-templates-ds/actions)


A collection of **OpenHEXA pipeline templates** created and maintained by the Bluesquare Data Services team.

---

## Continuous Integration (CI)

This repository uses **GitHub Actions** to automatically validate and test all OpenHEXA pipeline templates.

### Workflow: `ci.yaml`

**File:** [`.github/workflows/ci.yaml`](.github/workflows/ci.yaml)  
**Runs on:** every **push** and **pull request**

#### What it does
1. **Checks out** the repository.
2. **Pulls the latest OpenHEXA base environment**:  
  ```bash
   docker pull blsq/openhexa-blsq-environment:latest
  ```
3. **Runs the test suite** via [`scripts/tests.sh`](./scripts/tests.sh), which executes all validations.

---

## Test Process

### `scripts/tests.sh`

This script performs two main validation steps:

1. **Unit & Feature Tests**
   Runs `pytest` inside the OpenHEXA Docker environment to ensure all test cases pass.

   ```bash
   docker run -t blsq/openhexa-blsq-environment:latest pytest
   ```

2. **Template Structure Validation**
   Runs [`scripts/pipeline_template_contents.sh`](./scripts/pipeline_template_contents.sh) to ensure each valid pipeline folder contains:

   * `pipeline.py`
   * `README.md`
   * `requirements.txt`

   Folders **without a `pipeline.py` file** are automatically skipped.

If any test or structure check fails, the workflow exits with a non-zero status, marking the CI run as failed.

---

## Repository Structure

Typical layout:

```
openhexa-templates-ds/
├── template_a/
│   ├── pipeline.py
│   ├── requirements.txt
│   └── README.md
├── template_b/
│   ├── pipeline.py
│   ├── requirements.txt
│   └── README.md
├── scripts/
│   ├── tests.sh
│   └── pipeline_template_contents.sh
└── .github/
    └── workflows/
        └── ci.yaml
```

---

## Local Testing

You can run the same checks locally before pushing:

```bash
# Run the same validations as the CI
./scripts/tests.sh
```

---

## CI Summary

| Step                   | Description                                              |
| ---------------------- | -------------------------------------------------------- |
| **Build**              | Pulls the OpenHEXA environment |
| **Run Tests**          | Executes pytest inside Docker                            |
| **Validate Templates** | Ensures all required files exist per pipeline            |

All steps run automatically on every push or pull request to maintain template quality and consistency.

---

**Maintained by:** Bluesquare Data Services Team

**Environment:** [`blsq/openhexa-blsq-environment:latest`](https://hub.docker.com/r/blsq/openhexa-blsq-environment)
