# OpenHEXA Pipeline Templates

[![Templates CI](https://github.com/BLSQ/openhexa-templates-ds/actions/workflows/ci.yaml/badge.svg)](https://github.com/BLSQ/openhexa-templates-ds/actions/workflows/ci.yaml)

A collection of **OpenHEXA pipeline templates** created and maintained by the Bluesquare Data Services team.

---

## Continuous Integration (CI)

This repository uses **GitHub Actions** to ensure that all pipeline templates are valid and pass automated tests.

### Workflow: `ci.yaml`

**File:** [`.github/workflows/ci.yaml`](.github/workflows/ci.yaml)  
**Runs on:** every **push** and **pull request**  

#### What it does
1. Checks out the repository.
2. Sets up **Python 3.11**.
3. Pulls the latest OpenHEXA base environment:  
  ```bash
   docker pull blsq/openhexa-blsq-environment:latest
  ```
4. Runs the test suite via [`scripts/tests.sh`](./scripts/tests.sh).


## Test Process

### `scripts/tests.sh`

This script performs two key validation steps:

1. **Unit & Feature Tests**
   Runs `pytest` inside the OpenHEXA Docker environment to ensure all tests pass.

   ```bash
   docker run -t blsq/openhexa-blsq-environment:latest pytest
   ```

2. **Template Structure Validation**
   Runs [`scripts/pipeline_template_contents.sh`](./scripts/pipeline_template_contents.sh), which verifies that each valid pipeline folder contains the required files:

   * `pipeline.py`
   * `README.md`
   * `requirements.txt`

   Folders without a `pipeline.py` file are skipped automatically.

If any tests or structure checks fail, the workflow exits with a non-zero status, marking the CI run as failed.


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


## Local Testing

You can run the same checks locally before pushing:

```bash
# Run the same validations as the CI
./scripts/tests.sh
```


## CI Summary

| Step                      | Description                                       |
| ------------------------- | ------------------------------------------------- |
| **Build**              | Sets up Python and pulls the OpenHEXA environment |
| **Run Tests**          | Executes pytest inside Docker                     |
| **Validate Templates** | Ensures all required files exist per pipeline     |

All these steps run automatically on every push or pull request to maintain template quality and consistency.

---

**Maintained by:** Bluesquare Data Services Team

**Environment:** [`blsq/openhexa-blsq-environment:latest`](https://hub.docker.com/r/blsq/openhexa-blsq-environment)
