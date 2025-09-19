# OpenHEXA Pipeline Templates

[![Base Docker CI](https://github.com/BLSQ/openhexa-templates-ds/actions/workflows/base_docker.yaml/badge.svg)](https://github.com/BLSQ/openhexa-templates-ds/actions/workflows/base_docker.yaml)
[![Templates CI](https://github.com/BLSQ/openhexa-templates-ds/actions/workflows/ci.yaml/badge.svg)](https://github.com/BLSQ/openhexa-templates-ds/actions/workflows/ci.yaml)


A collection of OpenHEXA pipeline templates created and maintained by Bluesquare Data Services team.


Here’s the complete **`CI_GUIDE.md`** file (Markdown) with workflow badges included. You can link to this file from your `README.md` or merge it directly into your README.


# ⚙️ Continuous Integration (CI) Guide

This repository comes with **GitHub Actions CI/CD workflows** that help us automate testing.  
If you’re new to CI, this section walks you through how things work here.

---

## 1. Workflows Overview

We have two workflows inside [`.github/workflows/`](.github/workflows/):

### 🐳 `base_docker.yaml`
- **When it runs**: On every push to the `main` branch.  
- **What it does**:
  1. Checks if the file [`Dockerfile.base`](./Dockerfile.base) was modified in the push.
  2. If yes:
     - Logs in to Docker Hub using repository secrets:
       - `DOCKERHUB_USERNAME`
       - `DOCKERHUB_PASSWORD` (or personal access token).
     - Runs [`scripts/update_base_docker_image.sh`](./scripts/update_base_docker_image.sh)  
       which builds and pushes the base CI Docker image to Docker Hub (`dalmasbluesquarehub/templates-ci:latest`).

👉 The **base image** (`Dockerfile.base`) is meant for dependencies that **rarely change** (Python version, system libs, etc.).

---

### ✅ `ci.yaml`
- **When it runs**: On every push or pull request.  
- **What it does**:
  1. Pulls the latest base image from Docker Hub (`dalmasbluesquarehub/templates-ci:latest`).
  2. Runs [`scripts/tests.sh`](./scripts/tests.sh) which:
     - Builds a test image using the repo’s [`Dockerfile`](./Dockerfile).
     - Runs **pytest** for unit and feature tests.
     - Verifies that required files (`pipeline.py`, `README.md`, `requirements.txt`) exist in all top-level project folders (see [`scripts/pipeline_template_contents.sh`](./scripts/pipeline_template_contents.sh)).

👉 This ensures that any change to templates or code is automatically tested.

---

## 2. Dockerfiles in the Repo

- **[`Dockerfile.base`](./Dockerfile.base)**  
  Defines the **base CI image** with Python 3.11 and system libraries.  
  Gets built and pushed to Docker Hub only if this file changes.

- **[`Dockerfile`](./Dockerfile)**  
  Defines the **project test image** built on top of the base image.  
  Includes Python requirements and project code.  
  Used during test runs.

---

## 3. Secrets Setup (required for Docker pushes)

For the workflow to push to Docker Hub, add these secrets in your repo:

1. Go to **Settings → Secrets and variables → Actions**.  
2. Add:
   - `DOCKERHUB_USERNAME` → your Docker Hub username  
   - `DOCKERHUB_PASSWORD` → your Docker Hub password or [Docker Hub Personal Access Token](https://docs.docker.com/security/for-developers/access-tokens/)

---

## 4. Local Testing (optional)

You can run the same scripts locally before pushing:

```bash
# Run the test pipeline locally
./scripts/tests.sh

# Update and push base Docker image (requires Docker login)
./scripts/update_base_docker_image.sh
````

---

## 5. CI Flow Summary

1. **Change application code** → `ci.yaml` runs tests ✅
2. **Change `Dockerfile.base`** → `base_docker.yaml` rebuilds and pushes base image 🐳

This setup makes sure:

* Tests run automatically on every push/PR.
* The base Docker image is only rebuilt when needed.

---

