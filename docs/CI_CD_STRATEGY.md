# CI/CD Strategy and Architecture

## 1. Overview

The goal of this CI/CD pipeline is to establish a robust, secure, and efficient automated workflow for the `py_load_eurostat` project. The implementation focuses on best-in-class practices for Python development, including standardized dependency management, comprehensive static analysis, multi-platform testing, and container security scanning.

This document outlines the technology choices, the architecture of the workflows, and the security measures that have been put in place.

## 2. Technology Stack and Rationale

### Dependency Management: PDM

- **Decision:** The project has been standardized on **PDM (Python Development Master)**.
- **Rationale:** The repository already contained a `pdm.lock` file and `pyproject.toml` configured for PDM. To ensure a single source of truth and avoid dependency conflicts, PDM was chosen as the definitive tool. It is a modern, all-in-one tool that handles dependency resolution, virtual environment management, and packaging.

### Linting and Formatting: Ruff and Mypy

- **Decision:** Code quality is enforced using `ruff` and `mypy`, integrated via `pre-commit`.
- **Rationale:**
    - **Ruff** is an extremely fast linter and formatter that replaces multiple legacy tools (like Flake8, isort, and Black) with a single, high-performance binary.
    - **Mypy** is the standard for static type checking in Python, ensuring type safety and reducing runtime errors.

### Containerization: Docker

- **Decision:** A multi-stage `Dockerfile` is used to create a production-ready container image.
- **Rationale:** Docker provides a consistent, isolated environment for running the application. The multi-stage build strategy ensures the final image is minimal, containing only the application code and its production dependencies, which enhances security and reduces the image size.

### Security Scanning: Trivy

- **Decision:** The Docker image is scanned for vulnerabilities using **Trivy**.
- **Rationale:** Trivy is a comprehensive and easy-to-use open-source scanner that detects vulnerabilities in OS packages and application dependencies. Integrating it into the CI pipeline ensures that security issues are caught automatically before they reach production.

## 3. Proactive Improvements Made

The initial repository was missing several critical CI/CD components. The following files and configurations were created to address these gaps:

1.  **`.pre-commit-config.yaml`:** A comprehensive pre-commit configuration was created to automate code quality checks. It includes hooks for:
    - **Repository Hygiene:** Standard checks for file endings, YAML/TOML syntax, and merge conflicts.
    - **Linting & Formatting:** `ruff` and `ruff-format` to enforce a consistent code style.
    - **Type Checking:** `mypy` to enforce static type safety.

2.  **`Dockerfile`:** A secure, multi-stage `Dockerfile` was added. Key features include:
    - **Multi-Stage Build:** A builder stage installs PDM and exports a `requirements.txt` file, ensuring that PDM itself is not included in the final image.
    - **Non-Root User:** The application runs as an unprivileged `appuser` to enhance security.
    - **Lean Base Image:** Uses `python:3.12-slim-bookworm` for a smaller attack surface.

3.  **`.dockerignore`:** A `.dockerignore` file was created to exclude `.git`, `.venv`, `__pycache__`, and other unnecessary files from the Docker build context, which improves build speed and security.

4.  **GitHub Actions Workflows:** Two new workflow files were created in `.github/workflows/`.

## 4. Workflow Architecture

The CI/CD process is split into two parallel workflows: `ci.yml` and `docker.yml`.

### `ci.yml` (Linting and Testing)

This workflow ensures code quality and correctness. It runs on `push` and `pull_request` events.

- **Job 1: `lint`**
    - **Purpose:** Provides fast feedback on code style and static analysis.
    - **Process:** Runs on `ubuntu-latest` and executes all checks defined in `.pre-commit-config.yaml` using the `pre-commit/action`.

- **Job 2: `test`**
    - **Purpose:** Verifies that the application works correctly across different environments.
    - **Dependency:** This job `needs: lint` and will only run if the linting job succeeds.
    - **Matrix Strategy:** It runs on a matrix of:
        - **Operating Systems:** `ubuntu-latest`, `macos-latest`, `windows-latest`
        - **Python Versions:** `3.10`, `3.11`, `3.12`
    - **Process:**
        1.  Installs PDM and project dependencies using PDM's built-in caching.
        2.  Runs unit tests (`tests/unit`) and integration tests (`tests/integration`) separately using `pytest`.
        3.  Generates distinct coverage reports (`coverage-unit.xml`, `coverage-integration.xml`).
        4.  Uploads coverage reports to Codecov with flags identifying the OS, Python version, and test type.

### `docker.yml` (Build and Scan)

This workflow ensures the container is buildable and secure. It also runs on `push` and `pull_request` events.

- **Job: `build_and_scan`**
    - **Purpose:** To build a production-like Docker image and scan it for security vulnerabilities.
    - **Process:**
        1.  Authenticates with Docker Hub to avoid pull rate limits.
        2.  Builds the Docker image using the `docker/build-push-action`. The image is loaded into the local daemon but **not pushed** to a registry.
        3.  Uses the `aquasecurity/trivy-action` to scan the image for `HIGH` and `CRITICAL` vulnerabilities. The workflow will fail if any are found.

## 5. Security Hardening

Security is a core component of this CI/CD strategy, implemented through several measures:

- **Principle of Least Privilege (PoLP):** All workflow jobs are configured with `permissions: contents: read` to ensure they only have the minimum access required.
- **Action Pinning:** All third-party GitHub Actions are pinned to their full commit SHA to prevent supply chain attacks from a compromised tag.
- **Non-Root Docker Container:** The `Dockerfile` creates and runs the application as a non-root user (`appuser`).
- **Vulnerability Scanning:** The `docker.yml` workflow automatically scans every image build with Trivy, failing the build if critical vulnerabilities are detected.
- **Dependency Management:** Using a lock file (`pdm.lock`) ensures that dependency versions are pinned, providing reproducible builds.

## 6. How to Run Locally

To ensure consistency between local development and the CI environment, developers should run the following checks before pushing code.

### Running Pre-Commit Checks

1.  **Install pre-commit:**
    ```bash
    pip install pre-commit
    ```
2.  **Install the git hooks:**
    ```bash
    pre-commit install
    ```
3.  **Run all checks manually:**
    ```bash
    pre-commit run --all-files
    ```

### Running Tests

1.  **Install PDM and dependencies:**
    ```bash
    pip install pdm
    pdm install -d
    ```
2.  **Run the test suite:**
    ```bash
    # Run all tests
    pdm run pytest

    # Run only unit tests
    pdm run pytest tests/unit

    # Run only integration tests
    pdm run pytest tests/integration
    ```

### Building the Docker Image

To build the Docker image locally, run the following command from the root of the repository:

```bash
docker build -t py-load-eurostat .
```