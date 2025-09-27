# CI/CD Strategy and Implementation

## 1. Overview

This document outlines the architecture, rationale, and implementation details of the continuous integration and continuous delivery (CI/CD) pipeline for this repository. The primary goals of this pipeline are to:

-   **Ensure Code Quality:** Automatically enforce coding standards, formatting, and type safety.
-   **Guarantee Stability:** Run a comprehensive test suite across multiple platforms and Python versions to prevent regressions.
-   **Enhance Security:** Automate security checks, including dependency and container vulnerability scanning.
-   **Improve Developer Velocity:** Provide fast feedback to developers and automate the build and verification process.

## 2. Technology Stack and Rationale

The CI/CD pipeline is built on a foundation of modern, industry-standard tools selected for their efficiency and robustness.

-   **Dependency Management:** **PDM (Python Development Master)** was chosen as the definitive dependency manager. It is specified in `pyproject.toml` and provides a single, reliable source of truth for project dependencies, avoiding the ambiguity of multiple lock files or `requirements.txt` files.
-   **CI/CD Platform:** **GitHub Actions** is used as the automation platform due to its tight integration with the source code repository, extensive marketplace of actions, and robust support for matrix builds and caching.
-   **Linting and Formatting:** **Ruff** is used for both linting and formatting, offering exceptional speed and a comprehensive set of rules. **Mypy** is used for static type checking. These are managed and enforced via `pre-commit`.
-   **Testing Framework:** **Pytest** is the testing framework, chosen for its powerful features, extensive plugin ecosystem, and clear test organization capabilities.
-   **Containerization:** **Docker** is used to create portable, secure, and reproducible application environments.
-   **Security Scanning:** **Trivy** is integrated to scan Docker images for known vulnerabilities, providing a critical layer of security.

## 3. Proactive Improvements Made

Upon initial analysis, several gaps were identified and proactively addressed to establish a best-in-class CI/CD foundation.

-   **Standardized on PDM:** Confirmed PDM as the sole dependency manager, ensuring a single, unambiguous workflow for managing dependencies. No "cruft" from other managers like Poetry or Pipenv was present, but this strategy prevents it from appearing in the future.
-   **Added Comprehensive `pre-commit` Configuration:** Created a `.pre-commit-config.yaml` from scratch. This file now automatically enforces code formatting (`ruff format`), linting (`ruff`), and type checking (`mypy`), ensuring all code committed to the repository meets quality standards.
-   **Implemented a Secure and Efficient `Dockerfile`:** A new, multi-stage `Dockerfile` was created. It uses a builder pattern to keep the final image minimal, creates a non-root user for security, and is optimized for the PDM ecosystem.
-   **Created a `.dockerignore` File:** A comprehensive `.dockerignore` file was added to minimize the Docker build context. This speeds up image builds and prevents sensitive information (like `.env` files) or development artifacts (like `.pytest_cache`) from being included in the final image.

## 4. Workflow Architecture

The CI/CD pipeline is split into two distinct, parallel workflows:

### `ci.yml` (Linting and Testing)

This workflow is designed for fast feedback. It consists of two jobs in a dependency chain:

1.  **`lint` Job:** This job runs first on an `ubuntu-latest` runner. It quickly checks out the code, sets up Python, and runs the entire `pre-commit` suite. If any formatting, linting, or type-checking errors are found, the job fails immediately, providing rapid feedback without wasting resources on running tests.
2.  **`test` Job:** This job only runs if the `lint` job succeeds (`needs: [lint]`). It performs a comprehensive test run across a matrix of operating systems (`ubuntu-latest`, `macos-latest`, `windows-latest`) and Python versions (`3.10`, `3.11`, `3.12`), ensuring broad compatibility.

### `docker.yml` (Container Build and Scan)

This workflow runs in parallel to `ci.yml` and focuses exclusively on the containerization aspect of the project.

1.  **`build-and-scan` Job:** This single job builds the Docker image defined in the `Dockerfile`. For pull requests, the image is built for verification but not pushed. It then uses Trivy to scan the locally-built image for `HIGH` or `CRITICAL` severity vulnerabilities, failing the build if any are found.

## 5. Testing Strategy

The testing strategy is designed for clarity and comprehensiveness.

-   **Test Separation:** Tests are organized into `tests/unit` and `tests/integration` directories. In the CI workflow, they are executed as separate steps using `pytest` markers (`-m "not integration"` and `-m "integration"`).
-   **Matrix Builds:** The test suite is executed across a matrix of 3 operating systems and 3 Python versions, for a total of 9 distinct test environments. This ensures that platform-specific issues are caught before they are merged. The `fail-fast: false` strategy allows all jobs in the matrix to complete, providing a full picture of compatibility even if one combination fails.
-   **Code Coverage:** Code coverage is generated for both unit and integration test runs and uploaded to **Codecov** separately. Each report is tagged with the operating system, Python version, and test type (e.g., `ubuntu-latest-py3.12-unit`), allowing for detailed analysis of coverage across different environments.

## 6. Dependency Management and Caching

-   **PDM Installation:** PDM is installed in the CI environment using `pipx`, which ensures it is available on the `PATH` in an isolated, reliable manner.
-   **Caching:** The `actions/setup-python` action is configured with `cache: "pdm"`. This leverages GitHub Actions' native caching mechanism to store installed dependencies, significantly speeding up subsequent workflow runs.

## 7. Security Hardening

Security is a core principle of this CI/CD pipeline, implemented through several layers:

-   **Principle of Least Privilege (PoLP):** Workflows are configured with `permissions: contents: read`. The `docker.yml` workflow is granted the additional `security-events: write` permission, which is required for Trivy to upload scan results to GitHub's security dashboard.
-   **Action Pinning:** All third-party GitHub Actions are pinned to their full commit SHA. This prevents malicious or breaking changes from being introduced automatically and ensures workflow reproducibility.
-   **Non-Root Docker Container:** The `Dockerfile` creates a dedicated, unprivileged user (`appuser`) to run the application, reducing the attack surface.
-   **Vulnerability Scanning:** The `docker.yml` workflow integrates Trivy to scan for OS and library vulnerabilities, failing the build if `HIGH` or `CRITICAL` issues are detected.
-   **Docker Hub Authentication:** The workflow securely authenticates with Docker Hub using secrets. This is done conditionally, so forked repositories without the secrets do not fail. This prevents rate-limiting issues when pulling base images.

## 8. Docker Strategy

-   **Multi-Stage Builds:** The `Dockerfile` uses a multi-stage build. A `builder` stage installs all dependencies using PDM. A clean `final` stage then copies only the application source code and the generated virtual environment, resulting in a minimal, production-ready image.
-   **Build Caching:** The `docker/build-push-action` is configured to use the GitHub Actions cache (`type=gha`), which significantly speeds up image builds by reusing layers from previous runs.
-   **Verification, Not Deployment:** On pull requests, the Docker image is built and scanned but is **not** pushed to a registry. This verifies that the `Dockerfile` is valid and the resulting image is secure without polluting the container registry.

## 9. How to Run Locally

Developers can replicate the CI checks locally to ensure their changes will pass before pushing.

-   **Linting:** To run the same checks as the `lint` job, install `pre-commit` and run it against all files:
    ```bash
    pip install pre-commit
    pre-commit install
    pre-commit run --all-files
    ```
-   **Testing:** To run the test suite, install the dependencies with PDM and use `pytest`:
    ```bash
    # Install dependencies, including dev dependencies
    pdm install

    # Run unit tests
    pdm run pytest -m "not integration"

    # Run integration tests
    pdm run pytest -m "integration"
    ```
-   **Docker Build:** To build the Docker image locally, use the standard `docker build` command:
    ```bash
    docker build -t py-load-eurostat:local .
    ```