# ==============================================================================
# Builder Stage
#
# This stage installs dependencies using PDM, creating a self-contained
# virtual environment. This keeps the final image clean and minimal.
# ==============================================================================
ARG PYTHON_VERSION=3.12
ARG BASE_IMAGE=python:${PYTHON_VERSION}-slim-bookworm

FROM ${BASE_IMAGE} AS builder

# Set environment variables for non-interactive installs and to manage paths
ENV PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    PDM_USE_VENV=1 \
    PATH="/app/.venv/bin:$PATH"

# Create a non-root user and group for security
RUN groupadd --system --gid 1001 appgroup && \
    useradd --system --uid 1001 --gid appgroup appuser

# Create and set permissions for the application directory
WORKDIR /app
COPY --chown=appuser:appgroup . /app

# Install PDM using pipx for a clean, isolated installation
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install pipx && \
    pipx install pdm

# Install project dependencies into a virtual environment
# The --prod flag ensures that dev dependencies are not installed.
RUN --mount=type=cache,target=/root/.pdm_cache \
    pdm install --prod --no-editable

# ==============================================================================
# Final Stage
#
# This stage creates the final, lean production image. It copies the
# virtual environment and source code from the builder stage and runs the
# application as a non-root user.
# ==============================================================================
FROM ${BASE_IMAGE} AS final

# Set environment variables for the final image
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

# Create the same non-root user and group as in the builder stage
RUN groupadd --system --gid 1001 appgroup && \
    useradd --system --uid 1001 --gid appgroup appuser

# Copy the virtual environment and source code from the builder stage
WORKDIR /app
COPY --from=builder --chown=appuser:appgroup /app/.venv /app/.venv
COPY --from=builder --chown=appuser:appgroup /app/src /app/src

# Set the user to the non-root user
USER appuser

# Define the entrypoint for the container
ENTRYPOINT ["py-load-eurostat"]
CMD ["--help"]