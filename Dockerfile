# ---- Builder Stage: Exports production dependencies to requirements.txt ----
FROM python:3.12-slim-bookworm AS builder

# Prevent Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1
# Ensure Python output is sent straight to the terminal without buffering.
ENV PYTHONUNBUFFERED=1

# Install PDM for dependency management.
RUN pip install --no-cache-dir pdm

# Set the working directory.
WORKDIR /app

# Copy dependency definition files.
COPY pyproject.toml pdm.lock ./

# Export production-only dependencies to a requirements.txt file.
# This avoids installing PDM in the final image.
RUN pdm export -o requirements.txt --prod --without-hashes


# ---- Final Stage: Creates the final, lean production image ----
FROM python:3.12-slim-bookworm

# Prevent Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1
# Ensure Python output is sent straight to the terminal without buffering.
ENV PYTHONUNBUFFERED=1

# Create a non-root user to run the application.
RUN useradd --create-home --shell /bin/bash appuser

# Set the working directory.
WORKDIR /home/appuser

# Copy the requirements.txt from the builder stage.
COPY --from=builder /app/requirements.txt .

# Install the production dependencies as the non-root user.
RUN pip install --no-cache-dir --user -r requirements.txt

# Copy the application source code and project definition.
COPY --chown=appuser:appuser src/py_load_eurostat ./py_load_eurostat
COPY --chown=appuser:appuser pyproject.toml .

# Install the application itself. This will create the command-line entrypoint.
RUN pip install --no-cache-dir --user .

# Add the user's local bin directory to the PATH.
ENV PATH="/home/appuser/.local/bin:${PATH}"

# Switch to the non-root user.
USER appuser

# Set the default command to run when the container starts.
ENTRYPOINT ["py-load-eurostat"]