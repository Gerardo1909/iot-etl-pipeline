## ------------------------------- Builder Stage ------------------------------ ## 
FROM python:3.13-bookworm AS builder

RUN apt-get update && apt-get install --no-install-recommends -y \
        build-essential && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Download the latest installer, install it and then remove it
ADD https://astral.sh/uv/install.sh /install.sh
RUN chmod -R 655 /install.sh && /install.sh && rm /install.sh

# Set up the UV environment path correctly
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /app

COPY ./pyproject.toml .

# Sync only dependencies (not the project itself, which needs src/)
RUN uv sync --group dev --no-install-project

## ------------------------------- Production Stage ------------------------------ ##
FROM python:3.13-slim-bookworm AS production

# Install Java (required for PySpark) - using headless JRE for minimal size
RUN apt-get update && apt-get install --no-install-recommends -y \
        openjdk-17-jre-headless \
        procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Create non-root user for security
RUN useradd --create-home appuser

WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /app/.venv .venv

# Copy application code and tests
COPY --chown=appuser:appuser ./src ./src
COPY --chown=appuser:appuser ./tests ./tests
COPY --chown=appuser:appuser ./config ./config
COPY --chown=appuser:appuser ./pytest.ini ./pytest.ini

# Create data directories with proper permissions
RUN mkdir -p data/raw data/processed data/output && \
    chown -R appuser:appuser /app

USER appuser

# Set up environment variables
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app/src:${PYTHONPATH}"

# PySpark configurations for container environment
ENV PYSPARK_PYTHON=/app/.venv/bin/python
ENV PYSPARK_DRIVER_PYTHON=/app/.venv/bin/python