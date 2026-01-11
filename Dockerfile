ARG PYTHON_VERSION=3.11

FROM python:${PYTHON_VERSION}-slim-bullseye AS base

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV PYTHONUNBUFFERED=1 \
    # prevents python creating .pyc files
    PYTHONDONTWRITEBYTECODE=1 \
    # DBT env vars
    DBT_PROFILES_DIR="." \
    DBT_PROJECT_DIR="." \
    DBT_DATASET="Overwrite at runtime"

FROM base AS builder

WORKDIR /dbt

# Copy uv files
COPY uv.lock pyproject.toml ./

# Install dependencies
RUN uv sync --frozen --no-dev


FROM base AS dbt

# Copy in pre-built .venv
COPY --from=builder /dbt/.venv /dbt/.venv

# Update OS dependencies
RUN apt-get update && \
    rm -rf /var/lib/apt/lists/*

# Copy repo code
COPY . .

WORKDIR /

# Add venv to PATH
ENV PATH="/dbt/.venv/bin:$PATH"

RUN dbt deps && \
    dbt parse

CMD ["/bin/bash", "-c", "echo 'Expecting commands to be passed in.' && exit 1"]
