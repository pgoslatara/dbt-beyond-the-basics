ARG PYTHON_VERSION

FROM python:${PYTHON_VERSION}-slim-bullseye AS base

ENV PYTHONUNBUFFERED=1 \
    # prevents python creating .pyc files
    PYTHONDONTWRITEBYTECODE=1 \
    # make poetry install to this location
    POETRY_HOME="/opt/poetry" \
    # do not ask any interactive question
    POETRY_NO_INTERACTION=1 \
    PYSETUP_PATH="/opt/pysetup" \
    VENV_PATH="/opt/pysetup/.venv" \
    # DBT env vars
    DBT_PROFILES_DIR="." \
    DBT_PROJECT_DIR="." \
    DBT_DATASET="Overwrite at runtime"

# Prepend poetry and venv to path
ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"


FROM base AS builder

# Install OS dependencies
RUN apt-get update && \
    apt-get install -qq -y curl --fix-missing --no-install-recommends && \
    curl -sSL https://install.python-poetry.org | python3 - --version 2.0.1 && \
    rm -rf /var/lib/apt/lists/*
ENV PATH="$PATH:$POETRY_HOME/bin"

# Copy Poetry files
WORKDIR /dbt
COPY ./poetry.lock ./pyproject.toml ./
RUN poetry config virtualenvs.create true && \
    poetry config virtualenvs.in-project true && \
    poetry install --no-cache --no-interaction --no-root --only main && \
    rm -rf ~/.cache/pypoetry/artifacts


FROM base AS dbt

# Copy in pre-built .venv
COPY --from=builder /dbt/.venv /dbt/.venv

# Update OS dependencies
RUN apt-get update && \
    rm -rf /var/lib/apt/lists/*

# Copy repo code
COPY . .

WORKDIR /
RUN ln -s /dbt/.venv/bin/dbt /bin/dbt && \
    dbt deps && \
    dbt parse

CMD ["/bin/bash", "-c", "echo 'Expecting commands to be passed in.' && exit 1"]
