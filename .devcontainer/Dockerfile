FROM python:3.11.10-slim-bullseye

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
ENV PATH="$PATH:/root/google-cloud-sdk/bin"

# Install necessary packages
RUN apt update && \
    apt install -y --no-install-recommends gcc git g++ libffi-dev wget sudo && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    apt-get install apt-transport-https ca-certificates gnupg curl -y && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt-get update && apt-get install google-cloud-sdk -y && \
    rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install zsh
RUN sh -c "$(wget -O- https://github.com/deluan/zsh-in-docker/releases/download/v1.2.0/zsh-in-docker.sh)" \
    -p git \
    -p ssh-agent \
    -p https://github.com/zsh-users/zsh-autosuggestions \
    -p https://github.com/zsh-users/zsh-completions \
    -p https://github.com/zsh-users/zsh-syntax-highlighting.git \
    -p git-auto-fetch && \
    chsh -s $(which zsh)

## Install dependencies
COPY uv.lock pyproject.toml ./
RUN uv sync --frozen
ENV PATH="$PATH:.venv/bin"

# Set up pre-commit
COPY .pre-commit-config.yaml .pre-commit-config.yaml
COPY .git .git
RUN git config --global --add safe.directory /workspaces/dbt-beyond-the-basics && \
    uv run pre-commit install-hooks

COPY . .
