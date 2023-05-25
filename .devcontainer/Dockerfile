FROM python:3.11

RUN apt update && \
    apt install -y git sudo

# Install gcloud CLI
RUN curl https://sdk.cloud.google.com > install.sh && bash install.sh --disable-prompts
ENV PATH $PATH:/root/google-cloud-sdk/bin

## Pip dependencies
RUN pip install --upgrade pip

# Install dependencies
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Set up pre-commit
COPY .git .git
COPY .pre-commit-config.yaml .pre-commit-config.yaml
RUN git config --global --add safe.directory /workspaces/dbt-beyond-the-basics
RUN pre-commit install-hooks

# Install development dependencies
COPY requirements_dev.txt requirements_dev.txt
RUN pip install -r requirements_dev.txt

COPY . .
