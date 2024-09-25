FROM python:3.11

RUN apt update && \
    apt install -y git sudo

# Install gcloud CLI
RUN curl https://sdk.cloud.google.com > install.sh && bash install.sh --disable-prompts
ENV PATH $PATH:/root/google-cloud-sdk/bin

# Install zsh
RUN sh -c "$(wget -O- https://github.com/deluan/zsh-in-docker/releases/download/v1.2.0/zsh-in-docker.sh)" \
    -p git \
    -p ssh-agent \
    -p https://github.com/zsh-users/zsh-autosuggestions \
    -p https://github.com/zsh-users/zsh-completions \
    -p https://github.com/zsh-users/zsh-syntax-highlighting.git \
    -p git-auto-fetch && \
    chsh -s $(which zsh)

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
