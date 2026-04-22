# syntax=docker/dockerfile:1.4
FROM mcr.microsoft.com/devcontainers/python:3.11

ARG NODE_VERSION=20.19.0
ENV NVM_DIR=/usr/local/share/nvm

# Keep all system dependencies in one build layer so the container avoids the slow feature stage.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    bash -euxo pipefail -c " \
        export DEBIAN_FRONTEND=noninteractive; \
        apt-get update; \
        apt-get install -y --no-install-recommends \
            apt-transport-https \
            build-essential \
            ca-certificates \
            curl \
            dirmngr \
            gnupg \
            lsb-release \
            unixodbc-dev; \
        gpg --batch --keyserver keyserver.ubuntu.com --recv-keys EB3E94ADBE1229CF; \
        gpg --batch --export EB3E94ADBE1229CF | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg; \
        echo 'deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/ubuntu/22.04/prod jammy main' > /etc/apt/sources.list.d/mssql-release.list; \
        echo 'deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/repos/azure-cli/ jammy main' > /etc/apt/sources.list.d/azure-cli.list; \
        apt-get update; \
        ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
            azure-cli \
            msodbcsql18; \
        apt-get clean; \
        rm -rf /var/lib/apt/lists/* \
    "

# Pre-install Python packages so post-create.sh doesn't have to download them.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --prefer-binary -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

RUN su vscode -c "bash -lc 'source /usr/local/share/nvm/nvm.sh && nvm install ${NODE_VERSION} && nvm alias default ${NODE_VERSION} && nvm use default'"
