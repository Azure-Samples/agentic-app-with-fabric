FROM mcr.microsoft.com/devcontainers/python:3.11

# Install ODBC Driver 18 for SQL Server (required by pyodbc / langchain-sqlserver)
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gnupg unixodbc-dev \
    && curl -sSL https://packages.microsoft.com/keys/microsoft.asc \
       | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg arch=amd64,arm64] \
       https://packages.microsoft.com/debian/12/prod bookworm main" \
       > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
