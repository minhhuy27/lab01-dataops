FROM python:3.9-bullseye

WORKDIR /usr/app/dbt

# Install system dependencies
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    curl \
    gnupg2 \
    git \
    && rm -rf /var/lib/apt/lists/*

# Add Microsoft repo and install ODBC Driver 18 (using Debian bullseye support)
RUN set -eux; \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        curl ca-certificates gnupg apt-transport-https unixodbc && \
    KEYRING=/usr/share/keyrings/microsoft-prod.gpg; \
    mkdir -p "$(dirname "$KEYRING")" /etc/apt/sources.list.d; \
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
      | gpg --dearmor --batch --yes -o "$KEYRING"; \
    echo "deb [arch=amd64 signed-by=${KEYRING}] https://packages.microsoft.com/debian/11/prod bullseye main" \
      > /etc/apt/sources.list.d/mssql-release.list; \
    apt-get update && \
    ACCEPT_EULA=Y DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        msodbcsql18 && \
    rm -rf /var/lib/apt/lists/*


# Install specific version of dbt-core and related packages
RUN pip install --no-cache-dir \
    dbt-core==1.8.7 \
    dbt-sqlserver==1.8.7

# Copy dbt project files
COPY . .

# Initialize dbt project (with no packages)
RUN touch packages.yml && dbt deps 
