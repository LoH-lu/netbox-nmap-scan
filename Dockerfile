# NOTE: Build with $ docker build --platform linux/amd64 . -t netbox-nmap-scan
ARG PLATFORM="linux/amd64"

### 1/3 Base stage ###
FROM --platform="${PLATFORM}" debian:stable-slim  AS base
# Args in base stage are available in all stages that use it
ARG NETBOX_NMAP_SCAN_TAG="0.3.7"
ARG PYTHON_VERSION="3.12"
#
WORKDIR /app
#
# Install shared packages and do image upgrade in base stage
RUN apt-get update && apt-get upgrade -y && apt-get install -y \
    ca-certificates \
    nmap \
    && rm -rf /var/lib/apt/lists/*

### 2/3 Builder stage ###
FROM --platform="${PLATFORM}" base AS builder
#
# Install UV
COPY --from=ghcr.io/astral-sh/uv:0.5.20 /uv /bin/uv
#
# Copy files needed for dependency installation
COPY requirements.txt /app/
#
# Create virtual environment and install dependencies
RUN uv python list && \
    uv venv --python "${PYTHON_VERSION}" && \
    uv pip install -r requirements.txt
#
# Copy the application code
COPY . /app

### 3/3 Final stage ###
FROM --platform="${PLATFORM}" base AS final
#
WORKDIR /app
# Copy the application code from the builder stage
COPY --from=builder /app /app
# Sanity check: scripts package must exist
RUN test -f /app/scripts/__init__.py
# Install Python requirements and create a virtual environment
RUN uv python list && \
    uv venv --python "${PYTHON_VERSION}" && \
    uv pip install -r requirements.txt
#
# Command to run the application (adjust based on actual entry point)
CMD ["uv", "run", "main.py"]
