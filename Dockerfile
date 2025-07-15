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
# Install uv (Python virtual environment manager) - https://github.com/astral-sh/uv/releases - linux/amd64
COPY --from=ghcr.io/astral-sh/uv:0.7.2-python3.12-bookworm@sha256:fc4cfd86ed92eed3c70f9bb33452f6e8cc65d31f72a7dedc602bb7a5ee6bf7aa /usr/local/bin/uv /usr/local/bin/uvx /usr/local/bin/


### 2/3 Builder stage ###
# Add GIT we dont need it in the final image
FROM --platform="${PLATFORM}" base AS builder
# Add GIT as build dependency, not in final image
RUN apt-get update && apt-get install -y \
    git 
# Clone the repository (main branch, adjust for specific tag or commit)
RUN mkdir git && cd ./git && \
    git clone --branch main https://github.com/LoH-lu/netbox-nmap-scan.git . && \
    git checkout tags/${NETBOX_NMAP_SCAN_TAG} && \
    cd .. && \
    mv git/* . && \
    # Remove default config file
    mv var.ini var_original.ini && \
    rm -rf git


### 3/3 Final stage ###
FROM --platform="${PLATFORM}" base AS final
#
WORKDIR /app
# Copy the application code from the builder stage
COPY --from=builder /app /app
# Install Python requirements and create a virtual environment
RUN uv python list && \
    uv venv --python "${PYTHON_VERSION}" && \
    uv pip install -r requirements.txt
#
# Command to run the application (adjust based on actual entry point)
CMD ["uv", "run", "main.py"]
