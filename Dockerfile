# NOTE: Build with:
#   docker build --platform linux/amd64 . -t netbox-nmap-scan
ARG PLATFORM="linux/amd64"

### 1/3 Base stage ##############################################################
FROM --platform="${PLATFORM}" debian:stable-slim AS base

# Args in base stage are available in all stages that use it
ARG PYTHON_VERSION="3.12"

WORKDIR /app

# Install runtime OS dependencies:
# - ca-certificates: TLS trust store
# - nmap: scanner binary
RUN apt-get update \
 && apt-get upgrade -y \
 && apt-get install -y --no-install-recommends \
      ca-certificates \
      nmap \
 && rm -rf /var/lib/apt/lists/*

# Install uv (Python venv + runner)
# (Pinned image + digest to keep builds reproducible)
COPY --from=ghcr.io/astral-sh/uv:0.7.2-python3.12-bookworm@sha256:fc4cfd86ed92eed3c70f9bb33452f6e8cc65d31f72a7dedc602bb7a5ee6bf7aa \
  /usr/local/bin/uv /usr/local/bin/uvx /usr/local/bin/


### 2/3 Builder stage ###########################################################
FROM --platform="${PLATFORM}" base AS builder

ARG NETBOX_NMAP_SCAN_TAG="0.4.0"

# Build-time only dependency: git (not kept in final image)
RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*

# Clone the repository and checkout the desired tag
# Keep original var.ini as var_original.ini so the container expects a user-provided /app/var.ini
RUN mkdir -p /tmp/src \
 && git clone --branch main https://github.com/LoH-lu/netbox-nmap-scan.git /tmp/src \
 && cd /tmp/src \
 && git checkout "tags/${NETBOX_NMAP_SCAN_TAG}" \
 && cp -a . /app \
 && cd /app \
 && if [ -f var.ini ]; then mv var.ini var_original.ini; fi \
 && rm -rf /tmp/src


### 3/3 Final stage #############################################################
FROM --platform="${PLATFORM}" base AS final

ARG PYTHON_VERSION="3.12"

WORKDIR /app

# Copy the application code from the builder stage
COPY --from=builder /app /app

# Create venv and install Python deps
# If you removed tqdm/colorama from requirements.txt, this will naturally reflect it.
RUN uv python list \
 && uv venv --python "${PYTHON_VERSION}" \
 && uv pip install --no-cache-dir -r requirements.txt

# Optional: run as non-root (recommended)
# Create a dedicated user and ensure writable dirs for PREFIXES/ and logs/
RUN useradd --create-home --home-dir /home/appuser --shell /usr/sbin/nologin appuser \
 && mkdir -p /app/PREFIXES /app/logs \
 && chown -R appuser:appuser /app /home/appuser

USER appuser

# The app expects /app/var.ini to exist.
# Provide it by mounting a config file:
#   docker run --rm -v $(pwd)/var.ini:/app/var.ini:ro netbox-nmap-scan
CMD ["uv", "run", "main.py"]
