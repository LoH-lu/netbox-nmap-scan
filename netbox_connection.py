#!/usr/bin/env python3
"""
Netbox API connection helper.

Library module:
- does not configure logging handlers
- uses logger = logging.getLogger(__name__)
"""

from __future__ import annotations

import logging

import pynetbox
import requests

logger = logging.getLogger(__name__)


def connect_to_netbox(url: str, token: str) -> pynetbox.api:
    """
    Connect to a Netbox API and validate /api/status/ is reachable.

    Security note:
        session.verify=False matches current behavior; prefer enabling SSL verification
        if you can deploy the proper CA chain.
    """
    base_url = url.rstrip("/")

    session = requests.Session()
    session.verify = False

    headers = {"Authorization": f"Token {token}"}
    status_url = f"{base_url}/api/status/"

    try:
        resp = session.get(status_url, headers=headers, timeout=15)
        resp.raise_for_status()

        status_data = resp.json()
        if "netbox-version" not in status_data:
            raise RuntimeError("Unexpected response payload from /api/status/")

        nb = pynetbox.api(base_url, token)
        nb.http_session = session
        return nb

    except requests.RequestException as exc:
        logger.error("Failed to reach Netbox status endpoint %s", status_url, exc_info=True)
        raise RuntimeError(f"Failed to connect to Netbox ({status_url}): {exc}") from exc
    except ValueError as exc:
        logger.error("Netbox status endpoint returned non-JSON response %s", status_url, exc_info=True)
        raise RuntimeError(f"Netbox status endpoint returned invalid JSON: {exc}") from exc
