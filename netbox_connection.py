#!/usr/bin/env python3
"""netbox_connection.py

Small helper to create a `pynetbox` API client with a pre-configured HTTP session.

Why this module exists
----------------------
`pynetbox.api(...)` is intentionally lightweight; it does not validate connectivity.
This project wants an early, explicit check that the target NetBox instance is reachable
and that the provided token is accepted. We use `/api/status/` for that.

Design notes
------------
- This file is *library code*: it should not create logging handlers. It logs through
  ``logging.getLogger(__name__)`` and relies on the caller (e.g., ``main.py``) to configure
  handlers and formatting.
- SSL verification is disabled (``session.verify = False``) to match the current behavior.
  This is convenient in lab environments but weakens transport security. Prefer enabling
  verification once you can deploy the proper CA chain.

Exports
-------
- :func:`connect_to_netbox` - return a ready-to-use ``pynetbox.api`` instance.
"""

from __future__ import annotations

import logging

import pynetbox
import requests

logger = logging.getLogger(__name__)


def connect_to_netbox(url: str, token: str) -> pynetbox.api:
    """Create a NetBox API client and validate connectivity.

    The function:
    1) Normalizes the base URL (removes trailing slashes).
    2) Builds a `requests.Session` with a disabled TLS verification flag (current behavior).
    3) Calls ``GET <base_url>/api/status/`` to verify reachability and token validity.
    4) Constructs a ``pynetbox.api`` object and attaches the configured HTTP session.

    Args:
        url: Base NetBox URL (e.g. ``https://netbox.example.org``).
        token: NetBox API token ("Token ..." is added internally).

    Returns:
        A ``pynetbox.api`` client with ``http_session`` set to the prepared `requests` session.

    Raises:
        RuntimeError:
            - If the status endpoint cannot be reached,
            - If the response is not JSON,
            - Or if the payload does not look like a NetBox status response.

    Security:
        This function currently sets ``session.verify = False``. That disables certificate
        validation and exposes the connection to MITM risks. If possible, set it to True
        and install the correct CA bundle instead.
    """
    base_url = url.rstrip("/")

    session = requests.Session()
    session.verify = False  # NOTE: Matches current behavior. Prefer True with a proper CA chain.

    headers = {"Authorization": f"Token {token}"}
    status_url = f"{base_url}/api/status/"

    try:
        # A short timeout prevents the scheduler from hanging indefinitely.
        resp = session.get(status_url, headers=headers, timeout=15)
        resp.raise_for_status()

        status_data = resp.json()
        # NetBox typically returns keys like "netbox-version" and "python-version".
        if "netbox-version" not in status_data:
            raise RuntimeError("Unexpected response payload from /api/status/")

        nb = pynetbox.api(base_url, token)
        nb.http_session = session
        return nb

    except requests.RequestException as exc:
        logger.error("Failed to reach NetBox status endpoint %s", status_url, exc_info=True)
        raise RuntimeError(f"Failed to connect to NetBox ({status_url}): {exc}") from exc
    except ValueError as exc:
        # `.json()` can raise ValueError on invalid JSON.
        logger.error("NetBox status endpoint returned non-JSON response %s", status_url, exc_info=True)
        raise RuntimeError(f"NetBox status endpoint returned invalid JSON: {exc}") from exc
