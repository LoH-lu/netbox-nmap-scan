#!/usr/bin/env python3
"""netbox_export.py

Export IPAM prefixes from NetBox to a CSV file.

This module supports two usage patterns:

1) Library usage (imported)
   - Call :func:`get_ipam_prefixes` with an existing ``pynetbox.api`` instance.
   - Call :func:`write_to_csv` to serialize the returned prefix objects to CSV.

2) Script usage (executed directly)
   - Reads NetBox credentials from ``var.ini`` in the same folder as this script.
   - Establishes a NetBox connection (via :mod:`netbox_connection`).
   - Writes ``ipam_prefixes.csv`` next to this script.

Output format
-------------
The CSV contains one row per prefix with the following columns:

- Prefix: CIDR string (e.g. ``10.0.0.0/24``)
- VRF: VRF name or ``N/A``
- Status: NetBox status value (e.g. ``active``) or ``N/A``
- Tags: Comma-separated list of tag names
- Tenant: Tenant name or ``N/A``

Logging
-------
- When run as a script, logging is configured via :func:`logging_utils.configure_logging`
  using the optional ``[logging]`` section in ``var.ini``.
- When imported, this module does not configure handlers; it uses the logger created by
  the caller's logging configuration.

Configuration file (var.ini)
----------------------------
Expected structure::

    [credentials]
    url = https://netbox.example.org
    token = <API_TOKEN>

"""

from __future__ import annotations

import configparser
import csv
import logging
import os
import sys
from typing import List, Sequence, Tuple

import pynetbox

import netbox_connection

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger(__name__)

CSV_HEADERS = ["Prefix", "VRF", "Status", "Tags", "Tenant"]


def load_config() -> Tuple[str, str]:
    """Load NetBox URL and token from ``var.ini``.

    The function reads a local ``var.ini`` stored in the same directory as this script.

    Returns:
        A tuple ``(url, token)``.

    Raises:
        RuntimeError: If the file is missing/unreadable or required keys are absent.
    """
    config_path = os.path.join(SCRIPT_DIR, "var.ini")

    config = configparser.ConfigParser()
    if not config.read(config_path):
        raise RuntimeError(f"Configuration file not found: {config_path}")

    try:
        url = config["credentials"]["url"]
        token = config["credentials"]["token"]
        return url, token
    except KeyError as exc:
        raise RuntimeError(f"Missing configuration key in {config_path}: {exc}") from exc


def get_ipam_prefixes(netbox_instance: pynetbox.api) -> List[object]:
    """Retrieve all IPAM prefixes from NetBox.

    Args:
        netbox_instance: Authenticated NetBox client.

    Returns:
        A list of prefix records as returned by ``pynetbox``. The exact record type is a
        ``pynetbox`` Record, so we type it as ``object`` to avoid tight coupling.

    Raises:
        Exception: Propagates any exception raised by the underlying API call.
    """
    logger.info("Retrieving IPAM prefixes from NetBox")
    try:
        prefixes = list(netbox_instance.ipam.prefixes.all())
        logger.info("Retrieved %d prefix(es)", len(prefixes))
        return prefixes
    except Exception:
        logger.error("Failed to retrieve IPAM prefixes", exc_info=True)
        raise


def write_to_csv(prefixes: Sequence[object], filename: str) -> None:
    """Write prefixes to a CSV file.

    Args:
        prefixes: Iterable of prefix objects (typically from :func:`get_ipam_prefixes`).
        filename: Output filename (relative to ``SCRIPT_DIR``).

    Side effects:
        Creates/overwrites the CSV file at ``SCRIPT_DIR/filename``.

    Raises:
        Exception: If the file cannot be written or prefix objects are missing attributes.
    """
    file_path = os.path.join(SCRIPT_DIR, filename)
    logger.info("Writing CSV export to %s", file_path)

    try:
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADERS)

            for p in prefixes:
                # Tags can come back as objects, dicts, or None depending on NetBox/pynetbox versions.
                tags: List[str] = []
                for t in getattr(p, "tags", []) or []:
                    if isinstance(t, dict):
                        tags.append(t.get("name") or t.get("slug") or str(t))
                    else:
                        tags.append(getattr(t, "name", str(t)))

                tenant_name = p.tenant.name if getattr(p, "tenant", None) else "N/A"
                status_value = p.status.value if getattr(p, "status", None) else "N/A"
                vrf_name = p.vrf.name if getattr(p, "vrf", None) else "N/A"

                writer.writerow([p.prefix, vrf_name, status_value, ", ".join(tags), tenant_name])

        logger.info("Export completed: %s", filename)

    except Exception:
        logger.error("Failed to write CSV file %s", filename, exc_info=True)
        raise


def main() -> None:
    """CLI entrypoint.

    Reads configuration, connects to NetBox, exports prefixes to ``ipam_prefixes.csv``.
    Exits with status code 1 on failure.
    """
    from logging_utils import configure_logging

    configure_logging(app_name="netbox_export", script_dir=SCRIPT_DIR)
    logger.info("Starting NetBox IPAM export")

    try:
        url, token = load_config()
        logger.info("Connecting to NetBox...")
        nb = netbox_connection.connect_to_netbox(url, token)
        logger.info("Connected to NetBox")

        prefixes = get_ipam_prefixes(nb)
        if prefixes:
            write_to_csv(prefixes, "ipam_prefixes.csv")
        else:
            logger.warning("No prefixes found to export")

    except Exception:
        logger.error("Export failed", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
