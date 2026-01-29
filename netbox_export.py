#!/usr/bin/env python3
"""netbox_export.py

Export IPAM prefixes from Netbox to a CSV file `ipam_prefixes.csv`.

- As a module: import and call `get_ipam_prefixes(netbox_instance)`.
- As a script: reads credentials from var.ini, connects to Netbox, writes CSV.

Logging:
- When run as a script, logging is configured via logging_utils and var.ini [logging].
- When imported, it inherits logging configuration from the caller.
"""

from __future__ import annotations

import configparser
import csv
import logging
import os
import sys
from typing import List

import pynetbox

import netbox_connection

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger(__name__)

CSV_HEADERS = ["Prefix", "VRF", "Status", "Tags", "Tenant"]


def load_config() -> tuple[str, str]:
    """Load Netbox URL/token from var.ini."""
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
    """Retrieve all IPAM prefixes from Netbox."""
    logger.info("Retrieving IPAM prefixes from Netbox")
    try:
        prefixes = list(netbox_instance.ipam.prefixes.all())
        logger.info("Retrieved %d prefix(es)", len(prefixes))
        return prefixes
    except Exception:
        logger.error("Failed to retrieve IPAM prefixes", exc_info=True)
        raise


def write_to_csv(prefixes: List[object], filename: str) -> None:
    """Write IPAM prefixes to a CSV file in SCRIPT_DIR."""
    file_path = os.path.join(SCRIPT_DIR, filename)
    logger.info("Writing CSV export to %s", file_path)

    try:
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADERS)

            for p in prefixes:
                tags = []
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
    from logging_utils import configure_logging

    configure_logging(app_name="netbox_export", script_dir=SCRIPT_DIR)
    logger.info("Starting Netbox IPAM export")

    try:
        url, token = load_config()
        logger.info("Connecting to Netbox...")
        nb = netbox_connection.connect_to_netbox(url, token)
        logger.info("Connected to Netbox")

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
