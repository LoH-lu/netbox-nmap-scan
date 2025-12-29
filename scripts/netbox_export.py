#!/usr/bin/env python3
"""
Netbox IPAM Prefixes Export Script.

Exports IPAM prefixes from a Netbox instance to a CSV file.

Filter rules:
- Export ONLY prefixes with status == 'active'
- EXCLUDE prefixes that have a tag with name matching:
    - 'Disable Automatic Scanning'
    - OR any tag name starting with 'Disable Automatic Scanning'
      (covers variants like 'Disable Automatic Scanning (ID: 1)')

Requirements:
    - pynetbox
    - configparser
"""

import csv
import os
import logging
import sys
from typing import List, Optional, Tuple
import configparser
from datetime import datetime
import pynetbox
from scripts import netbox_connection

# Script configuration
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT_DIR = PROJECT_ROOT
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
CSV_HEADERS = ["Prefix", "VRF", "Status", "Tags", "Tenant"]

# Tag exclusion config (NAME ONLY)
EXCLUDED_TAG_NAME = "Disable Automatic Scanning"

# Ensure log directory exists
os.makedirs(LOG_DIR, exist_ok=True)


def setup_logging() -> logging.Logger:
    """Configure logging with both file and console handlers."""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # Avoid duplicate handlers if invoked multiple times
    if logger.handlers:
        return logger

    file_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
    )
    console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    debug_handler = logging.FileHandler(
        os.path.join(LOG_DIR, f"netbox_export_debug_{timestamp}.log")
    )
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(file_formatter)

    error_handler = logging.FileHandler(
        os.path.join(LOG_DIR, f"netbox_export_error_{timestamp}.log")
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    logger.addHandler(debug_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)

    return logger


def load_config() -> Tuple[str, str]:
    """
    Load configuration from var.ini file.

    Returns:
        (url, token)
    """
    logger = logging.getLogger(__name__)
    config_path = os.path.join(SCRIPT_DIR, "var.ini")

    try:
        logger.debug("Reading configuration from: %s", config_path)
        config = configparser.ConfigParser()
        config.read(config_path)

        url = config["credentials"]["url"]
        token = config["credentials"]["token"]

        logger.debug("Successfully loaded configuration")
        return url, token

    except Exception:
        logger.error("Failed to load configuration from %s", config_path, exc_info=True)
        raise


def _prefix_status_value(prefix_obj: object) -> str:
    """Safely extract status string from a NetBox Prefix object."""
    try:
        if getattr(prefix_obj, "status", None) is None:
            return ""
        # Usually a ChoiceSet-like object with .value
        return getattr(prefix_obj.status, "value", str(prefix_obj.status)).lower()
    except Exception:
        return ""


def _has_excluded_tag_by_name(prefix_obj: object) -> bool:
    """
    Returns True if the prefix has a tag whose name matches EXCLUDED_TAG_NAME
    or starts with it (to support variants like 'Disable Automatic Scanning (ID: 1)').
    """
    tags = getattr(prefix_obj, "tags", None) or []
    for tag in tags:
        try:
            tag_name = (getattr(tag, "name", "") or "").strip()
            if not tag_name:
                continue
            if tag_name == EXCLUDED_TAG_NAME or tag_name.startswith(EXCLUDED_TAG_NAME):
                return True
        except Exception:
            continue
    return False


def get_ipam_prefixes(netbox_instance: pynetbox.api) -> List[object]:
    """
    Retrieve and filter IPAM prefixes from Netbox.

    Filter:
    - status == 'active'
    - NOT tagged with excluded tag name (name-only match)
    """
    logger = logging.getLogger(__name__)
    logger.info("Retrieving IPAM prefixes from Netbox (filtered export)")

    try:
        # Prefer server-side filter for status if available, then apply tag exclusion locally
        try:
            raw_prefixes = list(netbox_instance.ipam.prefixes.filter(status="active"))
            logger.debug("Used server-side filter: status=active")
        except Exception:
            raw_prefixes = list(netbox_instance.ipam.prefixes.all())
            logger.debug("Server-side filter not available; fetched all prefixes")

        filtered: List[object] = []
        skipped_tag = 0
        skipped_status = 0

        for pfx in raw_prefixes:
            status_val = _prefix_status_value(pfx)
            if status_val != "active":
                skipped_status += 1
                continue

            if _has_excluded_tag_by_name(pfx):
                skipped_tag += 1
                continue

            filtered.append(pfx)

        logger.info(
            "Prefix filtering complete: %d exported, %d skipped (non-active), %d skipped (excluded tag name)",
            len(filtered),
            skipped_status,
            skipped_tag,
        )
        return filtered

    except Exception:
        logger.error("Failed to retrieve/filter IPAM prefixes", exc_info=True)
        raise


def write_to_csv(data: List[object], filename: str) -> None:
    """Write filtered prefixes to CSV."""
    logger = logging.getLogger(__name__)
    file_path = os.path.join(SCRIPT_DIR, filename)

    logger.info("Starting CSV export to: %s", file_path)
    logger.debug("Total records to write: %d", len(data))

    try:
        with open(file_path, "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(CSV_HEADERS)

            for prefix in data:
                try:
                    tag_names = [t.name for t in getattr(prefix, "tags", [])] if hasattr(prefix, "tags") else []
                    tenant_name = prefix.tenant.name if getattr(prefix, "tenant", None) else "N/A"
                    status_value = prefix.status.value if getattr(prefix, "status", None) else "N/A"
                    vrf_name = prefix.vrf.name if getattr(prefix, "vrf", None) else "N/A"

                    writer.writerow(
                        [
                            prefix.prefix,
                            vrf_name,
                            status_value,
                            ", ".join(tag_names),
                            tenant_name,
                        ]
                    )
                    logger.debug("Wrote prefix: %s", prefix.prefix)

                except AttributeError as attr_err:
                    logger.warning(
                        "Missing attribute while processing prefix %s: %s",
                        getattr(prefix, "prefix", "UNKNOWN"),
                        str(attr_err),
                    )
                except Exception:
                    logger.error(
                        "Error processing row for prefix %s",
                        getattr(prefix, "prefix", "UNKNOWN"),
                        exc_info=True,
                    )

        logger.info("Successfully exported filtered data to %s", filename)

    except Exception:
        logger.error("Failed to write to CSV file: %s", filename, exc_info=True)
        raise


def main() -> None:
    logger = setup_logging()
    logger.info("Starting Netbox IPAM export process (filtered)")

    try:
        url, token = load_config()

        logger.info("Connecting to Netbox...")
        netbox_instance = netbox_connection.connect_to_netbox(url, token)
        logger.info("Successfully connected to Netbox")

        prefixes = get_ipam_prefixes(netbox_instance)
        if prefixes:
            write_to_csv(prefixes, "ipam_prefixes.csv")
            logger.info("Export process completed successfully")
        else:
            logger.warning("No matching prefixes found (active and not excluded)")

    except Exception:
        logger.error("Script execution failed", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
