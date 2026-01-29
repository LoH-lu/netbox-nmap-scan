#!/usr/bin/env python3
"""netbox_import.py

Netbox IP address import (CSV -> Netbox).

Reads a CSV with the following columns (case-sensitive):
    address, dns_name, status, scantime, tags, tenant, VRF

Behavior:
- If the IP exists (same address + VRF):
    - Update it
    - Merge existing tags with CSV tags (deduplicated)
    - For DHCP addresses, only update custom_fields.scantime and tags
- If the IP does not exist:
    - Create it

Tag handling:
- Tags are resolved in Netbox by name or by slug.
- Missing tags are created as a fallback (best-effort).
- Canonical tag names are used when attaching tags to IPs.

Logging:
- When run as a script, logging is configured via logging_utils and var.ini [logging].
- When imported, it inherits logging configuration from the caller.
"""

from __future__ import annotations

import configparser
import csv
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

import pynetbox
import urllib3

from netbox_connection import connect_to_netbox

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger(__name__)


def _slugify(value: str) -> str:
    """Simple slugify: lowercase, replace separators with '-', keep alnum and '-'"""
    s = value.strip().lower()
    out: List[str] = []
    for ch in s:
        if ch.isalnum() or ch == "-":
            out.append(ch)
        elif ch.isspace() or ch in ("_", "."):
            out.append("-")
    slug = "".join(out)
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-")


def resolve_tags(netbox: pynetbox.api, tags_string: str) -> List[Dict[str, str]]:
    """Resolve comma-separated tags into Netbox write format [{'name': '...'}]."""
    if not tags_string:
        return []

    tokens = [t.strip() for t in tags_string.split(",") if t.strip()]
    resolved: List[Dict[str, str]] = []

    for token in tokens:
        # 1) Try exact name
        try:
            tag_obj = netbox.extras.tags.get(name=token)
            if tag_obj:
                resolved.append({"name": tag_obj.name})
                continue
        except Exception:
            logger.debug("Tag lookup by name failed: %s", token, exc_info=True)

        # 2) Try slug
        token_slug = _slugify(token)
        try:
            tag_obj = netbox.extras.tags.get(slug=token_slug)
            if tag_obj:
                resolved.append({"name": tag_obj.name})
                continue
        except Exception:
            logger.debug("Tag lookup by slug failed: %s", token, exc_info=True)

        # 3) Fallback: create tag (best-effort)
        try:
            created = netbox.extras.tags.create({"name": token, "slug": token_slug})
            if created:
                logger.info("Created missing tag '%s' (slug='%s')", created.name, created.slug)
                resolved.append({"name": created.name})
        except Exception as exc:
            logger.warning("Could not create tag '%s' (skipping). Error: %s", token, exc)

    # Deduplicate while preserving order
    seen = set()
    deduped: List[Dict[str, str]] = []
    for t in resolved:
        name = t["name"]
        if name not in seen:
            deduped.append(t)
            seen.add(name)

    return deduped


def _normalize_tags(existing_tags: object) -> List[Dict[str, str]]:
    """Convert pynetbox tag objects/dicts into the Netbox write format [{'name': '...'}]."""
    normalized: List[Dict[str, str]] = []
    if not existing_tags:
        return normalized

    for t in existing_tags:
        if isinstance(t, dict):
            name = t.get("name") or t.get("slug") or str(t)
        else:
            name = getattr(t, "name", str(t))
        normalized.append({"name": name})
    return normalized


def _merge_tags(existing_tags: object, new_tags: List[Dict[str, str]]) -> List[Dict[str, str]]:
    merged = _normalize_tags(existing_tags)
    existing_names = {t["name"] for t in merged}
    for t in new_tags:
        if t["name"] not in existing_names:
            merged.append(t)
            existing_names.add(t["name"])
    return merged


def _lookup_vrf(netbox: pynetbox.api, vrf_name: str) -> Optional[Dict[str, str]]:
    """Return {'id': <vrf_id>} or None if VRF is not specified or not found."""
    if not vrf_name or vrf_name == "N/A":
        return None

    try:
        vrf = netbox.ipam.vrfs.get(name=vrf_name)
        if not vrf:
            logger.warning("VRF not found in Netbox: %s", vrf_name)
            return None
        return {"id": vrf.id}
    except Exception:
        logger.error("Error looking up VRF %s", vrf_name, exc_info=True)
        return None


def _lookup_existing_ip(netbox: pynetbox.api, address: str, vrf: Optional[Dict[str, str]]) -> Optional[object]:
    try:
        return netbox.ipam.ip_addresses.get(address=address, vrf_id=vrf["id"] if vrf else None)
    except Exception:
        logger.error("Error querying existing address %s", address, exc_info=True)
        return None


def _update_existing_address(
    existing: object,
    row: Dict[str, str],
    tags_list: List[Dict[str, str]],
    tenant_data: Optional[Dict[str, str]],
    vrf_data: Optional[Dict[str, str]],
) -> None:
    """Update an existing Netbox IP object according to import rules."""
    merged_tags = _merge_tags(getattr(existing, "tags", None), tags_list)

    status_value = getattr(getattr(existing, "status", None), "value", None)
    is_dhcp = bool(status_value and str(status_value).lower() == "dhcp")

    existing.custom_fields = getattr(existing, "custom_fields", None) or {}
    existing.custom_fields.update({"scantime": row.get("scantime")})
    existing.tags = merged_tags

    if not is_dhcp:
        existing.status = row.get("status")
        if row.get("dns_name"):
            existing.dns_name = row.get("dns_name")
        existing.tenant = tenant_data
        existing.vrf = vrf_data

    existing.save()


def _create_new_address(
    netbox: pynetbox.api,
    address: str,
    row: Dict[str, str],
    tags_list: List[Dict[str, str]],
    tenant_data: Optional[Dict[str, str]],
    vrf_data: Optional[Dict[str, str]],
) -> None:
    """Create a new IP address in Netbox."""
    netbox.ipam.ip_addresses.create(
        address=address,
        status=row.get("status"),
        custom_fields={"scantime": row.get("scantime")},
        dns_name=row.get("dns_name"),
        tags=tags_list,
        tenant=tenant_data,
        vrf=vrf_data,
    )


def _process_row(row: Dict[str, str], netbox: pynetbox.api) -> None:
    """Process a single CSV row (create or update)."""
    tags_list = resolve_tags(netbox, row.get("tags", ""))
    tenant_data = {"name": row["tenant"]} if row.get("tenant") and row["tenant"] != "N/A" else None
    vrf_data = _lookup_vrf(netbox, row.get("VRF", "N/A"))

    address = row.get("address", "unknown")
    existing = _lookup_existing_ip(netbox, address, vrf_data)
    if existing:
        _update_existing_address(existing, row, tags_list, tenant_data, vrf_data)
    else:
        _create_new_address(netbox, address, row, tags_list, tenant_data, vrf_data)


def _load_max_workers() -> int:
    """Read scan_max_workers from var.ini; used as import worker count."""
    config = configparser.ConfigParser()
    config.read(os.path.join(SCRIPT_DIR, "var.ini"))
    workers = config.getint("scan_options", "scan_max_workers", fallback=5)
    return max(1, workers)


def write_data_to_netbox(url: str, token: str, csv_file: str) -> None:
    """Write data from a CSV file to Netbox."""
    max_workers = _load_max_workers()

    logger.info("Connecting to Netbox...")
    netbox = connect_to_netbox(url, token)
    logger.info("Connected to Netbox")

    csv_path = csv_file if os.path.isabs(csv_file) else os.path.join(SCRIPT_DIR, csv_file)
    logger.info("Reading CSV: %s", csv_path)

    with open(csv_path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    total = len(rows)
    logger.info("Rows to process: %d", total)

    failures = 0

    # If Netbox/API pressure is high, set scan_max_workers=1 in var.ini.
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_process_row, row, netbox) for row in rows]
        for fut in as_completed(futures):
            try:
                fut.result()
            except pynetbox.core.query.RequestError as exc:
                if "Duplicate IP address" in str(exc):
                    logger.warning("Duplicate IP address during import: %s", exc)
                    continue
                failures += 1
                logger.error("Netbox request error in worker", exc_info=True)
            except Exception:
                failures += 1
                logger.error("Worker failed during import", exc_info=True)

    if failures:
        logger.error("Import completed with %d failure(s). See error log for details.", failures)
    else:
        logger.info("Import completed successfully: %s", csv_path)


def main() -> None:
    from logging_utils import configure_logging

    configure_logging(app_name="netbox_import", script_dir=SCRIPT_DIR)
    logger.info("Starting Netbox import")

    config = configparser.ConfigParser()
    config_path = os.path.join(SCRIPT_DIR, "var.ini")
    if not config.read(config_path):
        raise RuntimeError(f"Configuration file not found: {config_path}")

    url = config["credentials"]["url"]
    token = config["credentials"]["token"]

    write_data_to_netbox(url, token, os.path.join(SCRIPT_DIR, "ipam_addresses.csv"))


if __name__ == "__main__":
    main()
