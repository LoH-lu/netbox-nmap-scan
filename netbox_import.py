#!/usr/bin/env python3
"""netbox_import.py

NetBox IP address import (CSV -> NetBox).

This module takes scan output produced by the scheduler pipeline and reconciles it
into NetBox IP Address records.

Input CSV schema (case-sensitive)
---------------------------------
The importer expects a CSV with these columns::

    address, dns_name, status, scantime, tags, tenant, VRF

Where:
- ``address`` is CIDR notation (e.g. ``10.0.0.1/24``)
- ``VRF`` is the VRF name (or ``N/A`` / empty for global table)
- ``tenant`` is the tenant name (or ``N/A``)
- ``tags`` is a comma-separated list of tag names
- ``scantime`` is stored in the IP address custom field ``custom_fields.scantime``

Reconciliation rules
--------------------
For each CSV row, the importer looks up an existing NetBox IP Address record by
(address, VRF):

- If the IP exists:
    - Merge existing tags with CSV tags (deduplicated).
    - Update ``custom_fields.scantime``.
    - If the existing IP status is **DHCP**, only update ``custom_fields.scantime`` and tags
      (do not overwrite status, tenant, VRF, dns_name). This preserves DHCP semantics.
    - Otherwise, update status, dns_name (when provided), tenant, and VRF.

- If the IP does not exist:
    - Create it with status, dns_name, tags, tenant, VRF, and custom field ``scantime``.

Tag handling
------------
NetBox expects tags in write format ``[{"name": "..."}, ...]``.
To be robust across deployments:
- Each CSV tag token is resolved by exact tag name first.
- If not found, it is resolved by slug.
- If still not found, the importer attempts to create the tag (best-effort).
  Failures to create tags are logged and the tag is skipped.

Concurrency
-----------
Rows are processed concurrently using a ``ThreadPoolExecutor``. The worker count is read
from ``var.ini`` under ``[scan_options]`` as ``scan_max_workers`` (shared with scan worker count).

Logging
-------
- When run as a script, logging is configured via :func:`logging_utils.configure_logging`.
- When imported, this module does not configure handlers; it uses the caller's configuration.

Security note
-------------
The underlying HTTP session in :func:`netbox_connection.connect_to_netbox` disables TLS
verification (``verify=False``). Warnings are suppressed here to reduce noise, but the
recommended long-term solution is to enable proper certificate validation.
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
    """Convert a human tag string into a NetBox-friendly slug.

    NetBox tag slugs are typically lowercase and dash-separated. This helper keeps the
    implementation intentionally simple to avoid extra dependencies.

    Args:
        value: Raw tag name (e.g. ``"Disable Automatic Scanning"``).

    Returns:
        Slug string (e.g. ``"disable-automatic-scanning"``).
    """
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
    """Resolve comma-separated tag names into NetBox write format.

    Args:
        netbox: Authenticated NetBox API client.
        tags_string: Comma-separated tag names. Whitespace is ignored.

    Returns:
        A list of dicts in NetBox "write" format: ``[{"name": "..."}, ...]``.
        Order is preserved and duplicates are removed.

    Notes:
        Resolution strategy per token:
        1) Lookup by exact name
        2) Lookup by slug
        3) Create the tag (best-effort)
    """
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
    """Normalize tag objects/dicts into NetBox write format.

    NetBox returns tags via the API as either dict-like objects or pynetbox Record objects,
    depending on version/configuration. This helper converts those into::

        [{"name": "TagName"}, ...]

    Args:
        existing_tags: Iterable of tag objects/dicts as returned by pynetbox.

    Returns:
        Normalized list in NetBox write format.
    """
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
    """Merge existing and new tags, preserving order and avoiding duplicates."""
    merged = _normalize_tags(existing_tags)
    existing_names = {t["name"] for t in merged}
    for t in new_tags:
        if t["name"] not in existing_names:
            merged.append(t)
            existing_names.add(t["name"])
    return merged


def _lookup_vrf(netbox: pynetbox.api, vrf_name: str) -> Optional[Dict[str, str]]:
    """Resolve a VRF name to NetBox write data.

    Args:
        netbox: Authenticated NetBox API client.
        vrf_name: VRF name from the CSV. ``"N/A"`` or empty means "no VRF".

    Returns:
        ``{"id": <vrf_id>}`` or ``None`` if VRF is not specified or not found.

    Notes:
        NetBox writes use objects or dicts depending on endpoint; pynetbox accepts a
        dict with ``id`` for foreign keys.
    """
    if not vrf_name or vrf_name == "N/A":
        return None

    try:
        vrf = netbox.ipam.vrfs.get(name=vrf_name)
        if not vrf:
            logger.warning("VRF not found in NetBox: %s", vrf_name)
            return None
        return {"id": vrf.id}
    except Exception:
        logger.error("Error looking up VRF %s", vrf_name, exc_info=True)
        return None


def _lookup_existing_ip(netbox: pynetbox.api, address: str, vrf: Optional[Dict[str, str]]) -> Optional[object]:
    """Lookup an existing IP Address record by (address, vrf_id)."""
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
    """Update an existing NetBox IP record according to the import rules.

    Args:
        existing: Pynetbox record for an existing IP address.
        row: Raw CSV row dict.
        tags_list: Resolved tags for the row.
        tenant_data: ``{"name": ...}`` or ``None``.
        vrf_data: ``{"id": ...}`` or ``None``.

    Side effects:
        Saves changes to NetBox via ``existing.save()``.
    """
    merged_tags = _merge_tags(getattr(existing, "tags", None), tags_list)

    status_value = getattr(getattr(existing, "status", None), "value", None)
    is_dhcp = bool(status_value and str(status_value).lower() == "dhcp")

    # Always update scan metadata and tags.
    existing.custom_fields = getattr(existing, "custom_fields", None) or {}
    existing.custom_fields.update({"scantime": row.get("scantime")})
    existing.tags = merged_tags

    # DHCP records are treated as authoritative; do not overwrite core fields.
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
    """Create a new IP Address record in NetBox."""
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
    """Worker function: reconcile a single CSV row into NetBox.

    This function is executed concurrently by the thread pool. It is intentionally
    side-effecting: each call may perform NetBox lookups and create/update operations.

    Args:
        row: CSV row dict (already parsed by csv.DictReader).
        netbox: Shared NetBox client instance.

    Raises:
        Any exception is allowed to propagate to the thread pool; the caller counts/logs failures.
    """
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
    """Read ``scan_max_workers`` from ``var.ini`` for import concurrency.

    Returns:
        Worker count >= 1.
    """
    config = configparser.ConfigParser()
    config.read(os.path.join(SCRIPT_DIR, "var.ini"))
    workers = config.getint("scan_options", "scan_max_workers", fallback=5)
    return max(1, workers)


def write_data_to_netbox(url: str, token: str, csv_file: str) -> None:
    """Import IP addresses from a CSV file into NetBox.

    Args:
        url: NetBox base URL.
        token: API token.
        csv_file: CSV path. If relative, it is resolved relative to ``SCRIPT_DIR``.

    Behavior:
        - Connects to NetBox.
        - Loads all rows into memory (sufficient for typical prefix-sized exports).
        - Processes rows concurrently with a thread pool.
        - Logs and continues on per-row failures.

    Notes:
        If NetBox or the API proxy is under pressure, reduce ``scan_max_workers`` to 1 in
        ``var.ini``. The scan and import worker counts share the same setting.
    """
    max_workers = _load_max_workers()

    logger.info("Connecting to NetBox...")
    netbox = connect_to_netbox(url, token)
    logger.info("Connected to NetBox")

    csv_path = csv_file if os.path.isabs(csv_file) else os.path.join(SCRIPT_DIR, csv_file)
    logger.info("Reading CSV: %s", csv_path)

    with open(csv_path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    total = len(rows)
    logger.info("Rows to process: %d", total)

    failures = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_process_row, row, netbox) for row in rows]
        for fut in as_completed(futures):
            try:
                fut.result()
            except pynetbox.core.query.RequestError as exc:
                # NetBox can error on duplicates if two workers race for the same IP.
                # This is treated as non-fatal (the record exists, which is the desired end state).
                if "Duplicate IP address" in str(exc):
                    logger.warning("Duplicate IP address during import: %s", exc)
                    continue
                failures += 1
                logger.error("NetBox request error in worker", exc_info=True)
            except Exception:
                failures += 1
                logger.error("Worker failed during import", exc_info=True)

    if failures:
        logger.error("Import completed with %d failure(s). See error log for details.", failures)
    else:
        logger.info("Import completed successfully: %s", csv_path)


def main() -> None:
    """CLI entrypoint for ad-hoc imports.

    Reads credentials from ``var.ini`` and imports ``ipam_addresses.csv`` from the script
    directory. Intended for manual runs or troubleshooting.
    """
    from logging_utils import configure_logging

    configure_logging(app_name="netbox_import", script_dir=SCRIPT_DIR)
    logger.info("Starting NetBox import")

    config = configparser.ConfigParser()
    config_path = os.path.join(SCRIPT_DIR, "var.ini")
    if not config.read(config_path):
        raise RuntimeError(f"Configuration file not found: {config_path}")

    url = config["credentials"]["url"]
    token = config["credentials"]["token"]

    write_data_to_netbox(url, token, os.path.join(SCRIPT_DIR, "ipam_addresses.csv"))


if __name__ == "__main__":
    main()
