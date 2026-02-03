#!/usr/bin/env python3
"""scan_processor.py

Per-prefix scan result processor.

This module converts raw scan outputs (``nmap_results_*.csv``) into a NetBox-ready
``ipam_addresses.csv`` suitable for import by :mod:`netbox_import`.

Conceptually, the processor keeps NetBox in sync with scan observations using two states:

- **active**: an IP that is present in the latest scan
- **deprecated**: an IP that was present in the previous scan but is missing now

This is intentionally conservative:
- It never marks an IP as deprecated based on *one* missing scan if there is no prior scan.
- It always includes all active IPs from the latest scan.

Input files
-----------
The processor looks for CSV files matching::

    nmap_results_<timestamp>.csv

It uses the two newest files (latest and previous) per prefix folder.

Output file
-----------
A single CSV is written (overwritten) at the provided output path.
Columns are fixed and case-sensitive because NetBox import relies on them:

    address, dns_name, status, scantime, tags, tenant, VRF

Logging
-------
Library code; uses ``logging.getLogger(__name__)`` and inherits handlers from the scheduler.
"""

from __future__ import annotations

import csv
import ipaddress
import logging
import os
from typing import Dict, List

CSV_FIELDNAMES = ["address", "dns_name", "status", "scantime", "tags", "tenant", "VRF"]

logger = logging.getLogger(__name__)


def _latest_scan_files(folder: str) -> List[str]:
    """Return the two newest scan filenames in a folder (newest first)."""
    files = [f for f in os.listdir(folder) if f.startswith("nmap_results_") and f.endswith(".csv")]
    files.sort(reverse=True)
    return files[:2]


def _read_rows(path: str) -> Dict[str, Dict[str, str]]:
    """Read a scan CSV into a dict keyed by (ip, vrf).

    The key format is ``"<ip>__<vrf>"`` so the same IP can exist in multiple VRFs without collision.

    Args:
        path: CSV file path.

    Returns:
        Mapping key -> raw CSV row dict.
    """
    data: Dict[str, Dict[str, str]] = {}
    with open(path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ip = row["address"].split("/")[0]
            vrf = row.get("VRF", "N/A") or "N/A"
            data[f"{ip}__{vrf}"] = row
    return data


def process_scan_results_in_dir(results_dir: str, output_file: str) -> None:
    """Merge the latest and previous scan results into a NetBox import CSV.

    Args:
        results_dir: Prefix folder containing ``nmap_results_*.csv`` files.
        output_file: Destination path for the generated ``ipam_addresses.csv``.

    Output rules:
        - Include ALL IPs from the latest scan with status ``active``.
        - Include ONLY IPs that existed in the previous scan but are missing now with status ``deprecated``.

    Raises:
        ValueError: If no scan files exist in ``results_dir``.
        OSError: If scan files cannot be read or the output cannot be written.
    """
    scans = _latest_scan_files(results_dir)
    if not scans:
        raise ValueError(f"No scan files found in {results_dir}")

    latest = _read_rows(os.path.join(results_dir, scans[0]))
    previous = _read_rows(os.path.join(results_dir, scans[1])) if len(scans) > 1 else {}

    merged: Dict[str, Dict[str, str]] = {}

    # 1) Latest scan -> active
    for k, r in latest.items():
        merged[k] = {
            "address": r["address"],
            "dns_name": r.get("dns_name", ""),
            "status": "active",
            "scantime": r.get("scantime", ""),
            "tags": r.get("tags", "autoscan"),
            "tenant": r.get("tenant", "N/A"),
            "VRF": r.get("VRF", "N/A"),
        }

    # 2) Previous scan entries missing in latest -> deprecated
    for k, r in previous.items():
        if k not in latest:
            merged[k] = {
                "address": r["address"],
                "dns_name": r.get("dns_name", ""),
                "status": "deprecated",
                "scantime": r.get("scantime", ""),
                "tags": r.get("tags", "autoscan"),
                "tenant": r.get("tenant", "N/A"),
                "VRF": r.get("VRF", "N/A"),
            }

    rows = list(merged.values())
    # Sort by IP address to produce stable diffs and human-friendly output.
    rows.sort(key=lambda r: ipaddress.ip_address(r["address"].split("/")[0]))

    os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    logger.debug(
        "Processed %s -> %s (latest=%s, previous=%s, rows=%d)",
        results_dir,
        output_file,
        scans[0],
        scans[1] if len(scans) > 1 else "N/A",
        len(rows),
    )
