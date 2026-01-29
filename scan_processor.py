#!/usr/bin/env python3
"""
Per-prefix scan result processor.

Merges the latest and previous scan CSVs for a prefix folder and emits
a Netbox-ready `ipam_addresses.csv`.

Output rules:
- Include ALL active IPs from the latest scan
- Include ONLY deprecated IPs that existed in the previous scan but are missing now

Logging:
- Inherits handlers configured by the scheduler (root logger).
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
    files = [f for f in os.listdir(folder) if f.startswith("nmap_results_") and f.endswith(".csv")]
    files.sort(reverse=True)
    return files[:2]


def _read_rows(path: str) -> Dict[str, Dict[str, str]]:
    data: Dict[str, Dict[str, str]] = {}
    with open(path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ip = row["address"].split("/")[0]
            vrf = row.get("VRF", "N/A") or "N/A"
            data[f"{ip}__{vrf}"] = row
    return data


def process_scan_results_in_dir(results_dir: str, output_file: str) -> None:
    scans = _latest_scan_files(results_dir)
    if not scans:
        raise ValueError(f"No scan files found in {results_dir}")

    latest = _read_rows(os.path.join(results_dir, scans[0]))
    previous = _read_rows(os.path.join(results_dir, scans[1])) if len(scans) > 1 else {}

    merged: Dict[str, Dict[str, str]] = {}

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
