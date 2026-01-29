#!/usr/bin/env python3
"""
Per-prefix scan result processor.

Output logic:
- Include ALL active IPs from latest scan
- Include ONLY deprecated IPs that existed in previous scan
- Never invent or enumerate addresses
"""

import csv
import os
import logging
from typing import Dict, List
from datetime import datetime
import ipaddress

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")

CSV_FIELDNAMES = [
    "address", "dns_name", "status", "scantime", "tags", "tenant", "VRF"
]

os.makedirs(LOG_DIR, exist_ok=True)


def _latest_scan_files(folder: str) -> List[str]:
    files = [f for f in os.listdir(folder) if f.startswith("nmap_results_")]
    files.sort(reverse=True)
    return files[:2]


def _read_rows(path: str) -> Dict[str, Dict[str, str]]:
    data = {}
    with open(path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ip = row["address"].split("/")[0]
            vrf = row.get("VRF", "N/A")
            data[f"{ip}__{vrf}"] = row
    return data


def process_scan_results_in_dir(results_dir: str, output_file: str) -> None:
    logger = logging.getLogger(__name__)

    scans = _latest_scan_files(results_dir)
    if not scans:
        raise ValueError("No scan files found")

    latest = _read_rows(os.path.join(results_dir, scans[0]))
    previous = _read_rows(os.path.join(results_dir, scans[1])) if len(scans) > 1 else {}

    merged: Dict[str, Dict[str, str]] = {}

    # Active from latest
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

    # Deprecated (only if seen before)
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

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
