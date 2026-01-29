#!/usr/bin/env python3
"""
Per-prefix Nmap scanner (NEW METHOD ONLY).

Behavior:
- Scans a single prefix
- Always writes:
    - prefix.info
    - nmap_results_<timestamp>.csv (header-only if no hosts found)
- Does NOT enumerate full prefixes
"""

import csv
import subprocess
import os
import configparser
import logging
import threading
from datetime import datetime
from dataclasses import dataclass
from typing import List, Optional, Tuple

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")
NMAP_TIMEOUT = 300
FILE_LOCK = threading.Lock()

OUTPUT_FIELDNAMES = [
    "address", "dns_name", "status", "tags", "tenant", "VRF", "scantime"
]

os.makedirs(LOG_DIR, exist_ok=True)


@dataclass
class ScanResult:
    address: str
    dns_name: Optional[str]
    status: str
    tags: str
    tenant: str
    VRF: str
    scantime: str


def _ensure_prefix_info(folder: str, prefix: str) -> None:
    with open(os.path.join(folder, "prefix.info"), "w", encoding="utf-8") as f:
        f.write(prefix.strip() + "\n")


def _ensure_scan_csv(folder: str, ts: datetime) -> str:
    name = f"nmap_results_{ts.strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    path = os.path.join(folder, name)

    with FILE_LOCK:
        if not os.path.exists(path):
            with open(path, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=OUTPUT_FIELDNAMES).writeheader()
    return path


def _parse_nmap_line(line: str, prefix: str, tenant: str, vrf: str) -> Optional[ScanResult]:
    config = configparser.ConfigParser()
    config.read(os.path.join(SCRIPT_DIR, "var.ini"))
    enable_scantime = config.getboolean("scan_options", "enable_scantime", fallback=True)

    try:
        parts = line.split()
        dns_name = parts[4] if len(parts) > 5 else None
        address = parts[5] if len(parts) > 5 else parts[-1]
        address = address.strip("()")

        mask = prefix.split("/")[-1]
        return ScanResult(
            address=f"{address}/{mask}",
            dns_name=dns_name,
            status="active",
            tags="autoscan",
            tenant=tenant,
            VRF=vrf,
            scantime=datetime.now().strftime("%Y-%m-%d %H:%M:%S") if enable_scantime else ""
        )
    except Exception:
        return None


def run_nmap_on_prefix(
    prefix: str,
    tenant: str,
    VRF: str,
    script_start_time: datetime,
    input_filename=None,
    output_folder=None,
    remove_from_input=False,
) -> Tuple[List[ScanResult], bool]:

    logger = logging.getLogger(__name__)
    os.makedirs(output_folder, exist_ok=True)

    _ensure_prefix_info(output_folder, prefix)
    csv_path = _ensure_scan_csv(output_folder, script_start_time)

    config = configparser.ConfigParser()
    config.read(os.path.join(SCRIPT_DIR, "var.ini"))
    enable_dns = config.getboolean("scan_options", "enable_dns", fallback=True)

    cmd = [
        "nmap", "-sn", "-T4", "--min-parallelism", "10", "--max-retries", "2",
        "-R" if enable_dns else "-n",
        prefix
    ]

    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = proc.communicate(timeout=NMAP_TIMEOUT)

        results: List[ScanResult] = []
        for line in stdout.splitlines():
            if "Nmap scan report for" in line:
                r = _parse_nmap_line(line, prefix, tenant, VRF)
                if r:
                    results.append(r)

        with FILE_LOCK:
            with open(csv_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDNAMES)
                for r in results:
                    writer.writerow(vars(r))

        return results, True

    except Exception as e:
        logger.error("Scan failed for %s: %s", prefix, e, exc_info=True)
        return [], False
