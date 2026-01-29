#!/usr/bin/env python3
"""
Per-prefix Nmap scanner.

- Reads scan options from var.ini [scan_options]:
    enable_dns
    enable_scantime

Logging:
- Inherits handlers configured by the scheduler (root logger).
"""

from __future__ import annotations

import configparser
import csv
import logging
import os
import re
import subprocess
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# `nmap` can be slow on large prefixes; keep a hard cap per prefix scan.
NMAP_TIMEOUT_SECONDS = 300

_FILE_LOCK = threading.Lock()
OUTPUT_FIELDNAMES = ["address", "dns_name", "status", "tags", "tenant", "VRF", "scantime"]

_NMAP_REPORT_RE = re.compile(r"^Nmap scan report for (?:(?P<host>.+?)\s+\((?P<ip>.+?)\)|(?P<ip_only>.+))$")

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ScanResult:
    address: str
    dns_name: Optional[str]
    status: str
    tags: str
    tenant: str
    VRF: str
    scantime: str


def _read_scan_options() -> tuple[bool, bool]:
    config = configparser.ConfigParser()
    config.read(os.path.join(SCRIPT_DIR, "var.ini"))
    enable_dns = config.getboolean("scan_options", "enable_dns", fallback=True)
    enable_scantime = config.getboolean("scan_options", "enable_scantime", fallback=True)
    return enable_dns, enable_scantime


def _ensure_prefix_info(folder: str, prefix: str) -> None:
    os.makedirs(folder, exist_ok=True)
    with open(os.path.join(folder, "prefix.info"), "w", encoding="utf-8") as f:
        f.write(prefix.strip() + "\n")


def _ensure_scan_csv(folder: str, ts: datetime) -> str:
    os.makedirs(folder, exist_ok=True)
    filename = f"nmap_results_{ts.strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    path = os.path.join(folder, filename)

    with _FILE_LOCK:
        if not os.path.exists(path):
            with open(path, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=OUTPUT_FIELDNAMES).writeheader()

    return path


def _parse_nmap_report_line(
    line: str,
    prefix: str,
    tenant: str,
    vrf: str,
    enable_scantime: bool,
) -> Optional[ScanResult]:
    m = _NMAP_REPORT_RE.match(line.strip())
    if not m:
        return None

    ip = (m.group("ip") or m.group("ip_only") or "").strip()
    host = (m.group("host") or "").strip() or None
    if not ip:
        return None

    mask = prefix.split("/")[-1]
    return ScanResult(
        address=f"{ip}/{mask}",
        dns_name=host,
        status="active",
        tags="autoscan",
        tenant=tenant,
        VRF=vrf,
        scantime=datetime.now().strftime("%Y-%m-%d %H:%M:%S") if enable_scantime else "",
    )


def run_nmap_on_prefix(
    *,
    prefix: str,
    tenant: str,
    VRF: str,
    script_start_time: datetime,
    input_filename=None,  # kept for backward compatibility
    output_folder: str,
    remove_from_input: bool = False,  # kept for backward compatibility
) -> Tuple[List[ScanResult], bool]:
    del input_filename, remove_from_input

    enable_dns, enable_scantime = _read_scan_options()

    _ensure_prefix_info(output_folder, prefix)
    csv_path = _ensure_scan_csv(output_folder, script_start_time)

    cmd = [
        "nmap",
        "-sn",
        "-T4",
        "--min-parallelism",
        "10",
        "--max-retries",
        "2",
        "-R" if enable_dns else "-n",
        prefix,
    ]

    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = proc.communicate(timeout=NMAP_TIMEOUT_SECONDS)

        if proc.returncode not in (0, None) and stderr:
            logger.warning("nmap returned code %s for %s. stderr=%s", proc.returncode, prefix, stderr.strip())

        results: List[ScanResult] = []
        for line in stdout.splitlines():
            if line.startswith("Nmap scan report for "):
                r = _parse_nmap_report_line(line, prefix, tenant, VRF, enable_scantime)
                if r:
                    results.append(r)

        with _FILE_LOCK:
            with open(csv_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDNAMES)
                for r in results:
                    writer.writerow(
                        {
                            "address": r.address,
                            "dns_name": r.dns_name or "",
                            "status": r.status,
                            "tags": r.tags,
                            "tenant": r.tenant,
                            "VRF": r.VRF,
                            "scantime": r.scantime,
                        }
                    )

        return results, True

    except subprocess.TimeoutExpired:
        logger.error("nmap timed out after %ss for prefix %s", NMAP_TIMEOUT_SECONDS, prefix, exc_info=True)
        return [], False
    except FileNotFoundError:
        logger.error("nmap binary not found on PATH; cannot scan prefix %s", prefix, exc_info=True)
        return [], False
    except Exception:
        logger.error("Scan failed for prefix %s", prefix, exc_info=True)
        return [], False
