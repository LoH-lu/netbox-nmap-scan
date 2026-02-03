#!/usr/bin/env python3
"""network_scan.py

Per-prefix Nmap scanner.

This module performs an *ICMP/ARP ping sweep* (``nmap -sn``) of a given CIDR prefix and
writes results to a timestamped CSV file in a prefix-specific folder.

Key behaviors
-------------
- Scan options are read from ``var.ini`` under ``[scan_options]``:
    - ``enable_dns`` (bool): whether to resolve DNS names (``-R``) or disable it (``-n``)
    - ``enable_scantime`` (bool): whether to write a timestamp per discovered host
- Results are appended to a CSV named::

      nmap_results_<YYYY-MM-DD_HH-MM-SS>.csv

- Thread safety:
    - A module-level lock guards file creation and appends, so multiple worker threads
      can scan different prefixes concurrently without corrupting CSV output.

Important integration note
--------------------------
This module intentionally does *not* own ``prefix.info``. The scheduler (``main.py``)
may store additional metadata there (e.g. effective scan interval). For that reason,
:func:`_ensure_prefix_info` will only create a minimal file if missing and will never
overwrite an existing one.

Logging
-------
This is library code; it uses ``logging.getLogger(__name__)`` and inherits handlers from
the entrypoint (scheduler).
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

# `nmap` can be slow on large prefixes; keep a hard cap per prefix scan to avoid
# indefinitely stuck scheduler workers.
NMAP_TIMEOUT_SECONDS = 300

# CSV writes are shared between worker threads; protect them with a single lock.
_FILE_LOCK = threading.Lock()

# Output schema used by both scan output and later processing/import stages.
OUTPUT_FIELDNAMES = ["address", "dns_name", "status", "tags", "tenant", "VRF", "scantime"]

# Example Nmap lines:
#   "Nmap scan report for host.example (10.0.0.1)"
#   "Nmap scan report for 10.0.0.2"
_NMAP_REPORT_RE = re.compile(
    r"^Nmap scan report for (?:(?P<host>.+?)\s+\((?P<ip>.+?)\)|(?P<ip_only>.+))$"
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ScanResult:
    """One discovered host record produced from a scan.

    Attributes:
        address: Host IP with the same mask as the scanned prefix (e.g. ``10.0.0.1/24``).
        dns_name: Optional reverse DNS name (only present when ``enable_dns`` is True and
            DNS resolution succeeded).
        status: NetBox IP status string written to the scan CSV (typically ``active``).
        tags: Tag(s) used later by NetBox import (e.g. ``autoscan``).
        tenant: Tenant name associated with the prefix at scan time.
        VRF: VRF name associated with the prefix at scan time (or ``N/A``).
        scantime: Timestamp string when the host was recorded (optional; controlled by var.ini).
    """

    address: str
    dns_name: Optional[str]
    status: str
    tags: str
    tenant: str
    VRF: str
    scantime: str


def _read_scan_options() -> tuple[bool, bool]:
    """Read scanner feature toggles from var.ini.

    Returns:
        (enable_dns, enable_scantime)
    """
    config = configparser.ConfigParser()
    config.read(os.path.join(SCRIPT_DIR, "var.ini"))
    enable_dns = config.getboolean("scan_options", "enable_dns", fallback=True)
    enable_scantime = config.getboolean("scan_options", "enable_scantime", fallback=True)
    return enable_dns, enable_scantime


def _ensure_prefix_info(folder: str, prefix: str) -> None:
    """Ensure a minimal ``prefix.info`` exists without overwriting.

    The scheduler (``main.py``) owns the full content of ``prefix.info`` and may store
    extra metadata such as ``scan_interval_hours``. Overwriting here would silently delete
    that metadata, so this function only creates the file when it does not exist.

    Args:
        folder: Prefix output folder (created if missing).
        prefix: CIDR string to write if the file is missing.

    Side effects:
        May create ``<folder>/prefix.info`` containing one line ``prefix=<CIDR>``.
    """
    os.makedirs(folder, exist_ok=True)
    info_path = os.path.join(folder, "prefix.info")

    if os.path.exists(info_path):
        return

    try:
        with open(info_path, "w", encoding="utf-8") as f:
            f.write(f"prefix={prefix.strip()}\n")
    except Exception:
        logger.warning("Failed to create %s", info_path, exc_info=True)


def _ensure_scan_csv(folder: str, ts: datetime) -> str:
    """Create the scan CSV for a given scan timestamp, if missing.

    Args:
        folder: Prefix folder.
        ts: Scan timestamp used to build the output filename.

    Returns:
        Absolute path to the CSV file.

    Thread safety:
        Creation is protected by ``_FILE_LOCK`` to avoid two threads attempting to
        create/write the header at the same time.
    """
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
    """Parse an ``nmap`` output line into a :class:`ScanResult`.

    Args:
        line: Raw stdout line from ``nmap``.
        prefix: Scanned prefix (used to apply the same mask to each discovered host).
        tenant: Tenant name to associate with this scan result.
        vrf: VRF name to associate with this scan result.
        enable_scantime: Whether to include a timestamp.

    Returns:
        A :class:`ScanResult` when the line matches the expected format, else ``None``.
    """
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
    """Run an Nmap ping scan on a prefix and append results to CSV.

    Args:
        prefix: Target CIDR (e.g. ``192.0.2.0/24``).
        tenant: Tenant name to store with each discovered host record.
        VRF: VRF name to store with each discovered host record.
        script_start_time: Timestamp used to name the CSV output file. This allows the
            scheduler to group all results from a scan cycle into a single file per prefix.
        input_filename: Legacy/unused parameter kept for API compatibility.
        output_folder: Folder where scan outputs are written.
        remove_from_input: Legacy/unused parameter kept for API compatibility.

    Returns:
        (results, success)

        - results: list of :class:`ScanResult` for each discovered host
        - success: True when the scan ran to completion and output was written

    Failure modes:
        - Timeout -> returns ``([], False)``
        - Missing nmap binary -> returns ``([], False)``
        - Any unexpected exception -> returns ``([], False)``

    Notes:
        ``nmap`` is invoked with:
        - ``-sn``: host discovery only (no port scan)
        - ``-T4``: faster timing template
        - ``--min-parallelism 10`` and ``--max-retries 2``: keep scans bounded
        - ``-R`` or ``-n`` depending on DNS option
    """
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

        # Append results to the scan CSV. File writes are protected by the lock.
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
