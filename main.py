#!/usr/bin/env python3
"""main.py

NetBox Nmap scan scheduler (concurrent, per-prefix folders).

High-level pipeline
-------------------
This script is the long-running orchestrator that keeps NetBox IP Address records aligned
with periodic network discovery.

For each scan cycle it:

1) Loads configuration from ``var.ini``.
2) Connects to NetBox and retrieves eligible prefixes.
3) Ensures a folder exists per (prefix, VRF) under ``PREFIXES/``.
4) Decides which prefixes are *due* for scanning (based on the last scan timestamp and
   the effective scan interval).
5) Runs prefix scans concurrently using a thread pool:
      - Nmap discovery (``network_scan``)
      - Merge with previous scan and produce ``ipam_addresses.csv`` (``scan_processor``)
      - Import into NetBox (``netbox_import``)
6) Performs housekeeping:
      - Deletes stale ``ipam_addresses.csv`` in prefix folders.
      - Keeps only the most recent ``nmap_results_*.csv`` files (configurable).

Prefix eligibility rules
------------------------
A prefix is scanned only when:
- NetBox prefix status is ``active``; and
- It does *not* have the tag ``Disable Automatic Scanning``; and
- Optional custom-field rules (controlled via ``var.ini``) do not exclude it.

Optional behavior toggles (var.ini)
-----------------------------------
Under ``[scan_options]``:

- ``use_scanrm_cf = true``:
    Uses the NetBox prefix custom field ``scanrm`` (boolean).
    If truthy => the prefix is excluded from scanning.

- ``per_prefix_scan_interval = true``:
    Uses the NetBox prefix custom field ``scaninterval`` (hours).
    If set to a valid >0 integer => overrides the global scan interval for that prefix.

If ``scaninterval`` is missing or invalid, the global ``scan_interval_hours`` is used.

On-disk state (PREFIXES/<prefix>__<vrf>/)
-----------------------------------------
Each prefix folder contains:

- ``prefix.info``:
    Stores the canonical prefix and the effective scan interval used for due decisions.
    Format::

        prefix=<CIDR>
        scan_interval_hours=<hours>

    Legacy support:
        Older deployments stored a single CIDR line only. Those files are detected and
        automatically migrated to the key/value format.

- ``nmap_results_<timestamp>.csv``:
    Raw scan observations produced by :mod:`network_scan`.

- ``ipam_addresses.csv``:
    NetBox-ready CSV produced by :mod:`scan_processor` (deleted during cleanup to avoid
    re-importing stale data).

Concurrency and safety
----------------------
- Scans are executed via a ``ThreadPoolExecutor`` (worker count configurable).
- A guard set (``_IN_PROGRESS``) prevents the same (prefix, VRF) from being scanned twice
  concurrently if it appears multiple times in a cycle.
- File writes inside scanning and processing modules handle their own local locking.

Logging
-------
Logging is configured at import time for this script via :func:`logging_utils.configure_logging`.
All other modules use ``logging.getLogger(__name__)`` and inherit these handlers.

"""

from __future__ import annotations

import configparser
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import netbox_connection
import netbox_export
import netbox_import
import network_scan
import scan_processor
from logging_utils import configure_logging

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PREFIXES_DIR = os.path.join(SCRIPT_DIR, "PREFIXES")
os.makedirs(PREFIXES_DIR, exist_ok=True)

configure_logging(app_name="scheduler", script_dir=SCRIPT_DIR)
logger = logging.getLogger(__name__)

_IN_PROGRESS: set[str] = set()
_IN_PROGRESS_LOCK = threading.Lock()


def load_scheduler_config() -> Tuple[str, str, int, bool, bool, int, int, int]:
    """Load scheduler configuration from var.ini."""
    config_path = os.path.join(SCRIPT_DIR, "var.ini")

    config = configparser.ConfigParser()
    if not config.read(config_path):
        raise RuntimeError(f"Configuration file not found: {config_path}")

    url = config["credentials"]["url"]
    token = config["credentials"]["token"]

    scan_interval_hours = config.getint("scan_options", "scan_interval_hours", fallback=4)
    per_prefix_scan_interval = config.getboolean("scan_options", "per_prefix_scan_interval", fallback=False)

    # Independent switch for scanrm custom field
    use_scanrm_cf = config.getboolean("scan_options", "use_scanrm_cf", fallback=False)

    scheduler_sleep_seconds = config.getint("scan_options", "scheduler_sleep_seconds", fallback=300)
    scan_max_workers = config.getint("scan_options", "scan_max_workers", fallback=5)

    keep_last = config.getint("scan_options", "nmap_results_keep_last", fallback=4)

    return (
        url,
        token,
        max(1, scan_interval_hours),
        bool(per_prefix_scan_interval),
        bool(use_scanrm_cf),
        max(5, scheduler_sleep_seconds),
        max(1, scan_max_workers),
        max(2, keep_last),
    )


def sanitize_prefix_folder_name(prefix: str, vrf: str) -> str:
    """Convert (prefix, vrf) into a filesystem-safe folder name."""
    folder = prefix.replace("/", "_").replace(":", "_")
    vrf_part = vrf if vrf and vrf != "N/A" else ""
    if vrf_part:
        folder = f"{folder}__{vrf_part}"
    return folder.replace(os.sep, "_")


def _parse_bool(value) -> bool:
    """Best-effort boolean coercion for Netbox custom field values."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


def _parse_interval_hours(value) -> int | None:
    """Best-effort int hours coercion. Returns None if missing/invalid."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            iv = int(value)
            return iv if iv > 0 else None
        except Exception:
            return None
    s = str(value).strip()
    if not s:
        return None
    try:
        iv = int(float(s))
        return iv if iv > 0 else None
    except Exception:
        return None


def get_active_prefixes(
    netbox_instance,
    *,
    per_prefix_scan_interval: bool,
    use_scanrm_cf: bool,
    default_scan_interval_hours: int,
) -> List[Dict[str, str]]:
    """Retrieve prefixes eligible for scanning."""
    logger.info("Retrieving prefixes from Netbox")
    prefixes = netbox_export.get_ipam_prefixes(netbox_instance)

    active: List[Dict[str, str]] = []
    for p in prefixes:
        status_value = p.status.value if getattr(p, "status", None) else "N/A"
        if status_value != "active":
            continue

        tag_names = [getattr(tag, "name", str(tag)) for tag in getattr(p, "tags", []) or []]
        if "Disable Automatic Scanning" in tag_names:
            continue

        cf = getattr(p, "custom_fields", None) or {}

        if use_scanrm_cf and _parse_bool(cf.get("scanrm")):
            continue

        interval_hours = int(default_scan_interval_hours)
        if per_prefix_scan_interval:
            per_val = _parse_interval_hours(cf.get("scaninterval"))
            if per_val is not None:
                interval_hours = int(per_val)

        tenant_name = p.tenant.name if getattr(p, "tenant", None) else "N/A"
        vrf_name = p.vrf.name if getattr(p, "vrf", None) else "N/A"
        active.append(
            {
                "prefix": p.prefix,
                "tenant": tenant_name,
                "vrf": vrf_name,
                "interval_hours": int(interval_hours),
            }
        )

    logger.info("Active prefixes eligible for scan: %d", len(active))
    return active


def write_prefix_info(prefix_dir: str, prefix: str, interval_hours: int) -> None:
    """Write/overwrite prefix.info with prefix and effective scan interval."""
    os.makedirs(prefix_dir, exist_ok=True)
    info_path = os.path.join(prefix_dir, "prefix.info")
    with open(info_path, "w", encoding="utf-8") as f:
        f.write(f"prefix={prefix.strip()}\n")
        f.write(f"scan_interval_hours={max(1, int(interval_hours))}\n")


def read_prefix_info(prefix_dir: str) -> tuple[str | None, int | None, bool]:
    """
    Read prefix.info.

    Returns: (prefix, interval_hours, is_legacy)

    Supports:
      - New format:
          prefix=...
          scan_interval_hours=...
      - Legacy format:
          <CIDR> (single line)
    """
    info_path = os.path.join(prefix_dir, "prefix.info")
    if not os.path.exists(info_path):
        return None, None, False

    try:
        with open(info_path, "r", encoding="utf-8") as f:
            lines = [ln.strip() for ln in f.readlines() if ln.strip()]
    except Exception:
        return None, None, False

    if not lines:
        return None, None, False

    # Legacy single line CIDR
    if len(lines) == 1 and "=" not in lines[0]:
        return lines[0], None, True

    prefix = None
    interval = None
    for ln in lines:
        if ln.startswith("prefix="):
            prefix = ln.split("=", 1)[1].strip() or None
        elif ln.startswith("scan_interval_hours="):
            raw = ln.split("=", 1)[1].strip()
            try:
                iv = int(raw)
                if iv > 0:
                    interval = iv
            except Exception:
                pass

    return prefix, interval, False


def _parse_scan_timestamp_from_filename(filename: str) -> datetime | None:
    """Extract timestamp from nmap_results_<YYYY-MM-DD_HH-MM-SS>.csv."""
    if not (filename.startswith("nmap_results_") and filename.endswith(".csv")):
        return None
    ts_str = filename[len("nmap_results_") : -len(".csv")]
    try:
        return datetime.strptime(ts_str, "%Y-%m-%d_%H-%M-%S")
    except ValueError:
        return None


def _list_scan_files_sorted(prefix_dir: str) -> List[str]:
    """Return nmap_results_*.csv sorted newest-first."""
    candidates: List[str] = []
    for name in os.listdir(prefix_dir):
        if name.startswith("nmap_results_") and name.endswith(".csv"):
            candidates.append(name)

    def sort_key(name: str):
        ts = _parse_scan_timestamp_from_filename(name)
        if ts is not None:
            return ts
        try:
            return datetime.fromtimestamp(os.path.getmtime(os.path.join(prefix_dir, name)))
        except OSError:
            return datetime.min

    candidates.sort(key=sort_key, reverse=True)
    return candidates


def cleanup_prefix_folder(prefix_dir: str, keep_last: int) -> tuple[int, int]:
    """Cleanup: keep newest N scan CSVs; delete ipam_addresses.csv."""
    deleted_scans = 0
    deleted_ipam = 0

    ipam_path = os.path.join(prefix_dir, "ipam_addresses.csv")
    if os.path.exists(ipam_path):
        try:
            os.remove(ipam_path)
            deleted_ipam = 1
            logger.debug("Deleted stale ipam file: %s", ipam_path)
        except OSError:
            logger.warning("Failed to delete %s", ipam_path, exc_info=True)

    try:
        scans = _list_scan_files_sorted(prefix_dir)
    except FileNotFoundError:
        return 0, deleted_ipam

    for name in scans[keep_last:]:
        path = os.path.join(prefix_dir, name)
        try:
            os.remove(path)
            deleted_scans += 1
            logger.debug("Deleted old scan file: %s", path)
        except OSError:
            logger.warning("Failed to delete old scan file: %s", path, exc_info=True)

    return deleted_scans, deleted_ipam


def cleanup_all_prefix_folders(keep_last: int) -> None:
    """Run cleanup across all prefix folders."""
    try:
        entries = [os.path.join(PREFIXES_DIR, d) for d in os.listdir(PREFIXES_DIR)]
    except FileNotFoundError:
        return

    total_scan_deleted = 0
    total_ipam_deleted = 0

    for d in entries:
        if not os.path.isdir(d):
            continue
        ds, di = cleanup_prefix_folder(d, keep_last=keep_last)
        total_scan_deleted += ds
        total_ipam_deleted += di

    logger.info(
        "Cleanup: deleted %d old nmap_results file(s); removed %d ipam_addresses.csv file(s); keep_last=%d",
        total_scan_deleted,
        total_ipam_deleted,
        keep_last,
    )


def get_last_scan_time(prefix_dir: str) -> datetime | None:
    """Determine last scan time from the newest nmap_results_*.csv."""
    try:
        scans = _list_scan_files_sorted(prefix_dir)
    except FileNotFoundError:
        return None

    if not scans:
        return None

    ts = _parse_scan_timestamp_from_filename(scans[0])
    if ts is not None:
        return ts

    return datetime.fromtimestamp(os.path.getmtime(os.path.join(prefix_dir, scans[0])))


def is_scan_due(prefix_dir: str, fallback_interval_hours: int, expected_prefix: str | None, expected_interval: int | None) -> bool:
    """
    Due decision uses interval stored in prefix.info.
    Self-heal:
      - If legacy/missing interval OR mismatch with expected values, rewrite prefix.info.
    """
    stored_prefix, stored_interval, is_legacy = read_prefix_info(prefix_dir)

    eff_interval = stored_interval if stored_interval is not None else int(fallback_interval_hours)

    # If we know expected values (from NetBox), keep prefix.info correct and migrate legacy.
    if expected_prefix is not None and expected_interval is not None:
        needs_rewrite = (
            is_legacy
            or stored_interval is None
            or stored_prefix is None
            or stored_prefix != expected_prefix
            or stored_interval != int(expected_interval)
        )
        if needs_rewrite:
            try:
                write_prefix_info(prefix_dir, expected_prefix, int(expected_interval))
                eff_interval = int(expected_interval)
                if is_legacy or stored_interval is None:
                    logger.info("Migrated prefix.info to include scan interval for %s", expected_prefix)
            except Exception:
                logger.warning("Failed to rewrite prefix.info in %s", prefix_dir, exc_info=True)

    last = get_last_scan_time(prefix_dir)
    if last is None:
        return True
    return datetime.now() - last >= timedelta(hours=eff_interval)


def precreate_prefix_folders(prefixes: List[Dict[str, str]]) -> None:
    """Create all prefix folders and prefix.info first (including effective interval)."""
    logger.info("Pre-creating prefix folders and prefix.info files")

    created = 0
    updated = 0

    for p in prefixes:
        prefix = p["prefix"]
        vrf = p["vrf"]
        interval_hours = int(p.get("interval_hours", 1)) or 1

        folder_name = sanitize_prefix_folder_name(prefix, vrf)
        prefix_dir = os.path.join(PREFIXES_DIR, folder_name)

        existed = os.path.isdir(prefix_dir)
        os.makedirs(prefix_dir, exist_ok=True)

        try:
            write_prefix_info(prefix_dir, prefix, interval_hours)
        except Exception:
            logger.error("Failed to write prefix.info for %s (VRF=%s)", prefix, vrf, exc_info=True)
            continue

        updated += int(existed)
        created += int(not existed)

    logger.info("Prefix folders prepared: created=%d, updated=%d", created, updated)


def _prefix_task_key(prefix: str, vrf: str) -> str:
    """Build a stable in-memory key for in-progress scan suppression.

    The scheduler may iterate over the prefix list multiple times within a cycle, and
    worker threads run concurrently. This key is used in ``_IN_PROGRESS`` to ensure that
    at most one worker per (prefix, VRF) is active at a time.

    Args:
        prefix: CIDR string.
        vrf: VRF name (or 'N/A').

    Returns:
        A string key in the form ``<prefix>__<vrf>``.
    """
    return f"{prefix}__{vrf or 'N/A'}"


def run_scan_for_prefix(url: str, token: str, prefix_info: Dict[str, str]) -> None:
    """Worker task: scan + process + import for a single prefix folder."""
    prefix = prefix_info["prefix"]
    tenant = prefix_info["tenant"]
    vrf = prefix_info["vrf"]
    interval_hours = int(prefix_info.get("interval_hours", 1)) or 1

    folder_name = sanitize_prefix_folder_name(prefix, vrf)
    prefix_dir = os.path.join(PREFIXES_DIR, folder_name)
    os.makedirs(prefix_dir, exist_ok=True)

    # Keep prefix.info up-to-date (including effective interval)
    write_prefix_info(prefix_dir, prefix, interval_hours)

    scan_start = datetime.now()
    logger.info("Starting scan for prefix %s (VRF=%s)", prefix, vrf)

    results, success = network_scan.run_nmap_on_prefix(
        prefix=prefix,
        tenant=tenant,
        VRF=vrf,
        script_start_time=scan_start,
        output_folder=prefix_dir,
    )

    if not success:
        logger.error("Scan failed for prefix %s (VRF=%s)", prefix, vrf)
        return

    logger.info("Scan completed for prefix %s (VRF=%s): %d active host(s)", prefix, vrf, len(results))

    output_csv = os.path.join(prefix_dir, "ipam_addresses.csv")
    scan_processor.process_scan_results_in_dir(prefix_dir, output_csv)

    logger.info("Importing scan results into Netbox for prefix %s (VRF=%s)", prefix, vrf)
    netbox_import.write_data_to_netbox(url, token, output_csv)


def main() -> None:
    logger.info("Starting Netbox Nmap scan scheduler")

    try:
        (
            url,
            token,
            scan_interval_hours,
            per_prefix_scan_interval,
            use_scanrm_cf,
            scheduler_sleep_seconds,
            scan_max_workers,
            keep_last,
        ) = load_scheduler_config()
    except Exception:
        logger.error("Failed to load configuration", exc_info=True)
        sys.exit(1)

    logger.info(
        "Config: scan_interval=%dh | per_prefix_scan_interval=%s | use_scanrm_cf=%s | sleep=%ds | workers=%d | keep_last_scans=%d",
        scan_interval_hours,
        per_prefix_scan_interval,
        use_scanrm_cf,
        scheduler_sleep_seconds,
        scan_max_workers,
        keep_last,
    )

    executor = ThreadPoolExecutor(max_workers=scan_max_workers)

    try:
        while True:
            try:
                logger.info("Connecting to Netbox...")
                netbox_instance = netbox_connection.connect_to_netbox(url, token)
                logger.info("Connected to Netbox")

                prefixes = get_active_prefixes(
                    netbox_instance,
                    per_prefix_scan_interval=per_prefix_scan_interval,
                    use_scanrm_cf=use_scanrm_cf,
                    default_scan_interval_hours=scan_interval_hours,
                )

                precreate_prefix_folders(prefixes)
                cleanup_all_prefix_folders(keep_last)

                futures = []
                scheduled_count = 0

                for p in prefixes:
                    prefix = p["prefix"]
                    vrf = p["vrf"]

                    folder_name = sanitize_prefix_folder_name(prefix, vrf)
                    prefix_dir = os.path.join(PREFIXES_DIR, folder_name)

                    # Due decision uses prefix.info and will auto-migrate legacy/missing interval.
                    if not is_scan_due(
                        prefix_dir,
                        scan_interval_hours,
                        expected_prefix=prefix,
                        expected_interval=int(p.get("interval_hours", scan_interval_hours)),
                    ):
                        continue

                    key = _prefix_task_key(prefix, vrf)
                    with _IN_PROGRESS_LOCK:
                        if key in _IN_PROGRESS:
                            continue
                        _IN_PROGRESS.add(key)

                    fut = executor.submit(run_scan_for_prefix, url, token, p)
                    fut._prefix_key = key  # type: ignore[attr-defined]
                    fut._prefix = prefix  # type: ignore[attr-defined]
                    fut._vrf = vrf  # type: ignore[attr-defined]
                    futures.append(fut)
                    scheduled_count += 1

                logger.info("Scheduled %d prefix scan(s) this cycle", scheduled_count)

                for fut in as_completed(futures):
                    key = getattr(fut, "_prefix_key", None)
                    prefix = getattr(fut, "_prefix", "unknown")
                    vrf = getattr(fut, "_vrf", "unknown")

                    try:
                        fut.result()
                    except Exception:
                        logger.error("Unhandled error while processing prefix %s (VRF=%s)", prefix, vrf, exc_info=True)
                    finally:
                        if key:
                            with _IN_PROGRESS_LOCK:
                                _IN_PROGRESS.discard(key)

            except KeyboardInterrupt:
                logger.info("Scheduler interrupted by user, exiting.")
                break
            except Exception:
                logger.error("Unexpected scheduler cycle error", exc_info=True)

            logger.info("Sleeping %d seconds", scheduler_sleep_seconds)
            time.sleep(scheduler_sleep_seconds)

    finally:
        executor.shutdown(wait=True)


if __name__ == "__main__":
    main()
