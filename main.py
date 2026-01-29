#!/usr/bin/env python3
"""main.py

Netbox Nmap scan scheduler (concurrent, per-prefix folders).

Per scheduler cycle:
1) Connect to Netbox
2) Fetch *active* prefixes
   - Prefixes tagged "Disable Automatic Scanning" are skipped
3) Pre-create ALL prefix folders under ./PREFIXES/ and write prefix.info for each
4) Cleanup per-prefix artifacts:
   - keep only the latest N scan CSVs (nmap_results_*.csv), delete the rest
   - delete ipam_addresses.csv so it can never be stale when a prefix isn't scanned
5) Schedule concurrent scans only for prefixes that are due (per scan_interval_hours)
6) For each scanned prefix:
   - network_scan writes nmap_results_<timestamp>.csv (header-only if no hosts)
   - scan_processor writes ipam_addresses.csv (latest computed view)
   - netbox_import imports per-prefix ipam_addresses.csv into Netbox

Configuration (var.ini):
[scan_options]
scan_interval_hours = 4
scheduler_sleep_seconds = 300
scan_max_workers = 5
nmap_results_keep_last = 4
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

# Configure root logger once so all modules inherit consistent handlers.
configure_logging(app_name="scheduler", script_dir=SCRIPT_DIR)
logger = logging.getLogger(__name__)

# Guard to avoid scheduling the same prefix concurrently.
_IN_PROGRESS: set[str] = set()
_IN_PROGRESS_LOCK = threading.Lock()


def load_scheduler_config() -> Tuple[str, str, int, int, int, int]:
    """Load scheduler configuration from var.ini."""
    config_path = os.path.join(SCRIPT_DIR, "var.ini")

    config = configparser.ConfigParser()
    if not config.read(config_path):
        raise RuntimeError(f"Configuration file not found: {config_path}")

    url = config["credentials"]["url"]
    token = config["credentials"]["token"]

    scan_interval_hours = config.getint("scan_options", "scan_interval_hours", fallback=4)
    scheduler_sleep_seconds = config.getint("scan_options", "scheduler_sleep_seconds", fallback=300)
    scan_max_workers = config.getint("scan_options", "scan_max_workers", fallback=5)

    # New: keep only the latest N scan CSVs per prefix
    keep_last = config.getint("scan_options", "nmap_results_keep_last", fallback=4)

    return (
        url,
        token,
        max(1, scan_interval_hours),
        max(5, scheduler_sleep_seconds),
        max(1, scan_max_workers),
        max(2, keep_last),  # keep at least 2 so diff logic can work
    )


def sanitize_prefix_folder_name(prefix: str, vrf: str) -> str:
    """Convert (prefix, vrf) into a filesystem-safe folder name."""
    folder = prefix.replace("/", "_").replace(":", "_")
    vrf_part = vrf if vrf and vrf != "N/A" else ""
    if vrf_part:
        folder = f"{folder}__{vrf_part}"
    return folder.replace(os.sep, "_")


def get_active_prefixes(netbox_instance) -> List[Dict[str, str]]:
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

        tenant_name = p.tenant.name if getattr(p, "tenant", None) else "N/A"
        vrf_name = p.vrf.name if getattr(p, "vrf", None) else "N/A"
        active.append({"prefix": p.prefix, "tenant": tenant_name, "vrf": vrf_name})

    logger.info("Active prefixes eligible for scan: %d", len(active))
    return active


def write_prefix_info(prefix_dir: str, prefix: str) -> None:
    """Write/overwrite prefix.info with the prefix CIDR."""
    os.makedirs(prefix_dir, exist_ok=True)
    with open(os.path.join(prefix_dir, "prefix.info"), "w", encoding="utf-8") as f:
        f.write(prefix.strip() + "\n")


def precreate_prefix_folders(prefixes: List[Dict[str, str]]) -> None:
    """Create all prefix folders and prefix.info first."""
    logger.info("Pre-creating prefix folders and prefix.info files")

    created = 0
    updated = 0

    for p in prefixes:
        prefix = p["prefix"]
        vrf = p["vrf"]

        folder_name = sanitize_prefix_folder_name(prefix, vrf)
        prefix_dir = os.path.join(PREFIXES_DIR, folder_name)

        existed = os.path.isdir(prefix_dir)
        os.makedirs(prefix_dir, exist_ok=True)

        try:
            write_prefix_info(prefix_dir, prefix)
        except Exception:
            logger.error("Failed to write prefix.info for %s (VRF=%s)", prefix, vrf, exc_info=True)
            continue

        updated += int(existed)
        created += int(not existed)

    logger.info("Prefix folders prepared: created=%d, updated=%d", created, updated)


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
    """
    Return nmap_results_*.csv sorted newest-first using filename timestamp
    (fallback to mtime if timestamp parsing fails).
    """
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
    """
    Cleanup a single prefix folder.

    - Keep only the newest `keep_last` nmap_results_*.csv files
    - Delete ipam_addresses.csv to avoid stale state when no scan occurs

    Returns:
        (deleted_scan_files, deleted_ipam_files)
    """
    deleted_scans = 0
    deleted_ipam = 0

    # 1) Remove stale ipam_addresses.csv (force "latest or nothing")
    ipam_path = os.path.join(prefix_dir, "ipam_addresses.csv")
    if os.path.exists(ipam_path):
        try:
            os.remove(ipam_path)
            deleted_ipam = 1
            logger.debug("Deleted stale ipam file: %s", ipam_path)
        except OSError:
            logger.warning("Failed to delete %s", ipam_path, exc_info=True)

    # 2) Keep only the newest N scan files
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

    # Fallback: mtime
    return datetime.fromtimestamp(os.path.getmtime(os.path.join(prefix_dir, scans[0])))


def is_scan_due(prefix_dir: str, interval_hours: int) -> bool:
    """Return True if never scanned or interval has elapsed."""
    last = get_last_scan_time(prefix_dir)
    if last is None:
        return True
    return datetime.now() - last >= timedelta(hours=interval_hours)


def _prefix_task_key(prefix: str, vrf: str) -> str:
    return f"{prefix}__{vrf or 'N/A'}"


def run_scan_for_prefix(url: str, token: str, prefix_info: Dict[str, str]) -> None:
    """Worker task: scan + process + import for a single prefix folder."""
    prefix = prefix_info["prefix"]
    tenant = prefix_info["tenant"]
    vrf = prefix_info["vrf"]

    folder_name = sanitize_prefix_folder_name(prefix, vrf)
    prefix_dir = os.path.join(PREFIXES_DIR, folder_name)
    os.makedirs(prefix_dir, exist_ok=True)
    write_prefix_info(prefix_dir, prefix)

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
        url, token, scan_interval_hours, scheduler_sleep_seconds, scan_max_workers, keep_last = load_scheduler_config()
    except Exception:
        logger.error("Failed to load configuration", exc_info=True)
        sys.exit(1)

    logger.info(
        "Config: scan_interval=%dh | sleep=%ds | workers=%d | keep_last_scans=%d",
        scan_interval_hours,
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

                prefixes = get_active_prefixes(netbox_instance)

                # 1) Ensure all folders exist and have prefix.info
                precreate_prefix_folders(prefixes)

                # 2) Cleanup:
                #    - keep only newest N scan files per prefix
                #    - delete ipam_addresses.csv to prevent stale state
                cleanup_all_prefix_folders(keep_last)

                # 3) Schedule due scans
                futures = []
                scheduled_count = 0

                for p in prefixes:
                    prefix = p["prefix"]
                    vrf = p["vrf"]

                    folder_name = sanitize_prefix_folder_name(prefix, vrf)
                    prefix_dir = os.path.join(PREFIXES_DIR, folder_name)

                    if not is_scan_due(prefix_dir, scan_interval_hours):
                        continue

                    key = _prefix_task_key(prefix, vrf)
                    with _IN_PROGRESS_LOCK:
                        if key in _IN_PROGRESS:
                            continue
                        _IN_PROGRESS.add(key)

                    fut = executor.submit(run_scan_for_prefix, url, token, p)
                    fut._prefix_key = key  # type: ignore[attr-defined]
                    fut._prefix = prefix   # type: ignore[attr-defined]
                    fut._vrf = vrf         # type: ignore[attr-defined]
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
