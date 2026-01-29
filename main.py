#!/usr/bin/env python3
"""
Netbox nmap scan scheduler (NEW METHOD ONLY, concurrent).

Flow per scheduler cycle:
1) Fetch active prefixes from Netbox
2) Pre-create ALL prefix folders under ./PREFIXES/ and write prefix.info for each
3) Schedule concurrent scans only for prefixes that are due (per scan_interval_hours)
4) For each scanned prefix:
   - nmap writes nmap_results_<timestamp>.csv (header-only if no hosts)
   - scan_processor writes ipam_addresses.csv containing:
       - latest active hosts
       - deprecated hosts only if present in previous scan but missing now
   - netbox_import imports per-prefix ipam_addresses.csv (create only if active; update existing)

Configuration (var.ini):
[credentials]
url = ...
token = ...

[scan_options]
scan_interval_hours = 4
scheduler_sleep_seconds = 300
scan_max_workers = 5
"""

import logging
import os
import sys
import time
import configparser
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import netbox_connection
import netbox_export
import netbox_import
import network_scan
import scan_processor

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")
PREFIXES_DIR = os.path.join(SCRIPT_DIR, "PREFIXES")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(PREFIXES_DIR, exist_ok=True)

# Guard to avoid scheduling the same prefix concurrently
_IN_PROGRESS: set[str] = set()
_IN_PROGRESS_LOCK = threading.Lock()


def setup_logging() -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    file_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
    )
    console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    debug_handler = logging.FileHandler(os.path.join(LOG_DIR, f"scheduler_debug_{timestamp}.log"))
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(file_formatter)

    error_handler = logging.FileHandler(os.path.join(LOG_DIR, f"scheduler_error_{timestamp}.log"))
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    logger.addHandler(debug_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)

    return logger


def load_scheduler_config() -> Tuple[str, str, int, int, int]:
    config_path = os.path.join(SCRIPT_DIR, "var.ini")
    config = configparser.ConfigParser()
    read_files = config.read(config_path)

    if not read_files:
        raise RuntimeError(f"Configuration file not found: {config_path}")

    url = config["credentials"]["url"]
    token = config["credentials"]["token"]

    scan_interval_hours = config.getint("scan_options", "scan_interval_hours", fallback=4)
    scheduler_sleep_seconds = config.getint("scan_options", "scheduler_sleep_seconds", fallback=300)
    scan_max_workers = config.getint("scan_options", "scan_max_workers", fallback=5)
    if scan_max_workers < 1:
        scan_max_workers = 1

    return url, token, scan_interval_hours, scheduler_sleep_seconds, scan_max_workers


def sanitize_prefix_folder_name(prefix: str, vrf: str) -> str:
    folder = prefix.replace("/", "_").replace(":", "_")
    vrf_part = vrf if vrf and vrf != "N/A" else ""
    if vrf_part:
        folder = f"{folder}__{vrf_part}"
    folder = folder.replace(os.sep, "_")
    return folder


def get_active_prefixes(netbox_instance) -> List[Dict[str, str]]:
    logger = logging.getLogger(__name__)
    logger.info("Retrieving prefixes from Netbox")

    ipam_prefixes = netbox_export.get_ipam_prefixes(netbox_instance)
    active: List[Dict[str, str]] = []

    for prefix in ipam_prefixes:
        status_value = prefix.status.value if getattr(prefix, "status", None) else "N/A"
        if status_value != "active":
            continue

        tag_names = [tag.name for tag in getattr(prefix, "tags", [])]
        if "Disable Automatic Scanning" in tag_names:
            continue

        tenant_name = prefix.tenant.name if getattr(prefix, "tenant", None) else "N/A"
        vrf_name = prefix.vrf.name if getattr(prefix, "vrf", None) else "N/A"

        active.append({"prefix": prefix.prefix, "tenant": tenant_name, "vrf": vrf_name})

    logger.info("Found %d active prefixes for scanning", len(active))
    return active


def write_prefix_info(prefix_dir: str, prefix: str) -> None:
    """
    Ensure prefix.info exists and contains the CIDR.
    Overwrites with current CIDR (safe) in case Netbox prefix changed.
    """
    os.makedirs(prefix_dir, exist_ok=True)
    path = os.path.join(prefix_dir, "prefix.info")
    with open(path, "w", encoding="utf-8") as f:
        f.write(prefix.strip() + "\n")


def precreate_prefix_folders(prefixes: List[Dict[str, str]]) -> None:
    """
    Create all prefix folders and prefix.info FIRST, before any scanning/processing.
    This is fast and reduces work after a crash/restart.
    """
    logger = logging.getLogger(__name__)
    logger.info("Pre-creating prefix folders and prefix.info files...")

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

        if existed:
            updated += 1
        else:
            created += 1

    logger.info("Prefix folders prepared: created=%d, updated=%d", created, updated)


def get_last_scan_time(prefix_dir: str) -> datetime | None:
    try:
        files = [
            f for f in os.listdir(prefix_dir)
            if f.startswith("nmap_results_") and f.endswith(".csv")
        ]
    except FileNotFoundError:
        return None

    if not files:
        return None

    latest = max(files)
    ts_str = latest[len("nmap_results_"):-len(".csv")]

    try:
        return datetime.strptime(ts_str, "%Y-%m-%d_%H-%M-%S")
    except ValueError:
        return datetime.fromtimestamp(os.path.getmtime(os.path.join(prefix_dir, latest)))


def is_scan_due(prefix_dir: str, interval_hours: int) -> bool:
    last = get_last_scan_time(prefix_dir)
    if last is None:
        return True
    return datetime.now() - last >= timedelta(hours=interval_hours)


def _prefix_task_key(prefix: str, vrf: str) -> str:
    return f"{prefix}__{vrf or 'N/A'}"


def run_scan_for_prefix(url: str, token: str, prefix_info: Dict[str, str]) -> None:
    """
    Worker task: scan + process + import for a single prefix folder.
    """
    logger = logging.getLogger(__name__)

    prefix = prefix_info["prefix"]
    tenant = prefix_info["tenant"]
    vrf = prefix_info["vrf"]

    folder_name = sanitize_prefix_folder_name(prefix, vrf)
    prefix_dir = os.path.join(PREFIXES_DIR, folder_name)
    os.makedirs(prefix_dir, exist_ok=True)

    # Ensure prefix.info exists even if the folder was prepared earlier
    # (cheap and guarantees correctness if prefix changed)
    write_prefix_info(prefix_dir, prefix)

    scan_start = datetime.now()
    logger.info("Starting scan for prefix %s (VRF=%s)", prefix, vrf)

    results, success = network_scan.run_nmap_on_prefix(
        prefix=prefix,
        tenant=tenant,
        VRF=vrf,
        script_start_time=scan_start,
        input_filename=None,
        output_folder=prefix_dir,
        remove_from_input=False,
    )

    if not success:
        logger.error("Scan failed for prefix %s (VRF=%s)", prefix, vrf)
        return

    if results:
        logger.info("Scan completed for prefix %s (VRF=%s), %d active hosts", prefix, vrf, len(results))
    else:
        logger.info("Scan completed for prefix %s (VRF=%s) with no active hosts found", prefix, vrf)

    output_csv = os.path.join(prefix_dir, "ipam_addresses.csv")
    scan_processor.process_scan_results_in_dir(prefix_dir, output_csv)

    logger.info("Importing scan results into Netbox for prefix %s (VRF=%s)", prefix, vrf)
    netbox_import.write_data_to_netbox(url, token, output_csv)


def main() -> None:
    logger = setup_logging()
    logger.info("Starting Netbox nmap scan scheduler (concurrent, precreate folders first)")

    try:
        url, token, scan_interval_hours, scheduler_sleep_seconds, scan_max_workers = load_scheduler_config()
    except Exception:
        logger.error("Failed to load configuration", exc_info=True)
        sys.exit(1)

    logger.info(
        "Scan interval: %d hours | scheduler sleep: %d seconds | scan workers: %d",
        scan_interval_hours, scheduler_sleep_seconds, scan_max_workers
    )

    executor = ThreadPoolExecutor(max_workers=scan_max_workers)

    try:
        while True:
            try:
                logger.info("Connecting to Netbox...")
                netbox_instance = netbox_connection.connect_to_netbox(url, token)
                logger.info("Successfully connected to Netbox")

                prefixes = get_active_prefixes(netbox_instance)

                # 1) Precreate ALL folders + prefix.info BEFORE scheduling anything
                precreate_prefix_folders(prefixes)

                # 2) Schedule due scans concurrently
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
                    fut._prefix = prefix    # type: ignore[attr-defined]
                    fut._vrf = vrf          # type: ignore[attr-defined]
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
                        logger.error(
                            "Unexpected error while processing prefix %s (VRF=%s)",
                            prefix, vrf, exc_info=True
                        )
                    finally:
                        if key:
                            with _IN_PROGRESS_LOCK:
                                _IN_PROGRESS.discard(key)

            except KeyboardInterrupt:
                logger.info("Scheduler interrupted by user, exiting.")
                break
            except Exception:
                logger.error("Unexpected scheduler cycle error", exc_info=True)

            logger.info("Scheduler sleeping for %d seconds", scheduler_sleep_seconds)
            time.sleep(scheduler_sleep_seconds)

    finally:
        executor.shutdown(wait=True)


if __name__ == "__main__":
    main()
