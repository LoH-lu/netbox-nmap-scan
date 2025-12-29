#!/usr/bin/env python3
"""
Network Scanner Script.

This script performs nmap scans on network prefixes retrieved from a CSV file.
It includes comprehensive logging, error handling, and concurrent execution
capabilities for efficient scanning operations.

The script:
1. Reads network prefixes from a CSV file
2. Performs concurrent nmap scans on all prefixes
3. Writes results to CSV file immediately after each scan
4. Removes each scanned network from input file immediately after scanning
"""

import csv
import subprocess
import os
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import configparser
import logging
import threading
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import tempfile
import shutil

# Script configuration
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT_DIR = PROJECT_ROOT
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
RESULTS_DIR = os.path.join(PROJECT_ROOT, "results")
MAX_WORKERS = 5
NMAP_TIMEOUT = 300  # seconds
FILE_LOCK = threading.Lock()

# CSV field definitions
INPUT_FIELDNAMES = ["Prefix", "VRF", "Status", "Tags", "Tenant"]
OUTPUT_FIELDNAMES = ["address", "dns_name", "status", "tags", "tenant", "VRF", "scantime"]

# Ensure required directories exist
for directory in (LOG_DIR, RESULTS_DIR):
    os.makedirs(directory, exist_ok=True)


@dataclass
class ScanResult:
    """Data class for storing scan results."""
    address: str
    dns_name: Optional[str]
    status: str
    tags: str
    tenant: str
    VRF: str
    scantime: str


def setup_logging() -> logging.Logger:
    """Configure logging with both file and console handlers."""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    file_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
    )
    console_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    debug_handler = logging.FileHandler(
        os.path.join(LOG_DIR, f"network_scan_debug_{timestamp}.log")
    )
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(file_formatter)

    error_handler = logging.FileHandler(
        os.path.join(LOG_DIR, f"network_scan_error_{timestamp}.log")
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    logger.addHandler(debug_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)

    return logger


def read_from_csv(filename: str) -> List[Dict[str, str]]:
    """
    Read data from a CSV file.

    Args:
        filename: Path to the CSV file

    Returns:
        List of dictionaries representing rows from the CSV file
    """
    logger = logging.getLogger(__name__)
    filepath = os.path.join(SCRIPT_DIR, filename)

    logger.info("Reading CSV file: %s", filepath)

    with FILE_LOCK:
        with open(filepath, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            data = [row for row in reader]

    logger.info("Successfully read %d rows from %s", len(data), filename)
    return data


def remove_prefix_from_csv(filename: str, prefix_to_remove: str) -> None:
    """
    Remove a single prefix from the CSV file immediately after successful scan.
    """
    logger = logging.getLogger(__name__)
    filepath = os.path.join(SCRIPT_DIR, filename)

    with FILE_LOCK:
        temp_file = tempfile.NamedTemporaryFile(
            mode="w",
            delete=False,
            newline="",
            encoding="utf-8",
            dir=os.path.dirname(filepath),
        )

        removed = False
        with open(filepath, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            writer = csv.DictWriter(temp_file, fieldnames=reader.fieldnames)
            writer.writeheader()

            for row in reader:
                if row["Prefix"] != prefix_to_remove:
                    writer.writerow(row)
                else:
                    removed = True

        temp_file.close()
        shutil.move(temp_file.name, filepath)

        if removed:
            logger.info("Removed prefix %s from %s", prefix_to_remove, filename)
        else:
            logger.warning("Prefix %s not found in %s", prefix_to_remove, filename)


def write_scan_results(
    results: List[ScanResult],
    output_folder: str,
    start_time: datetime,
) -> None:
    """
    Write scan results to CSV file immediately.
    """
    logger = logging.getLogger(__name__)
    start_time_str = start_time.strftime("%Y-%m-%d_%H-%M-%S")
    output_file = os.path.join(output_folder, f"nmap_results_{start_time_str}.csv")

    with FILE_LOCK:
        is_new_file = not os.path.exists(output_file)

        with open(output_file, "a", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=OUTPUT_FIELDNAMES)
            if is_new_file:
                writer.writeheader()

            for result in results:
                writer.writerow(vars(result))

    logger.debug("Wrote %d results to %s", len(results), output_file)


def _parse_nmap_output(
    line: str,
    prefix: str,
    tenant: str,
    vrf: str,
) -> Optional[ScanResult]:
    """Parse a single line of nmap output."""
    config = configparser.ConfigParser()
    config.read("var.ini")
    enable_scantime = config.getboolean("scan_options", "enable_scantime", fallback=True)

    scantime = datetime.now().strftime("%Y-%m-%d %H:%M:%S") if enable_scantime else None

    parts = line.split()
    dns_name = None

    if len(parts) > 5:
        dns_name = parts[4]
        address = parts[5].strip("()")
    else:
        address = parts[-1].strip("()")

    subnet_mask = prefix.split("/")[-1]
    address_with_mask = f"{address}/{subnet_mask}"

    return ScanResult(
        address=address_with_mask,
        dns_name=dns_name,
        status="active",
        tags="autoscan",
        tenant=tenant,
        VRF=vrf,
        scantime=scantime,
    )


def run_nmap_on_prefix(
    prefix: str,
    tenant: str,
    vrf: str,
    script_start_time: datetime,
    input_filename: str,
) -> Tuple[List[ScanResult], bool]:
    """
    Run nmap scan on a given prefix.
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting scan on prefix: %s", prefix)

    config = configparser.ConfigParser()
    config.read("var.ini")
    enable_dns = config.getboolean("scan_options", "enable_dns", fallback=True)

    command = [
        "nmap",
        "-sn",
        "-T4",
        "--min-parallelism",
        "10",
        "--max-retries",
        "2",
    ]

    command.append("-R" if enable_dns else "-n")
    command.append(prefix)

    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        output, error = process.communicate(timeout=NMAP_TIMEOUT)

        if process.returncode != 0:
            logger.error("Nmap error for %s: %s", prefix, error)
            return [], False

        results = [
            _parse_nmap_output(line, prefix, tenant, vrf)
            for line in output.split("\n")
            if "Nmap scan report for" in line
        ]

        results = [r for r in results if r]

        if results:
            write_scan_results(results, RESULTS_DIR, script_start_time)

        remove_prefix_from_csv(input_filename, prefix)
        logger.info("Completed scan on prefix %s (%d hosts)", prefix, len(results))
        return results, True

    except subprocess.TimeoutExpired:
        logger.error("Scan timeout for prefix: %s", prefix)
        return [], False
    except Exception:
        logger.error("Error scanning prefix %s", prefix, exc_info=True)
        return [], False


def process_network_prefixes() -> None:
    """Main function to coordinate network scanning operations."""
    logger = logging.getLogger(__name__)
    logger.info("Starting network scanning process")

    input_filename = "ipam_prefixes.csv"
    data = read_from_csv(input_filename)

    logger.info("Scanning %d prefixes", len(data))
    script_start_time = datetime.now()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                run_nmap_on_prefix,
                row["Prefix"],
                row["Tenant"],
                row["VRF"],
                script_start_time,
                input_filename,
            ): row
            for row in data
        }

        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                logger.error("Error processing scan result", exc_info=True)

    logger.info("Network scanning process completed")


def main() -> None:
    logger = setup_logging()
    process_network_prefixes()


if __name__ == "__main__":
    main()
