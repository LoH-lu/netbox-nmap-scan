#!/usr/bin/env python3
"""
Network Scanner Script.

This script performs nmap scans on network prefixes retrieved from a CSV file.
It includes comprehensive logging, error handling, and concurrent execution
capabilities for efficient scanning operations.

The script:
1. Reads network prefixes from a CSV file
2. Performs concurrent nmap scans on active prefixes
3. Writes results to timestamped CSV files
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
from typing import Dict, List, Tuple, Optional, Set
from dataclasses import dataclass
import queue
import tempfile
import shutil

# Script configuration
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, 'logs')
RESULTS_DIR = os.path.join(SCRIPT_DIR, 'results')
MAX_WORKERS = 5
NMAP_TIMEOUT = 300  # seconds
FILE_LOCK = threading.Lock()

# CSV field definitions
INPUT_FIELDNAMES = ['Prefix', 'VRF', 'Status', 'Tags', 'Tenant']
OUTPUT_FIELDNAMES = ['address', 'dns_name', 'status', 'tags', 'tenant', 'VRF', 'scantime']

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

    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )

    # Create file handlers
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    debug_handler = logging.FileHandler(
        os.path.join(LOG_DIR, f'network_scan_debug_{timestamp}.log')
    )
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(file_formatter)

    error_handler = logging.FileHandler(
        os.path.join(LOG_DIR, f'network_scan_error_{timestamp}.log')
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_formatter)

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    # Add handlers to logger
    logger.addHandler(debug_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)

    return logger

def remove_prefix_from_csv(filename: str, prefix_to_remove: str) -> None:
    """
    Remove a single prefix from the CSV file immediately after successful scan.
    Uses file locking to ensure thread safety.
    
    Args:
        filename: Path to the CSV file
        prefix_to_remove: The prefix to remove from the file
    """
    logger = logging.getLogger(__name__)
    filepath = os.path.join(SCRIPT_DIR, filename)
    
    with FILE_LOCK:  # Ensure thread safety when modifying the file
        try:
            # Create a temporary file
            temp_file = tempfile.NamedTemporaryFile(
                mode='w',
                delete=False,
                newline='',
                encoding='utf-8',
                dir=os.path.dirname(filepath)
            )
            
            removed = False
            # Read and write in a single pass
            with open(filepath, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                writer = csv.DictWriter(temp_file, fieldnames=reader.fieldnames)
                writer.writeheader()
                
                for row in reader:
                    if row['Prefix'] != prefix_to_remove:
                        writer.writerow(row)
                    else:
                        removed = True
            
            temp_file.close()
            
            # Atomic replacement of the original file
            shutil.move(temp_file.name, filepath)
            
            if removed:
                logger.info(f"Successfully removed prefix {prefix_to_remove} from {filename}")
            else:
                logger.warning(f"Prefix {prefix_to_remove} not found in {filename}")
                
        except Exception as exc:
            logger.error(f"Error removing prefix {prefix_to_remove} from CSV: {str(exc)}", exc_info=True)
            if 'temp_file' in locals() and os.path.exists(temp_file.name):
                os.unlink(temp_file.name)
            raise

def read_from_csv(filename: str) -> List[Dict[str, str]]:
    """
    Read data from a CSV file.

    Args:
        filename: Path to the CSV file

    Returns:
        List of dictionaries representing rows from the CSV file
        
    Raises:
        FileNotFoundError: If the file doesn't exist
        csv.Error: If there's an error reading the CSV file
    """
    logger = logging.getLogger(__name__)
    filepath = os.path.join(SCRIPT_DIR, filename)

    try:
        logger.info(f"Reading CSV file: {filepath}")
        with FILE_LOCK:  # Add lock when reading to ensure consistency
            with open(filepath, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                data = [row for row in reader]

        logger.info(f"Successfully read {len(data)} rows from {filename}")
        return data

    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        raise
    except csv.Error as exc:
        logger.error(f"Error reading CSV file {filepath}: {str(exc)}")
        raise

def run_nmap_on_prefix(
    prefix: str,
    tenant: str,
    VRF: str,
    result_queue: queue.Queue,
    input_filename: str
) -> Tuple[List[ScanResult], bool]:
    """
    Run nmap scan on a given prefix and remove it from input file if successful.

    Args:
        prefix: Network prefix to scan
        tenant: Tenant associated with the prefix
        VRF: VRF associated with the prefix
        result_queue: Queue for storing scan results
        input_filename: Name of the input CSV file to update

    Returns:
        Tuple containing list of scan results and success status
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Starting scan on prefix: {prefix}")

    config = configparser.ConfigParser()
    config.read('var.ini')
    enable_dns = config.getboolean('scan_options', 'enable_dns', fallback=True)

    try:
        command = [
            "nmap",
            "-sn",  # Ping scan
            "-T4",  # Aggressive timing
            "--min-parallelism", "10",
            "--max-retries", "2"
        ]

        if enable_dns:
            command.append("-R")
        else:
            command.append("-n")

        command.append(prefix)

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        output, error = process.communicate(timeout=NMAP_TIMEOUT)

        if process.returncode != 0:
            logger.error(f"Nmap error for {prefix}: {error}")
            return [], False

        results = []
        for line in output.split('\n'):
            if "Nmap scan report for" in line:
                result = _parse_nmap_output(line, prefix, tenant, VRF)
                if result:
                    results.append(result)
                    result_queue.put(result)

        # If scan was successful, remove the prefix immediately
        if process.returncode == 0:
            remove_prefix_from_csv(input_filename, prefix)
            logger.info(f"Completed scan on prefix: {prefix} - Found {len(results)} hosts")
            return results, True

        return [], False

    except subprocess.TimeoutExpired:
        logger.error(f"Scan timeout for prefix: {prefix}")
        process.kill()
        return [], False
    except Exception as exc:
        logger.error(f"Error scanning prefix {prefix}: {str(exc)}", exc_info=True)
        return [], False

def _parse_nmap_output(
    line: str,
    prefix: str,
    tenant: str,
    VRF: str
) -> Optional[ScanResult]:
    """Parse a single line of nmap output."""
    logger = logging.getLogger(__name__)

    config = configparser.ConfigParser()
    config.read('var.ini')
    enable_scantime = config.getboolean('scan_options', 'enable_scantime', fallback=True)

    scantime = datetime.now().strftime('%Y-%m-%d %H:%M:%S') if enable_scantime else None

    try:
        parts = line.split()
        dns_name = None

        if len(parts) > 5:
            dns_name = parts[4]
            address = parts[5].strip('()')
        else:
            address = parts[-1].strip('()')

        subnet_mask = prefix.split('/')[-1]
        address_with_mask = f"{address}/{subnet_mask}"

        return ScanResult(
            address=address_with_mask,
            dns_name=dns_name,
            status='active',
            tags='autoscan',
            tenant=tenant,
            VRF=VRF,
            scantime=scantime
        )

    except Exception:
        logger.error(f"Error parsing nmap output line: {line}", exc_info=True)
        return None

def write_results_to_csv(
    results: List[ScanResult],
    output_folder: str,
    start_time: datetime
) -> None:
    """Write scan results to CSV file."""
    logger = logging.getLogger(__name__)
    start_time_str = start_time.strftime('%Y-%m-%d_%H-%M-%S')
    output_file = os.path.join(output_folder, f'nmap_results_{start_time_str}.csv')

    try:
        with FILE_LOCK:
            is_new_file = not os.path.exists(output_file)

            with open(output_file, 'a', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=OUTPUT_FIELDNAMES)

                if is_new_file:
                    writer.writeheader()

                for result in results:
                    writer.writerow(vars(result))

        logger.debug(f"Wrote {len(results)} results to {output_file}")

    except Exception as exc:
        logger.error(f"Error writing results to CSV: {str(exc)}", exc_info=True)
        raise

def process_network_prefixes() -> None:
    """Main function to coordinate network scanning operations."""
    logger = logging.getLogger(__name__)
    logger.info("Starting network scanning process")

    try:
        # Read input data
        input_filename = 'ipam_prefixes.csv'
        data = read_from_csv(input_filename)

        # Filter active prefixes
        rows_to_scan = [
            row for row in data
            if row['Status'] == 'active' and 'Disable Automatic Scanning' not in row['Tags']
        ]

        logger.info(f"Found {len(rows_to_scan)} prefixes to scan")

        # Set up result queue and start time
        result_queue = queue.Queue()
        script_start_time = datetime.now()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(
                    run_nmap_on_prefix,
                    row['Prefix'],
                    row['Tenant'],
                    row['VRF'],
                    result_queue,
                    input_filename
                ): row for row in rows_to_scan
            }

            # Wait for all futures to complete
            for future in as_completed(futures):
                try:
                    results, success = future.result()
                    if success:
                        logger.info(f"Successfully processed: {futures[future]['Prefix']}")
                except Exception:
                    logger.error("Error processing scan result", exc_info=True)

        # Write final results from the result queue
        results_to_write = []
        while not result_queue.empty():
            results_to_write.append(result_queue.get())
            if len(results_to_write) >= 100:
                write_results_to_csv(results_to_write, RESULTS_DIR, script_start_time)
                results_to_write = []

        if results_to_write:
            write_results_to_csv(results_to_write, RESULTS_DIR, script_start_time)

        logger.info("Network scanning process completed")

    except Exception:
        logger.error("Fatal error during scanning process", exc_info=True)
        sys.exit(1)

def main() -> None:
    """Main entry point of the script."""
    logger = setup_logging()
    process_network_prefixes()

if __name__ == "__main__":
    main()
