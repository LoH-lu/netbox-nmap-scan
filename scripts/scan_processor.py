#!/usr/bin/env python3
"""
Network Scan Results Processor.

This script processes network scan results stored in CSV files, comparing the latest
scan with previous results to track deprecated addresses. It includes comprehensive
logging and error handling for reliable operation.

The script:
1. Finds the latest CSV files in a specified directory
2. Reads and processes the scan results
3. Marks addresses as deprecated if they're missing from the latest scan
4. Outputs the combined results to a new CSV file
"""

import csv
import os
import logging
from datetime import datetime
from typing import Dict, List
import sys

# Script configuration
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, 'logs')
RESULTS_DIR = os.path.join(SCRIPT_DIR, 'results')
CSV_FIELDNAMES = ['address', 'dns_name', 'status', 'scantime', 'tags', 'tenant', 'VRF']

# Ensure required directories exist
for directory in (LOG_DIR, RESULTS_DIR):
    os.makedirs(directory, exist_ok=True)

def setup_logging() -> logging.Logger:
    """
    Configure logging with both file and console handlers.
    
    Returns:
        logging.Logger: Configured logger instance
    """
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
        os.path.join(LOG_DIR, f'scan_processor_debug_{timestamp}.log')
    )
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(file_formatter)

    error_handler = logging.FileHandler(
        os.path.join(LOG_DIR, f'scan_processor_error_{timestamp}.log')
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

def get_file_path(dir_path: str, date_time: datetime) -> str:
    """
    Generate a file path based on the directory and date.

    Args:
        dir_path: The directory where the file will be located
        date_time: The date to be included in the file name

    Returns:
        The full file path
    """
    return os.path.join(
        SCRIPT_DIR,
        dir_path,
        f'nmap_results_{date_time.strftime("%Y-%m-%d_%H-%M-%S")}.csv'
    )

def get_latest_files(dir_path: str, num_files: int = 2) -> List[str]:
    """
    Get the list of CSV files in a directory and sort them by modification time.

    Args:
        dir_path: The directory to search for CSV files
        num_files: The number of latest files to retrieve

    Returns:
        List of latest CSV file names
        
    Raises:
        FileNotFoundError: If the directory doesn't exist
        ValueError: If no CSV files are found in the directory
    """
    logger = logging.getLogger(__name__)
    full_directory = os.path.join(SCRIPT_DIR, dir_path)

    logger.debug(f"Searching for CSV files in: {full_directory}")

    try:
        files = [f for f in os.listdir(full_directory) if f.endswith('.csv')]

        if not files:
            logger.error(f"No CSV files found in {full_directory}")
            raise ValueError(f"No CSV files found in {full_directory}")

        files.sort(
            key=lambda x: os.path.getmtime(os.path.join(full_directory, x)),
            reverse=True
        )

        selected_files = files[:num_files]
        logger.info(f"Found {len(selected_files)} latest CSV files")
        logger.debug(f"Selected files: {selected_files}")

        return selected_files

    except FileNotFoundError:
        logger.error(f"Directory not found: {full_directory}")
        raise

def read_csv(file_path: str) -> Dict[str, Dict[str, str]]:
    """
    Read a CSV file and return a dictionary with address_vrf combinations as keys.

    Args:
        file_path: The path to the CSV file

    Returns:
        Dictionary with address_vrf combinations as keys and corresponding row data as values
        
    Raises:
        FileNotFoundError: If the file doesn't exist
        csv.Error: If there's an error reading the CSV file
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Reading CSV file: {file_path}")

    data = {}
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Create composite key using address and VRF
                address = row['address']
                vrf = row['VRF']
                composite_key = f"{address}_{vrf}"
                data[composite_key] = row
                logger.debug(f"Processed row for address: {address} in VRF: {vrf}")

        logger.info(f"Successfully read {len(data)} records from {file_path}")
        return data

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except csv.Error as exc:
        logger.error(f"Error reading CSV file {file_path}: {str(exc)}")
        raise

def write_csv(data: Dict[str, Dict[str, str]], file_path: str) -> None:
    """
    Write data to a new CSV file.

    Args:
        data: Dictionary containing row data with addresses as keys
        file_path: The path to the output CSV file
        
    Raises:
        PermissionError: If writing to the file is not permitted
        csv.Error: If there's an error writing the CSV file
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Writing data to CSV file: {file_path}")

    try:
        with open(file_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=CSV_FIELDNAMES)
            writer.writeheader()

            for row in data.values():
                writer.writerow(row)
                logger.debug(f"Wrote row for address: {row['address']}")

        logger.info(f"Successfully wrote {len(data)} records to {file_path}")

    except PermissionError:
        logger.error(f"Permission denied writing to file: {file_path}")
        raise
    except csv.Error as exc:
        logger.error(f"Error writing CSV file {file_path}: {str(exc)}")
        raise

def process_scan_results() -> None:
    """
    Main function to process network scan results.
    
    Coordinates the reading of input files, processing of data, and writing of results.
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting scan results processing")

    try:
        # Get the latest file paths
        latest_files = get_latest_files(RESULTS_DIR)
        file_paths = [
            get_file_path(
                RESULTS_DIR,
                datetime.strptime(file_name[13:32], "%Y-%m-%d_%H-%M-%S")
            ) for file_name in latest_files
        ]

        # Read data from the latest file
        data = read_csv(file_paths[0])

        # Process older file if available
        if len(file_paths) == 2:
            logger.info("Processing older file to identify deprecated addresses")
            older_data = read_csv(file_paths[1])

            # Check for deprecated addresses
            deprecated_count = 0
            for address, older_row in older_data.items():
                if address not in data:
                    older_row['status'] = 'deprecated'
                    data[address] = older_row
                    deprecated_count += 1
                    logger.debug(f"Marked address as deprecated: {address}")

            logger.info(f"Found {deprecated_count} deprecated addresses")

        # Write the updated data
        output_file_path = os.path.join(SCRIPT_DIR, 'ipam_addresses.csv')
        write_csv(data, output_file_path)

        logger.info(f"Processing completed. Output file: {output_file_path}")

    except Exception:
        logger.error("Fatal error during processing", exc_info=True)
        sys.exit(1)

def main() -> None:
    """Main entry point of the script."""
    logger = setup_logging()
    process_scan_results()

if __name__ == "__main__":
    main()
