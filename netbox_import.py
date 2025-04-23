#!/usr/bin/env python3
"""
Netbox IP Address Management Script.

This script provides functionality to import and update IP address data from a CSV file
into a Netbox instance. It supports concurrent processing of records and includes
comprehensive logging of all operations.

Requirements:
    - pynetbox
    - tqdm
    - configparser
    - urllib3
"""

import csv
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import configparser
from typing import Dict, List, Optional
import urllib3
import pynetbox
from tqdm import tqdm
from netbox_connection import connect_to_netbox

# Disable insecure HTTPS warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Script configuration
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, 'logs')
MAX_WORKERS = 5

# Ensure log directory exists
os.makedirs(LOG_DIR, exist_ok=True)

# Configure logging with both file and console handlers
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

    # File handler for debugging (detailed logs)
    debug_handler = logging.FileHandler(
        os.path.join(LOG_DIR, 'netbox_import_debug.log')
    )
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(file_formatter)

    # File handler for errors
    error_handler = logging.FileHandler(
        os.path.join(LOG_DIR, 'netbox_import_error.log')
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    # Add all handlers to logger
    logger.addHandler(debug_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)

    return logger

def parse_tags(tags_string: str) -> List[Dict[str, str]]:
    """
    Convert a comma-separated string of tags into a list of tag dictionaries.
    
    Args:
        tags_string (str): Comma-separated string of tags
        
    Returns:
        List[Dict[str, str]]: List of tag dictionaries
    """
    return [{'name': tag.strip()} for tag in tags_string.split(',') if tag.strip()]

def process_row(row: Dict[str, str], pbar: tqdm, netbox_instance: pynetbox.api) -> None:
    """
    Process a single row from the CSV file and update/create IP addresses in Netbox.
    
    Args:
        row (Dict[str, str]): Dictionary representing a single row from the CSV file
        pbar (tqdm): Progress bar instance
        netbox_instance (pynetbox.api): Netbox API instance
        
    Raises:
        Exception: If there's an error processing the row
    """
    logger = logging.getLogger(__name__)
    address = row.get('address', 'unknown')

    try:
        logger.debug(f"Starting to process address: {address}")

        # Parse tags
        tags_list = parse_tags(row['tags'])
        logger.debug(f"Parsed tags for {address}: {tags_list}")

        # Prepare tenant and VRF data
        tenant_data = {'name': row['tenant']} if row['tenant'] != 'N/A' else None
        vrf_data = {'name': row['VRF']} if row['VRF'] != 'N/A' else None

        # Get existing address
        existing_address = netbox_instance.ipam.ip_addresses.get(address=address, vrf=vrf_data['name'], tenant=tenant_data['name'])

        if existing_address:
            _update_existing_address(
                existing_address, row, tags_list, tenant_data, vrf_data
            )
        else:
            _create_new_address(
                netbox_instance, address, row, tags_list, tenant_data, vrf_data
            )

    except Exception:
        logger.error(f"Failed to process row for address {address}", exc_info=True)
        raise
    finally:
        if pbar:
            pbar.update(1)

def _update_existing_address(
    existing_address: object,
    row: Dict[str, str],
    tags_list: List[Dict[str, str]],
    tenant_data: Optional[Dict[str, str]],
    vrf_data: Optional[Dict[str, str]]
) -> None:
    """
    Update an existing IP address in Netbox while preserving existing tags.
    
    Args:
        existing_address: Existing Netbox IP address object
        row (Dict[str, str]): Row data from CSV
        tags_list (List[Dict[str, str]]): Processed tags
        tenant_data (Optional[Dict[str, str]]): Tenant information
        vrf_data (Optional[Dict[str, str]]): VRF information
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Updating existing address: {row['address']}")

    try:
        # Get existing tags
        existing_tags = existing_address.tags

        # Convert existing tags to the same format as new tags if necessary
        if existing_tags and not isinstance(existing_tags[0], dict):
            existing_tags = [{'name': tag.name} for tag in existing_tags]

        # Merge existing and new tags, avoiding duplicates
        merged_tags = []
        existing_tag_names = {tag['name'] for tag in existing_tags} if existing_tags else set()

        # Add all existing tags
        merged_tags.extend(existing_tags or [])

        # Add new tags that don't already exist
        for tag in tags_list:
            if tag['name'] not in existing_tag_names:
                merged_tags.append(tag)

        if existing_address.status and existing_address.status.value.lower() == 'dhcp':
            # For DHCP addresses, only update scantime and tags
            existing_address.custom_fields = {'scantime': row['scantime']}
            existing_address.tags = merged_tags
            logger.debug(f"Updated scantime and tags for DHCP address: {row['address']}")
        else:
            # For non-DHCP addresses, update all fields
            existing_address.status = row['status']
            existing_address.custom_fields = {'scantime': row['scantime']}
            if row['dns_name']:
                existing_address.dns_name = row['dns_name']
            existing_address.tags = merged_tags
            existing_address.tenant = tenant_data
            existing_address.vrf = vrf_data
            logger.debug(f"Updated all fields for non-DHCP address: {row['address']}")

        existing_address.save()
        logger.debug(f"Successfully updated address {row['address']}")
        logger.debug(f"Merged tags for {row['address']}: {merged_tags}")

    except Exception as exc:
        logger.error(f"Error updating address {row['address']}: {str(exc)}", exc_info=True)
        raise

def _create_new_address(
    netbox_instance: pynetbox.api,
    address: str,
    row: Dict[str, str],
    tags_list: List[Dict[str, str]],
    tenant_data: Optional[Dict[str, str]],
    vrf_data: Optional[Dict[str, str]]
) -> None:
    """
    Create a new IP address in Netbox.
    
    Args:
        netbox_instance (pynetbox.api): Netbox API instance
        address (str): IP address to create
        row (Dict[str, str]): Row data from CSV
        tags_list (List[Dict[str, str]]): Processed tags
        tenant_data (Optional[Dict[str, str]]): Tenant information
        vrf_data (Optional[Dict[str, str]]): VRF information
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Creating new address: {address}")

    try:
        netbox_instance.ipam.ip_addresses.create(
            address=address,
            status=row['status'],
            custom_fields={'scantime': row['scantime']},
            dns_name=row['dns_name'],
            tags=tags_list,
            tenant=tenant_data,
            vrf=vrf_data
        )
        logger.debug(f"Successfully created new address: {address}")

    except pynetbox.core.query.RequestError as exc:
        if 'Duplicate IP address' in str(exc):
            logger.warning(f"Duplicate IP address found: {address}")
        else:
            logger.error(f"Error creating address {address}: {str(exc)}", exc_info=True)
            raise
    except Exception as exc:
        logger.error(f"Unexpected error creating address {address}: {str(exc)}",exc_info=True)
        raise

def write_data_to_netbox(url: str, token: str, csv_file: str) -> None:
    """
    Write data from a CSV file to Netbox.
    
    Args:
        url (str): Base URL of the Netbox instance
        token (str): Authentication token for Netbox API
        csv_file (str): Path to the CSV file containing data
        
    Raises:
        Exception: If there's an error in the overall process
    """
    logger = logging.getLogger(__name__)

    config = configparser.ConfigParser()
    config.read('var.ini')
    show_progress = config.getboolean('scan_options', 'show_progress', fallback=True)

    try:
        logger.info("Initializing Netbox connection...")
        netbox_instance = connect_to_netbox(url, token)
        logger.info("Successfully connected to Netbox")

        csv_file_path = os.path.join(SCRIPT_DIR, csv_file)
        logger.debug(f"Reading CSV file from: {csv_file_path}")

        with open(csv_file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            rows = list(reader)
            total_rows = len(rows)
            logger.info(f"Found {total_rows} rows to process")

            if show_progress:
                pbar = tqdm(total=total_rows, desc="Processing Rows")
            else:
                pbar = None

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [
                    executor.submit(
                        process_row, row, pbar if show_progress else None, netbox_instance
                    ) for row in rows
                ]

                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception:
                        logger.error("Error in thread pool execution", exc_info=True)
                        continue

            if pbar:
                pbar.close()

        logger.info("Completed processing all rows")

    except Exception as exc:
        logger.error(f"Fatal error in write_data_to_netbox: {str(exc)}", exc_info=True)
        raise

def main() -> None:
    """
    Main entry point of the script.
    
    Reads configuration, sets up logging, and initiates the data import process.
    """
    logger = setup_logging()
    logger.info("Starting Netbox import process")

    try:
        # Read configuration
        config = configparser.ConfigParser()
        config_path = os.path.join(SCRIPT_DIR, 'var.ini')
        config.read(config_path)

        url = config['credentials']['url']
        token = config['credentials']['token']

        logger.debug(f"Configuration loaded from: {config_path}")
        write_data_to_netbox(url, token, 'ipam_addresses.csv')
        logger.info("Netbox import process completed successfully")

    except Exception:
        logger.error("Script execution failed", exc_info=True)
        raise

if __name__ == "__main__":
    main()
