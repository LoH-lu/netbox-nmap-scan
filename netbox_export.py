#!/usr/bin/env python3
"""
Netbox IPAM Prefixes Export Script.

This script exports IPAM prefixes from a Netbox instance to a CSV file.
It includes comprehensive logging and error handling to ensure reliable operation.

Requirements:
    - pynetbox
    - configparser
"""

import csv
import os
import logging
import sys
from typing import List, Optional
import configparser
from datetime import datetime
import pynetbox
import netbox_connection

# Script configuration
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, 'logs')
CSV_HEADERS = ['Prefix', 'VRF', 'Status', 'Tags', 'Tenant']

# Ensure log directory exists
os.makedirs(LOG_DIR, exist_ok=True)

def setup_logging() -> logging.Logger:
    """
    Configure logging with both file and console handlers.
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logger
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
        os.path.join(LOG_DIR, f'netbox_export_debug_{timestamp}.log')
    )
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(file_formatter)

    error_handler = logging.FileHandler(
        os.path.join(LOG_DIR, f'netbox_export_error_{timestamp}.log')
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

def get_ipam_prefixes(
    netbox_instance: pynetbox.api
) -> Optional[List[pynetbox.models.ipam.Prefixes]]:
    """
    Retrieve all IPAM prefixes from Netbox.

    Args:
        netbox_instance: The Netbox API object

    Returns:
        Optional[List[pynetbox.models.ipam.Prefixes]]: List of IPAM prefixes or None if error

    Raises:
        Exception: If there's an error retrieving prefixes
    """
    logger = logging.getLogger(__name__)
    logger.info("Retrieving IPAM prefixes from Netbox")

    try:
        ipam_prefixes = list(netbox_instance.ipam.prefixes.all())
        logger.info(f"Successfully retrieved {len(ipam_prefixes)} IPAM prefixes")
        logger.debug(f"First prefix retrieved: {ipam_prefixes[0].prefix if ipam_prefixes else 'None'}")
        return ipam_prefixes

    except Exception:
        logger.error("Failed to retrieve IPAM prefixes", exc_info=True)
        raise

def write_to_csv(data: List[pynetbox.models.ipam.Prefixes], filename: str) -> None:
    """
    Write IPAM prefixes data to a CSV file.

    Args:
        data: IPAM prefixes data retrieved from Netbox
        filename: Name of the CSV file to write data to

    Raises:
        Exception: If there's an error writing to the CSV file
    """
    logger = logging.getLogger(__name__)
    file_path = os.path.join(SCRIPT_DIR, filename)

    logger.info(f"Starting CSV export to: {file_path}")
    logger.debug(f"Total records to write: {len(data)}")

    try:
        with open(file_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(CSV_HEADERS)

            for prefix in data:
                try:
                    # Extract data with safe fallbacks
                    tag_names = [tag.name for tag in prefix.tags] if hasattr(prefix, 'tags') else []
                    tenant_name = prefix.tenant.name if prefix.tenant else 'N/A'
                    status_value = prefix.status.value if prefix.status else 'N/A'
                    vrf_name = prefix.vrf.name if prefix.vrf else 'N/A'

                    writer.writerow([
                        prefix.prefix,
                        vrf_name,
                        status_value,
                        ', '.join(tag_names),
                        tenant_name
                    ])
                    logger.debug(f"Wrote prefix: {prefix.prefix}")

                except AttributeError as attr_err:
                    logger.warning(f"Missing attribute while processing prefix {prefix.prefix}: {str(attr_err)}")
                except Exception:
                    logger.error(f"Error processing row for prefix {prefix.prefix}",exc_info=True)

        logger.info(f"Successfully exported data to {filename}")

    except Exception:
        logger.error(f"Failed to write to CSV file: {filename}", exc_info=True)
        raise

def load_config() -> tuple:
    """
    Load configuration from var.ini file.
    
    Returns:
        tuple: (url, token) from configuration
        
    Raises:
        Exception: If there's an error reading the configuration
    """
    logger = logging.getLogger(__name__)
    config_path = os.path.join(SCRIPT_DIR, 'var.ini')

    try:
        logger.debug(f"Reading configuration from: {config_path}")
        config = configparser.ConfigParser()
        config.read(config_path)

        url = config['credentials']['url']
        token = config['credentials']['token']

        logger.debug("Successfully loaded configuration")
        return url, token

    except Exception:
        logger.error(f"Failed to load configuration from {config_path}", exc_info=True)
        raise

def main() -> None:
    """
    Main entry point of the script.
    
    Coordinates the export of IPAM prefixes from Netbox to CSV.
    """
    logger = setup_logging()
    logger.info("Starting Netbox IPAM export process")

    try:
        # Load configuration
        url, token = load_config()

        # Connect to Netbox
        try:
            logger.info("Connecting to Netbox...")
            netbox_instance = netbox_connection.connect_to_netbox(url, token)
            logger.info("Successfully connected to Netbox")
        except Exception as e:
            logger.error("Failed to connect to Netbox", exc_info=True)
            raise

        # Retrieve and export data
        ipam_prefixes = get_ipam_prefixes(netbox_instance)
        if ipam_prefixes:
            write_to_csv(ipam_prefixes, 'ipam_prefixes.csv')
            logger.info("Export process completed successfully")
        else:
            logger.warning("No IPAM prefixes found to export")

    except Exception:
        logger.error("Script execution failed", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
