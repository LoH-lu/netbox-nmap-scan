import csv
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import configparser
import pynetbox
from tqdm import tqdm
from netbox_connection import connect_to_netbox
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(script_dir, 'netbox_import.log'))
    ]
)
logger = logging.getLogger(__name__)

def process_row(row, pbar):
    """
    Process a single row from the CSV file and update/create IP addresses in Netbox.
    Args:
    - row (dict): A dictionary representing a single row from the CSV file.
    - pbar (tqdm.tqdm): Progress bar to update the progress of processing rows.
    """
    try:
        logger.info(f"Processing address: {row['address']}")
        
        # Convert 'tags' from a comma-separated string to a list of dictionaries
        tags_list = [{'name': tag.strip()} for tag in row['tags'].split(',')]
        logger.debug(f"Tags for {row['address']}: {tags_list}")

        # Attempting to get existing address
        existing_address = netbox.ipam.ip_addresses.get(address=row['address'])
        
        if existing_address:
            logger.info(f"Updating existing address: {row['address']}")
            try:
                # Update the existing address
                existing_address.status = row['status']
                existing_address.custom_fields = {'scantime': row['scantime']}
                existing_address.dns_name = row['dns_name']
                existing_address.tags = tags_list
                
                if row['tenant'] != 'N/A':
                    existing_address.tenant = {'name': row['tenant']}
                if row['VRF'] != 'N/A':
                    existing_address.vrf = {'name': row['VRF']}
                
                existing_address.save()
                logger.info(f"Successfully updated address: {row['address']}")
                
            except Exception as e:
                logger.error(f"Error updating address {row['address']}: {str(e)}")
                raise
                
        else:
            logger.info(f"Creating new address: {row['address']}")
            try:
                # Create a new address if it doesn't exist
                tenant_data = {'name': row['tenant']} if row['tenant'] != 'N/A' else None
                vrf_data = {'name': row['VRF']} if row['VRF'] != 'N/A' else None
                
                netbox.ipam.ip_addresses.create(
                    address=row['address'],
                    status=row['status'],
                    custom_fields={'scantime': row['scantime']},
                    dns_name=row['dns_name'],
                    tags=tags_list,
                    tenant=tenant_data,
                    vrf=vrf_data
                )
                logger.info(f"Successfully created address: {row['address']}")
                
            except pynetbox.core.query.RequestError as e:
                if 'Duplicate IP address' in str(e):
                    logger.warning(f"Duplicate IP address found: {row['address']}")
                else:
                    logger.error(f"Error creating address {row['address']}: {str(e)}")
                    raise
            except Exception as e:
                logger.error(f"Unexpected error creating address {row['address']}: {str(e)}")
                raise
                
    except Exception as e:
        logger.error(f"Failed to process row for address {row.get('address', 'unknown')}: {str(e)}")
        raise
    finally:
        # Update progress bar for each processed row
        pbar.update(1)

def write_data_to_netbox(url, token, csv_file):
    """
    Write data from a CSV file to Netbox.
    Args:
    - url (str): The base URL of the Netbox instance.
    - token (str): The authentication token for accessing the Netbox API.
    - csv_file (str): Path to the CSV file containing data to be written to Netbox.
    """
    global netbox
    try:
        logger.info("Connecting to Netbox...")
        netbox = connect_to_netbox(url, token)
        logger.info("Successfully connected to Netbox")
        
        csv_file_path = os.path.join(script_dir, csv_file)
        logger.info(f"Reading CSV file: {csv_file_path}")
        
        with open(csv_file_path, 'r') as file:
            reader = csv.DictReader(file)
            rows = list(reader)
            total_rows = len(rows)
            logger.info(f"Found {total_rows} rows to process")
            
            with tqdm(total=total_rows, desc="Processing Rows") as pbar:
                with ThreadPoolExecutor(max_workers=5) as executor:
                    # Submit all tasks and store futures
                    futures = [executor.submit(process_row, row, pbar) for row in rows]
                    
                    # Wait for completion and handle any exceptions
                    for future in as_completed(futures):
                        try:
                            future.result()  # This will raise any exceptions from the future
                        except Exception as e:
                            logger.error(f"Error processing row: {str(e)}")
                            # Continue processing other rows even if one fails
                            continue
                            
        logger.info("Completed processing all rows")
        
    except Exception as e:
        logger.error(f"Fatal error in write_data_to_netbox: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        # Read URL and token from var.ini
        config = configparser.ConfigParser()
        config.read(os.path.join(script_dir, 'var.ini'))
        url = config['credentials']['url']
        token = config['credentials']['token']
        
        logger.info("Starting Netbox import process")
        write_data_to_netbox(url, token, 'ipam_addresses.csv')
        logger.info("Netbox import process completed successfully")
        
    except Exception as e:
        logger.error(f"Script failed: {str(e)}")
        raise
