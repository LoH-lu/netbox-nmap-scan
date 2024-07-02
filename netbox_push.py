import csv
import os
from concurrent.futures import ThreadPoolExecutor
import configparser
import pynetbox
from tqdm import tqdm
from netbox_connection import connect_to_netbox

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

def process_row(row, pbar):
    """
    Process a single row from the CSV file and update/create IP addresses in Netbox.

    Args:
    - row (dict): A dictionary representing a single row from the CSV file.
    - pbar (tqdm.tqdm): Progress bar to update the progress of processing rows.
    """
    # Convert 'tags' from a comma-separated string to a list of dictionaries
    tags_list = [{'name': tag.strip()} for tag in row['tags'].split(',')]

    # Assuming you're writing to the 'ipam' endpoint, replace with the correct endpoint if not
    existing_address = netbox.ipam.ip_addresses.get(address=row['address'])

    if existing_address:
        # Update the existing address
        existing_address.status = row['status']
        existing_address.custom_fields = {'scantime': row['scantime']}  # Changed 'description' to 'scantime'
        existing_address.dns_name = row['dns_name']
        existing_address.tags = tags_list
        if row['tenant'] != 'N/A':  # Check if tenant is not 'N/A'
            existing_address.tenant = {'name': row['tenant']}
        if row['VRF'] != 'N/A':  # Check if VRF is not 'N/A'
            existing_address.vrf = {'name': row['VRF']}
        existing_address.save()
    else:
        try:
            # Create a new address if it doesn't exist
            tenant_data = {'name': row['tenant']} if row['tenant'] != 'N/A' else None
            vrf_data = {'name': row['VRF']} if row['VRF'] != 'N/A' else None
            netbox.ipam.ip_addresses.create(
                address=row['address'],
                status=row['status'],
                custom_fields={'scantime': row['scantime']},  # Changed 'description' to 'scantime'
                dns_name=row['dns_name'],
                tags=tags_list,
                tenant=tenant_data,
                vrf=vrf_data
            )
        except pynetbox.core.query.RequestError as e:
            # Handle duplicate address error
            if 'Duplicate IP address' in str(e):
                None
            else:
                # Propagate other errors
                raise

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
    netbox = connect_to_netbox(url, token)

    csv_file_path = os.path.join(script_dir, csv_file)
    with open(csv_file_path, 'r') as file:
        reader = csv.DictReader(file)
        rows = list(reader)

        total_rows = len(rows)
        with tqdm(total=total_rows, desc="Processing Rows") as pbar:
            with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust max_workers as needed
                futures = [executor.submit(process_row, row, pbar) for row in rows]
                # Wait for all futures to complete
                for future in futures:
                    future.result()

# Read URL and token from var.ini
config = configparser.ConfigParser()
config.read(os.path.join(script_dir, 'var.ini'))
url = config['credentials']['url']
token = config['credentials']['token']

write_data_to_netbox(url, token, 'ipam_addresses.csv')
