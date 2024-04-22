import csv
import subprocess
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

def read_from_csv(filename):
    """
    Read data from a CSV file.

    Args:
    - filename (str): The path to the CSV file.

    Returns:
    - data (list): A list of dictionaries representing rows from the CSV file.
    """
    with open(filename, 'r') as file:
        reader = csv.DictReader(file)
        data = [row for row in reader]
    return data

def remove_scanned_prefixes(data, scanned_prefixes):
    """
    Remove scanned prefixes from the original data and rewrite it to the CSV file.

    Args:
    - data (list): The original data read from the CSV file.
    - scanned_prefixes (list): A list of scanned prefixes to be removed from the data.
    """
    # Remove the scanned prefixes from the original data
    updated_data = [row for row in data if row['Prefix'] not in scanned_prefixes]
    
    # Rewrite the updated data to the CSV file
    with open('ipam_prefixes.csv', 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=['Prefix', 'Status', 'Tags', 'Tenant'])
        writer.writeheader()
        writer.writerows(updated_data)

def run_nmap_on_prefix(prefix, tenant):
    """
    Run nmap scan on a given prefix.

    Args:
    - prefix (str): The prefix to be scanned.
    - tenant (str): The tenant associated with the prefix.

    Returns:
    - results (list): A list of dictionaries containing scan results.
    - success (bool): True if the scan was successful, False otherwise.
    """
    print(f"Starting scan on prefix: {prefix}")
    # Run nmap on the prefix with DNS resolution and specified DNS servers
    command = f"nmap -sn -R -T3 --min-parallelism 10 {prefix}"
    process = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()

    if error:
        print(f"Error: {error}")
        return [], False

    results = []
    # Parse the standard output
    lines = output.decode().split('\n')
    for line in lines:
        if "Nmap scan report for" in line:
            parts = line.split()
            dns_name = None
            if len(parts) > 5:  # Check if there are more than 5 parts in the line
                dns_name = parts[4]  # Extract DNS name
                address = parts[5]  # Extract IP address
            else:
                address = parts[-1]  # Extract IP address
            # Remove parenthesis from IP address if present
            address = address.strip('()')
            # Include the subnet mask from the prefix in the address
            address_with_mask = f"{address}/{prefix.split('/')[-1]}"
            results.append({
                'address': address_with_mask,
                'dns_name': dns_name,  # Add DNS name to the results
                'status': 'active',
                'description': 'Scanned IP address',
                'tags': 'autoscan',
                'tenant': tenant
            })
    print(f"Finished scan on prefix: {prefix}")
    return results, True

def run_nmap_on_prefixes(data, output_folder):
    """
    Run nmap scans on prefixes in parallel and write results to CSV files.

    Args:
    - data (list): The list of dictionaries containing prefix data.
    - output_folder (str): The directory where output CSV files will be stored.
    """
    results = []
    scanned_prefixes = []

    # Filter rows to scan only those with status 'active' and without the tag 'Disable Automatic Scanning'
    rows_to_scan = [row for row in data if row['Status'] == 'active' and 'Disable Automatic Scanning' not in row['Tags']]

    with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust the max_workers parameter based on your system's capabilities
        # Use executor.map to asynchronously run the scans and get results
        futures = {executor.submit(run_nmap_on_prefix, row['Prefix'], row['Tenant']): row for row in rows_to_scan}

        for future in concurrent.futures.as_completed(futures):
            prefix_results, success = future.result()
            if success:
                results.extend(prefix_results)
                scanned_prefixes.append(futures[future]['Prefix'])
                write_results_to_csv(prefix_results, output_folder)  # Write results to CSV after each prefix scan

    remove_scanned_prefixes(data, scanned_prefixes)
    return results

def write_results_to_csv(results, output_folder):
    """
    Write scan results to CSV files.

    Args:
    - results (list): A list of dictionaries containing scan results.
    - output_folder (str): The directory where output CSV files will be stored.
    """
    # Create the results folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Generate the current date as a string
    current_date = datetime.now().strftime('%Y-%m-%d')

    # Set the filename with the full path including the date
    output_filename = os.path.join(output_folder, f'nmap_results_{current_date}.csv')

    # Check if the file is empty
    is_empty = not os.path.exists(output_filename) or os.stat(output_filename).st_size == 0

    with open(output_filename, 'a', newline='') as file:  # Use 'a' (append) mode to add results to the file
        writer = csv.DictWriter(file, fieldnames=['address', 'dns_name', 'status', 'description', 'tags', 'tenant'])

        # Add headers if the file is empty
        if is_empty:
            writer.writeheader()

        for result in results:
            writer.writerow(result)

data = read_from_csv('ipam_prefixes.csv')
output_folder = 'results'
run_nmap_on_prefixes(data, output_folder)
