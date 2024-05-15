import csv
from datetime import datetime
import os

def get_file_path(directory, date_time):
    """
    Generate a file path based on the directory and date.

    Args:
    - directory (str): The directory where the file will be located.
    - date (datetime.datetime): The date to be included in the file name.

    Returns:
    - file_path (str): The full file path based on the directory and date.
    """
    return os.path.join(directory, f'nmap_results_{date_time.strftime("%Y-%m-%d_%H-%M-%S")}.csv')

def get_latest_files(directory, num_files=2):
    """
    Get the list of CSV files in a directory and sort them by modification time.

    Args:
    - directory (str): The directory to search for CSV files.
    - num_files (int): The number of latest files to retrieve.

    Returns:
    - files (list): The list of latest CSV file names.
    """
    files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    files.sort(key=lambda x: os.path.getmtime(os.path.join(directory, x)), reverse=True)
    return files[:num_files]

# Directory for result files
directory = 'results/'

# Get the two latest file paths
latest_files = get_latest_files(directory)
file_paths = [get_file_path(directory, datetime.strptime(file_name[13:32], "%Y-%m-%d_%H-%M-%S")) for file_name in latest_files]

# Output file path
output_file_path = 'ipam_addresses.csv'

def read_csv(file_path):
    """
    Read a CSV file and return a dictionary with addresses as keys.

    Args:
    - file_path (str): The path to the CSV file.

    Returns:
    - data (dict): A dictionary with addresses as keys and corresponding row data as values.
    """
    data = {}
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            address = row['address']
            data[address] = row
    return data

def write_csv(data, file_path):
    """
    Write data to a new CSV file.

    Args:
    - data (dict): A dictionary containing row data with addresses as keys.
    - file_path (str): The path to the output CSV file.
    """
    with open(file_path, 'w', newline='') as file:
        fieldnames = ['address', 'dns_name', 'status', 'scantime', 'tags', 'tenant', 'VRF']  # Added 'VRF' to fieldnames
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        
        # Write header
        writer.writeheader()

        # Write data
        for row in data.values():
            writer.writerow(row)

# Read data from the latest file
data = read_csv(file_paths[0])

# Check for deprecated addresses in the older file and update their status
if len(file_paths) == 2:
    older_data = read_csv(file_paths[1])
    for address, older_row in older_data.items():
        if address not in data:
            # Address is missing in latest file, mark as deprecated
            older_row['status'] = 'deprecated'
            data[address] = older_row

# Write the updated data to the new CSV file
write_csv(data, output_file_path)

print("Comparison and processing completed. Check the output file:", output_file_path)
