import csv
import os
import configparser
import netbox_connection

def get_ipam_prefixes(netbox):
    """
    Retrieve all IPAM prefixes from Netbox.

    Args:
    - netbox (pynetbox.core.api.Api): The Netbox API object.

    Returns:
    - ipam_prefixes (pynetbox.core.query.Request): All IPAM prefixes retrieved from Netbox.
    """
    ipam_prefixes = netbox.ipam.prefixes.all()
    return ipam_prefixes

def write_to_csv(data, filename):
    """
    Write IPAM prefixes data to a CSV file.

    Args:
    - data (pynetbox.core.query.Request): IPAM prefixes data retrieved from Netbox.
    - filename (str): Name of the CSV file to write data to.
    """
    script_dir = os.path.dirname(os.path.realpath(__file__))  # Get the directory of the running script
    file_path = os.path.join(script_dir, filename)  # Construct the full path to the output file
    with open(file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Prefix', 'VRF', 'Status', 'Tags', 'Tenant'])  # Writing headers
        for prefix in data:
            tag_names = [tag.name for tag in prefix.tags]
            tenant_name = prefix.tenant.name if prefix.tenant else 'N/A'
            status_value = prefix.status.value if prefix.status else 'N/A'  # Extract the value of the status field
            vrf_name = prefix.vrf.name if prefix.vrf else 'N/A'  # Extract the name of the VRF
            writer.writerow([prefix.prefix, vrf_name, status_value, ', '.join(tag_names), tenant_name])

# Read URL and token from var.ini
config = configparser.ConfigParser()
config.read('var.ini')
url = config['credentials']['url']
token = config['credentials']['token']

netbox = netbox_connection.connect_to_netbox(url, token)

ipam_prefixes = get_ipam_prefixes(netbox)
write_to_csv(ipam_prefixes, 'ipam_prefixes.csv')
