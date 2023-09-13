import pynetbox
import requests
import ipaddress
import subprocess
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

NETBOX_URL = "netbox_url"
NETBOX_TOKEN = "netbox_token"


def is_pingable(ip_address_str):
    """
    Check if an IP address is pingable.
    Args:
        ip_address_str (str): The IP address to ping.
    Returns:
        bool: True if the ping was successful, False otherwise.
    """
    try:
        # Execute the ping command and capture the output
        # command = ["ping", "-c", "1", ip_address_str]  # Adjust flags based on your system (Linux/macOS)
        command = ["ping", ip_address_str, "-n", "1", "-w", "500"]  # Windows ping command with 500ms timeout
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Check if the ping was successful based on the output
        if "Destination host unreachable" in result.stdout or "Request timed out" in result.stdout:
            # code to be executed if either string is found in result.stdout
            return False
        else:
            return True
    except Exception as e:
        print(f"Error while pinging {ip_address_str}: {e}")
        return False


def main():
    """
    Main function that performs a series of operations on IP addresses obtained from NetBox.

    This function does the following:
    1. Initializes a session with NetBox API and disables SSL verification.
    2. Retrieves a list of active IP prefixes from NetBox.
    3. Iterates through each active prefix and checks each IP address within the prefix.
    4. If an IP address is pingable, it checks if the IP address exists in NetBox.
       - If the IP address does not exist, it creates the IP address in NetBox with the specified details.
       - If the IP address exists, it updates the status of the IP address to 'active' if necessary.
    5. If an IP address is not pingable, it checks if the IP address exists in NetBox.
       - If the IP address exists and is not pingable, it updates the status of the IP address to 'deprecated'.

    Parameters:
        None

    Returns:
        None
    """
    session = requests.Session()
    session.verify = False
    nb = pynetbox.api(NETBOX_URL, token=NETBOX_TOKEN)
    nb.http_session = session

    # Get a list of active IP prefixes from NetBox
    active_prefixes = nb.ipam.prefixes.filter(status="active", tag=["toscan"])

    # Iterate through active prefixes
    for prefix in active_prefixes:
        ip_network = ipaddress.ip_network(prefix.prefix)
        for ip_address in ip_network.hosts():
            ip_address_str = str(ip_address)

            if is_pingable(ip_address_str):
                ip_data = nb.ipam.ip_addresses.get(address=ip_address_str)
                if ip_data is None:
                    print(f"IP Address: {ip_address_str} - Not found")
                    # Create the IP address in NetBox
                    create_ip_data = {
                        "address": ip_address_str,
                        "status": "active",  # Change this to the appropriate status
                        "description": "Scanned IP address",
                        "tags": [{'name': 'autoscan'}],
                        # Add other fields as needed
                    }
                    nb.ipam.ip_addresses.create(create_ip_data)
                    print(f"IP Address: {ip_address_str} - Added to NetBox")
                else:
                    print(f"IP Address: {ip_address_str} - Status: {ip_data.status}")
                    ip_data = nb.ipam.ip_addresses.get(address=ip_address_str, status="deprecated", tag=["autoscan"])
                    if ip_data is None:
                        print(f"IP Address: {ip_address_str} - Not updated")
                    else:
                        # Update the status of the IP address to 'active'
                        ip_data.status = "active"
                        ip_data.save()
                        print(f"IP Address: {ip_address_str} - Status updated to 'Active'")

            else:
                print(f"IP Address: {ip_address_str} - Not pingable")
                ip_data = nb.ipam.ip_addresses.get(address=ip_address_str, status="active", tag=["autoscan"])
                # Update status for not pingable IPs with the 'autoscan' tag
                if ip_data is None:
                    print(f"IP Address: {ip_address_str} - Not updated")
                else:
                    if not is_pingable(ip_address_str):
                        # Update the status of the IP address to 'Deprecated'
                        ip_data.status = "deprecated"
                        ip_data.save()
                        print(f"IP Address: {ip_address_str} - Status updated to 'Deprecated'")


if __name__ == "__main__":
    main()
