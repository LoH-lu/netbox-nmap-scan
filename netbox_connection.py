import pynetbox
import requests

def connect_to_netbox(url, token):
    """
    Connect to the Netbox API using the provided URL and token.

    Args:
    - url (str): The base URL of the Netbox instance.
    - token (str): The authentication token for accessing the Netbox API.

    Returns:
    - netbox (pynetbox.core.api.Api): The Netbox API object configured to use the provided URL and token.
    """
    # Create a custom requests session with SSL verification disabled
    session = requests.Session()
    session.verify = False  # Disabling SSL verification for the session

    # Create a Netbox API object without specifying any session
    netbox = pynetbox.api(url, token)

    # Set the custom session for the Netbox API object's requests session
    netbox.http_session = session

    return netbox
