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

    Raises:
    - Exception: If the connection to Netbox fails.
    """
    # Create a custom requests session with SSL verification disabled
    session = requests.Session()
    session.verify = False  # Disabling SSL verification for the session

    # Test the connection by making a direct request to /api/status/
    try:
        headers = {"Authorization": f"Token {token}"}
        response = session.get(f"{url}/api/status/", headers=headers)
        response.raise_for_status()  # Raise an error for HTTP status codes 4xx/5xx

        # Check if the response contains the "netbox-version" key
        status_data = response.json()
        if "netbox-version" in status_data:
            # Create and return the Netbox API object
            netbox = pynetbox.api(url, token)
            netbox.http_session = session
            return netbox
        else:
            raise Exception("Unexpected response from Netbox /api/status/ endpoint.")
    except Exception as e:
        raise Exception(f"Failed to connect to Netbox: {e}")
