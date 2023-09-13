# netbox-ping-scan

1. Initializes a session with NetBox API
2. Retrieves a list of active IP prefixes from NetBox with custom tag "toscan".
3. Iterates through each active prefix and checks each IP address within the prefix.
4. If an IP address is pingable and have custom tag "autoscan", it checks if the IP address exists in NetBox.
  - If the IP address does not exist, it creates the IP address in NetBox with the specified details.
  - If the IP address exists, it updates the status of the IP address to 'active' if necessary.
5. If an IP address is not pingable, it checks if the IP address exists in NetBox.
  - If the IP address exists and is not pingable, it updates the status of the IP address to 'deprecated'.
