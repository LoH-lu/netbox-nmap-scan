# netbox-ping-scan

This is a simple Python script that achieve the purpose of keeping an updated ip IP Address of active element in your network.
Only IP added with the tag "autoscan" will be modified, as long you don't have this tag, the script will not touch your existing manual IP.
Anyway, even with the tag, it will only update the status of the IP Address.

If you have a lot of Prefixes to scan, the script will take a long time to finish. Because it is scanning each IP one by one and send the result to Netbox one by one as well.
I'm working on a new version that will run multiple ping at once and keep them in a database/list and push all the changes at once to Netbox.

Tested and working with Python 3.11 and Netbox 3.5.x - 3.6.x

1. Initializes a session with NetBox API
2. Retrieves a list of active IP prefixes from NetBox with custom tag "toscan".
3. Iterates through each active prefix and checks each IP address within the prefix.
4. If an IP address is pingable and have custom tag "autoscan", it checks if the IP address exists in NetBox.
  - If the IP address does not exist, it creates the IP address in NetBox with the specified details.
  - If the IP address exists, it updates the status of the IP address to 'active' if necessary.
5. If an IP address is not pingable, it checks if the IP address exists in NetBox.
  - If the IP address exists and is not pingable, it updates the status of the IP address to 'deprecated'.
