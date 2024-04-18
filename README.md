# netbox-nmap-scan

This is a simple Python scripts that achieve the purpose of keeping an updated list of IP Address which are active/responding in your network.
To achieve that we are using nmap.

By default the scripts is scanning all prefixes with the active status inside your Netbox instance.
If you don't want a prefix to get scan, create a new tag 'Disable Automatic Scanning'

![image](https://github.com/henrionlo/netbox-nmap-scan/assets/139378145/b7a223ae-3a55-42cb-8f28-87d282e103c8)

Create as well the tag 'autoscan', this will allow you to quickly know which IP Addresses has been added by the script.

![image](https://github.com/henrionlo/netbox-nmap-scan/assets/139378145/435cec58-1f92-42f2-b4eb-1448a4d22161)

The more prefixes you want to scan, the more time it will require to finish.

Tested and working with Python 3.12.2 and Netbox 3.6.x - 3.7.x

The How-To are located in https://github.com/henrionlo/netbox-nmap-scan/wiki

TODO
- Add DNS server selection for the nmap command in the ini file (if required to have a different one from the system DNS running the script)
- Allow users to disable the DNS part of the script and only run the regular nmap command
- Cleanup of code and import
- Adding more description
- All-in-One script for easier setup
