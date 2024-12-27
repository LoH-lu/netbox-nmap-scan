# netbox-nmap-scan

Automatically maintain an up-to-date inventory of active IP addresses in your network using Netbox and nmap. This Python-based tool scans your network prefixes and keeps your Netbox instance synchronized with the current state of your network.

## Features

- Automatic scanning of all active prefixes in Netbox
- Custom tag support for excluding prefixes from scanning
- Tracking of last scan time for each IP address
- DNS resolution support
- Compatible with Python 3.12.6 and Netbox 4.1.10

## Prerequisites

- Python 3.12.6 or later
- Netbox 4.1.10 or later
- nmap installed on your system
- Required Python packages (listed in requirements.txt)

## Setup

1. Create the following Netbox configurations:

   ### Tags
   - `autoscan`: Identifies IP addresses added by this script
   - `Disable Automatic Scanning`: Add this tag to prefixes you want to exclude from scanning
   
   ![Disable Scanning Tag Configuration](https://github.com/henrionlo/netbox-nmap-scan/assets/139378145/b7a223ae-3a55-42cb-8f28-87d282e103c8)
   
   ![Autoscan Tag Configuration](https://github.com/henrionlo/netbox-nmap-scan/assets/139378145/435cec58-1f92-42f2-b4eb-1448a4d22161)

   ### Custom Fields
   Add a custom field to track the last scan time for each IP address:
   
   ![Last Scan Time Custom Field](https://github.com/LoH-lu/netbox-nmap-scan/assets/139378145/c812ee55-71d0-4d8e-9b14-f337a5d867a5)

2. Follow the detailed installation guide in our [Wiki](https://github.com/henrionlo/netbox-nmap-scan/wiki)

## Usage

The script will scan all prefixes with active status in your Netbox instance by default. Scanning time increases with the number of prefixes being scanned.

For detailed usage instructions and examples, please refer to our [Wiki](https://github.com/henrionlo/netbox-nmap-scan/wiki).

## Performance Considerations

- Scanning time scales with the number of prefixes
- Consider scheduling scans during off-peak hours for large networks
- Use the `Disable Automatic Scanning` tag strategically to optimize scan times

## Roadmap

- [ ] DNS server configuration in INI file for custom DNS resolution
- [X] Option to disable DNS resolution functionality
- [ ] Toggle for last scan time tracking
- [ ] Toggle for the progress bar display while importing
- [ ] All-in-One setup script for easier deployment

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues, questions, or contributions, please:
1. Check our [Wiki](https://github.com/henrionlo/netbox-nmap-scan/wiki)
2. Open an issue in this repository
3. Submit a pull request with your proposed changes
