# netbox-nmap-scan

Automatically maintain an accurate inventory of active IP addresses in Netbox
by scanning your network prefixes with nmap.

This project is designed as a long-running scheduler that continuously:
- scans active prefixes from Netbox,
- detects appearing and disappearing IP addresses,
- and keeps Netbox synchronized with the current network state.

The design favors correctness, traceability, and safe automation over visual output.


## OVERVIEW

netbox-nmap-scan performs ICMP discovery scans (nmap -sn) against all
Netbox prefixes that meet the following criteria:

- Prefix status is "active"
- Prefix does NOT have the tag "Disable Automatic Scanning"

Each prefix is handled independently and stored in its own folder,
allowing safe concurrency, restartability, and forensic inspection.


## KEY FEATURES

- Automatic discovery of active IP addresses using nmap
- Continuous synchronization with Netbox IPAM
- Per-prefix isolation (each prefix has its own working directory)
- Accurate detection of:
  - new IPs (created in Netbox)
  - disappearing IPs (marked deprecated)
- DNS resolution support (optional)
- Custom tag support for excluding prefixes from scanning
- Centralized logging with daily rotation
- Safe concurrency with configurable worker limits
- Automatic cleanup of historical scan artifacts


## IMPORTANT BEHAVIORAL GUARANTEES

1) ipam_addresses.csv IS ALWAYS LATEST

- ipam_addresses.csv is DELETED at the start of each scheduler cycle.
- It is only recreated when a prefix is successfully scanned.
- This guarantees the file is never stale if:
  - a prefix is skipped,
  - a scan fails,
  - or the scheduler restarts.

If ipam_addresses.csv exists, it represents the most recent valid state.

------------------------------------
2) SCAN HISTORY IS PRESERVED BY COUNT (NOT TIME)

- Each prefix folder keeps ONLY the latest N scan files:
  nmap_results_<YYYY-MM-DD_HH-MM-SS>.csv
- Default: keep last 4 scans per prefix
- Older scan files are automatically deleted

This ensures:
- reliable deprecation detection (comparison always possible)
- bounded disk usage
- predictable behavior regardless of scan frequency

----------------
3) SAFE RESTARTS

- The scheduler can be stopped and restarted at any time
- Prefix folders and scan history are preserved
- No partial or corrupted state is reused


## DIRECTORY STRUCTURE

````
project_root/
├── main.py
├── network_scan.py
├── scan_processor.py
├── netbox_import.py
├── netbox_export.py
├── netbox_connection.py
├── logging_utils.py
├── var.ini
├── logs/
│   ├── scheduler.log
│   ├── scheduler.error.log
│   ├── netbox_import.log
│   └── ...
└── PREFIXES/
    ├── 10.0.0.0_24/
    │   ├── prefix.info
    │   ├── nmap_results_2025-01-28_22-00-00.csv
    │   ├── nmap_results_2025-01-29_02-00-00.csv
    │   └── ipam_addresses.csv
    └── 192.168.1.0_24__VRF-CORP/
        └── ...
````

## PREFIX FOLDER CONTENT

prefix.info
- Contains the CIDR of the prefix
- Used as a guard against folder mis-association

nmap_results_<timestamp>.csv
- Raw nmap discovery results
- One file per scan
- Header-only if no hosts are found

ipam_addresses.csv
- Computed, Netbox-ready view
- Contains:
  - active IPs from the latest scan
  - deprecated IPs detected by comparison with the previous scan
- Deleted automatically if not refreshed


## NETBOX CONFIGURATION

TAGS

Required:
- autoscan
  Used to tag IP addresses managed by this tool

Optional:
- Disable Automatic Scanning
  Apply to prefixes to exclude them from scanning

-----------------------
CUSTOM FIELDS (OPTIONAL)

scantime (DateTime)
- Stores last scan time per IP
- Controlled via enable_scantime in var.ini


## CONFIGURATION (var.ini)

Example configuration:

[credentials]
url   = https://netbox.example.com/
token = <NETBOX_API_TOKEN>

[scan_options]
enable_dns = true
enable_scantime = true
scan_interval_hours = 4
scheduler_sleep_seconds = 300
scan_max_workers = 5
nmap_results_keep_last = 4

[logging]
log_dir = logs
backup_count = 14
root_level = DEBUG
file_level = INFO
console_level = INFO

---------------------
SCAN OPTIONS EXPLAINED

enable_dns
- Enables DNS resolution during nmap scans

enable_scantime
- Writes scan timestamp into Netbox custom field

scan_interval_hours
- Minimum delay between scans of the same prefix

scheduler_sleep_seconds
- Sleep duration between scheduler cycles

scan_max_workers
- Maximum concurrent prefix scans
- Also used as Netbox import worker limit

nmap_results_keep_last
- Number of scan history files preserved per prefix
- Minimum enforced: 2

---------------
LOGGING OPTIONS

- Logs are rotated daily
- Two files per application:
  - <app>.log        (INFO and above)
  - <app>.error.log  (ERROR only)
- backup_count controls retention (in days)


## USAGE

Run the scheduler:

    python3 main.py

Recommended:
- Run as a systemd service or container
- Ensure nmap is available on PATH


## PERFORMANCE NOTES

- Scan duration scales with number and size of prefixes
- Netbox API pressure scales with number of IP changes
- For large environments:
  - Reduce scan_max_workers
  - Increase scan_interval_hours
  - Exclude irrelevant prefixes via tag


## WHAT THIS TOOL DOES NOT DO

- It does NOT enumerate unused IPs
- It does NOT assign IPs arbitrarily
- It does NOT override DHCP-managed IP data
- It does NOT assume scan success means network correctness

The tool reflects observed reality, nothing more.


## ROADMAP

[X] Disable DNS resolution
[X] Toggle scan timestamp tracking
[X] Centralized logging with rotation
[X] Automatic scan artifact cleanup
[ ] Optional DNS server override
[X] Prefix-level scan tuning
[ ] Native systemd unit file


## CONTRIBUTING

Contributions are welcome.

Please ensure:
- Changes are deterministic
- Logging remains explicit and structured
- Netbox safety is preserved (no destructive assumptions)


## SUPPORT

For issues or improvements:
1) Check the Wiki
2) Open a GitHub issue
3) Submit a Pull Request

This project prioritizes correctness over convenience.










FOR WIKI:
nano /etc/systemd/system/netbox-nmap-scheduler.service

sudo systemctl daemon-reload

sudo systemctl enable --now netbox-nmap-scheduler.service

sudo systemctl status netbox-nmap-scheduler.service

journalctl -u netbox-nmap-scheduler.service -f

Put all script file in /opt/netbox-nmap-scheduler/
