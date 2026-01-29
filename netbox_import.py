#!/usr/bin/env python3
"""
Netbox IP Address Management Script.

Updated: tag resolution now looks up tags in Netbox by name or slug and
uses the canonical tag name when attaching tags to IP addresses. If a tag
is missing, the script attempts to create it as a fallback.
"""

import csv
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import configparser
from typing import Dict, List, Optional
import urllib3
import pynetbox
from tqdm import tqdm
from netbox_connection import connect_to_netbox

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")
MAX_WORKERS = 5

os.makedirs(LOG_DIR, exist_ok=True)


def setup_logging() -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    file_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
    )
    console_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    )

    debug_handler = logging.FileHandler(
        os.path.join(LOG_DIR, "netbox_import_debug.log")
    )
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(file_formatter)

    error_handler = logging.FileHandler(
        os.path.join(LOG_DIR, "netbox_import_error.log")
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    logger.addHandler(debug_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)

    return logger


def _slugify(s: str) -> str:
    """Simple slugify: lowercase, replace spaces with '-', keep alnum and '-'."""
    s = s.strip().lower()
    allowed = []
    for ch in s:
        if ch.isalnum() or ch == "-":
            allowed.append(ch)
        elif ch.isspace() or ch in ("_", "."):
            allowed.append("-")
        # drop other punctuation
    slug = "".join(allowed)
    # collapse multiple hyphens
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-")


def resolve_tags(netbox_instance: pynetbox.api, tags_string: str) -> List[Dict[str, str]]:
    """
    Resolve a comma-separated tag string to a list of tag dictionaries accepted by Netbox.

    For each token:
      - Try to find tag by exact name (display name)
      - If not found, try by slug
      - If still not found, attempt to create the tag (fallback)
    Returns list like: [{"name": "Automatic Scanning"}, {"name": "Other Tag"}]
    """
    logger = logging.getLogger(__name__)
    tags_out: List[Dict[str, str]] = []

    if not tags_string:
        return tags_out

    tokens = [t.strip() for t in tags_string.split(",") if t.strip()]
    for token in tokens:
        # Try exact name
        try:
            tag_obj = netbox_instance.extras.tags.get(name=token)
            if tag_obj:
                logger.debug("Resolved tag token '%s' to tag name '%s' (by name)", token, tag_obj.name)
                tags_out.append({"name": tag_obj.name})
                continue
        except Exception:
            logger.debug("Error while looking up tag by name: %s", token, exc_info=True)

        # Try by slug
        try:
            token_slug = _slugify(token)
            tag_obj = netbox_instance.extras.tags.get(slug=token_slug)
            if tag_obj:
                logger.debug("Resolved tag token '%s' to tag name '%s' (by slug)", token, tag_obj.name)
                tags_out.append({"name": tag_obj.name})
                continue
        except Exception:
            logger.debug("Error while looking up tag by slug: %s", token, exc_info=True)

        # Fallback: attempt to create the tag.
        # Use the original token as display name and slugified token as slug.
        try:
            created = netbox_instance.extras.tags.create({
                "name": token,
                "slug": _slugify(token)
            })
            if created:
                logger.info("Created missing tag '%s' with slug '%s'", created.name, created.slug)
                tags_out.append({"name": created.name})
                continue
        except Exception as exc:
            logger.warning(
                "Could not create tag '%s' (will skip it). Error: %s", token, str(exc)
            )
            # Skip this tag if cannot create

    # Remove duplicates preserving order
    seen = set()
    deduped: List[Dict[str, str]] = []
    for t in tags_out:
        if t["name"] not in seen:
            deduped.append(t)
            seen.add(t["name"])

    return deduped


def process_row(row: Dict[str, str], pbar: tqdm, netbox_instance: pynetbox.api) -> None:
    logger = logging.getLogger(__name__)
    address = row.get("address", "unknown")

    try:
        logger.debug("Starting to process address: %s", address)

        tags_list = resolve_tags(netbox_instance, row.get("tags", ""))
        logger.debug("Resolved tags for %s: %s", address, tags_list)

        tenant_data = {"name": row["tenant"]} if row.get("tenant") and row["tenant"] != "N/A" else None

        vrf_data = None
        if row.get("VRF") and row["VRF"] != "N/A":
            try:
                vrf = netbox_instance.ipam.vrfs.get(name=row["VRF"])
                if vrf:
                    vrf_data = {"id": vrf.id}
                    logger.debug("Found VRF %s with ID %s", row["VRF"], vrf.id)
                else:
                    logger.warning("VRF %s not found in Netbox", row["VRF"])
            except Exception as e:
                logger.error("Error looking up VRF %s: %s", row["VRF"], str(e))

        existing_address = None
        try:
            existing_address = netbox_instance.ipam.ip_addresses.get(
                address=address,
                vrf_id=vrf_data["id"] if vrf_data else None,
            )
        except Exception as exc:
            logger.error("Error querying existing address %s: %s", address, str(exc), exc_info=True)

        if existing_address:
            _update_existing_address(
                existing_address, row, tags_list, tenant_data, vrf_data
            )
        else:
            _create_new_address(
                netbox_instance, address, row, tags_list, tenant_data, vrf_data
            )

    except Exception:
        logger.error("Failed to process row for address %s", address, exc_info=True)
        raise
    finally:
        if pbar:
            pbar.update(1)


def _update_existing_address(
    existing_address: object,
    row: Dict[str, str],
    tags_list: List[Dict[str, str]],
    tenant_data: Optional[Dict[str, str]],
    vrf_data: Optional[Dict[str, str]],
) -> None:
    logger = logging.getLogger(__name__)
    logger.info("Updating existing address: %s", row["address"])

    try:
        # Normalize existing tags to list of dicts with name
        existing_tags = existing_address.tags
        normalized_existing = []

        if existing_tags:
            # existing_tags items may be objects or dicts
            for t in existing_tags:
                if isinstance(t, dict):
                    tname = t.get("name") or t.get("slug") or str(t)
                    normalized_existing.append({"name": tname})
                else:
                    # object
                    try:
                        tname = t.name
                    except Exception:
                        tname = str(t)
                    normalized_existing.append({"name": tname})

        # Merge tags: keep canonical names and avoid duplicates
        existing_names = {t["name"] for t in normalized_existing}
        merged_tags = normalized_existing.copy()

        for tag in tags_list:
            if tag["name"] not in existing_names:
                merged_tags.append(tag)
                existing_names.add(tag["name"])

        # Update fields depending on DHCP or not
        status_value = getattr(existing_address.status, "value", None)
        if status_value and status_value.lower() == "dhcp":
            existing_address.custom_fields = existing_address.custom_fields or {}
            existing_address.custom_fields.update({"scantime": row.get("scantime")})
            existing_address.tags = merged_tags
            logger.debug("Updated scantime and tags for DHCP address: %s", row["address"])
        else:
            existing_address.status = row.get("status")
            existing_address.custom_fields = existing_address.custom_fields or {}
            existing_address.custom_fields.update({"scantime": row.get("scantime")})
            if row.get("dns_name"):
                existing_address.dns_name = row.get("dns_name")
            existing_address.tags = merged_tags
            existing_address.tenant = tenant_data
            existing_address.vrf = vrf_data
            logger.debug("Updated all fields for non-DHCP address: %s", row["address"])

        existing_address.save()
        logger.debug("Successfully updated address %s", row["address"])

    except pynetbox.core.query.RequestError as exc:
        # Surface Netbox request errors with more context
        logger.error("Error updating address %s: %s", row["address"], str(exc), exc_info=True)
        raise
    except Exception as exc:
        logger.error("Unexpected error updating address %s: %s", row["address"], str(exc), exc_info=True)
        raise


def _create_new_address(
    netbox_instance: pynetbox.api,
    address: str,
    row: Dict[str, str],
    tags_list: List[Dict[str, str]],
    tenant_data: Optional[Dict[str, str]],
    vrf_data: Optional[Dict[str, str]],
) -> None:
    logger = logging.getLogger(__name__)
    logger.info("Creating new address: %s", address)

    try:
        # netbox expects tags as list of dicts with 'name' or tag IDs; pass names we resolved
        netbox_instance.ipam.ip_addresses.create(
            address=address,
            status=row.get("status"),
            custom_fields={"scantime": row.get("scantime")},
            dns_name=row.get("dns_name"),
            tags=tags_list,
            tenant=tenant_data,
            vrf=vrf_data,
        )
        logger.debug("Successfully created new address: %s", address)

    except pynetbox.core.query.RequestError as exc:
        if "Duplicate IP address" in str(exc):
            logger.warning("Duplicate IP address found: %s", address)
        else:
            logger.error("Error creating address %s: %s", address, str(exc), exc_info=True)
            raise
    except Exception as exc:
        logger.error("Unexpected error creating address %s: %s", address, str(exc), exc_info=True)
        raise


def write_data_to_netbox(url: str, token: str, csv_file: str) -> None:
    """
    Write data from a CSV file to Netbox.

    csv_file can be relative to SCRIPT_DIR or an absolute path.
    """
    logger = logging.getLogger(__name__)

    config = configparser.ConfigParser()
    config.read(os.path.join(SCRIPT_DIR, "var.ini"))
    show_progress = config.getboolean("scan_options", "show_progress", fallback=True)

    try:
        logger.info("Initializing Netbox connection...")
        netbox_instance = connect_to_netbox(url, token)
        logger.info("Successfully connected to Netbox")

        if os.path.isabs(csv_file):
            csv_file_path = csv_file
        else:
            csv_file_path = os.path.join(SCRIPT_DIR, csv_file)

        logger.debug("Reading CSV file from: %s", csv_file_path)

        with open(csv_file_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            rows = list(reader)
            total_rows = len(rows)
            logger.info("Found %d rows to process", total_rows)

            if show_progress:
                pbar = tqdm(total=total_rows, desc="Processing Rows")
            else:
                pbar = None

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [
                    executor.submit(
                        process_row, row, pbar if show_progress else None, netbox_instance
                    )
                    for row in rows
                ]

                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception:
                        logger.error("Error in thread pool execution", exc_info=True)
                        continue

            if pbar:
                pbar.close()

        logger.info("Completed processing all rows from %s", csv_file_path)

    except Exception as exc:
        logger.error(
            "Fatal error in write_data_to_netbox for %s: %s", csv_file, str(exc), exc_info=True
        )
        raise


def main() -> None:
    """
    Main entry point of the script.

    Uses the default global ipam_addresses.csv file.
    """
    logger = setup_logging()
    logger.info("Starting Netbox import process")

    try:
        config = configparser.ConfigParser()
        config_path = os.path.join(SCRIPT_DIR, "var.ini")
        config.read(config_path)

        url = config["credentials"]["url"]
        token = config["credentials"]["token"]

        logger.debug("Configuration loaded from: %s", config_path)
        write_data_to_netbox(url, token, os.path.join(SCRIPT_DIR, "ipam_addresses.csv"))
        logger.info("Netbox import process completed successfully")

    except Exception:
        logger.error("Script execution failed", exc_info=True)
        raise


if __name__ == "__main__":
    main()
