#!/usr/bin/env python3
"""logging_utils.py

Centralized logging configuration for this project.

This project can run as:
- a long-running scheduler (``main.py``), and/or
- short-lived helper scripts (e.g. ``netbox_export.py``, ``netbox_import.py``).

To keep logs consistent across entrypoints, this module provides a single
:func:`configure_logging` function that sets up:

- Daily-rotated log files (midnight rotation)
- Separate error log file (ERROR and above)
- Optional console logging
- A consistent log format (timestamps, module, file:line)

Log files
---------
Logs are created in ``<log_dir>`` (default: ``logs`` under the script directory):

- ``<app_name>.log``       : INFO and above (or configured level)
- ``<app_name>.error.log`` : ERROR and above

Configuration source (var.ini)
------------------------------
If present, values are read from the ``[logging]`` section of ``var.ini``::

    [logging]
    log_dir = logs
    backup_count = 14
    root_level = DEBUG
    file_level = INFO
    console_level = INFO

Design notes
------------
- Library modules should *not* attach handlers. They should only do:
  ``logger = logging.getLogger(__name__)``.
- ``configure_logging`` is safe to call more than once: it avoids duplicating handlers.
  This matters if an entrypoint imports another script that also calls this function.
"""

from __future__ import annotations

import configparser
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from typing import Optional, Tuple

# Mapping of string levels (as found in var.ini) to logging module constants.
_LEVEL_MAP = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET,
}


def _parse_level(value: str, default: int) -> int:
    """Parse a log level string.

    Args:
        value: e.g. "INFO", "debug", "WARNING". Empty/unknown -> default.
        default: Level constant to return when parsing fails.

    Returns:
        A logging level constant (e.g. ``logging.INFO``).
    """
    if not value:
        return default
    return _LEVEL_MAP.get(value.strip().upper(), default)


def _read_logging_config(var_ini_path: str) -> Tuple[str, int, int, int, int]:
    """Read logging configuration from ``var.ini``.

    Args:
        var_ini_path: Absolute path to ``var.ini``.

    Returns:
        A tuple ``(log_dir, backup_count, root_level, file_level, console_level)``.

    Notes:
        - Missing ``var.ini`` or missing ``[logging]`` section are not errors.
          Defaults are returned in that case.
        - Invalid integer/level values are ignored and replaced with defaults.
    """
    # Defaults (used if var.ini is absent or incomplete).
    log_dir = "logs"
    backup_count = 14
    root_level = logging.DEBUG
    file_level = logging.INFO
    console_level = logging.INFO

    config = configparser.ConfigParser()
    if not os.path.exists(var_ini_path):
        return log_dir, backup_count, root_level, file_level, console_level

    config.read(var_ini_path)

    if "logging" not in config:
        return log_dir, backup_count, root_level, file_level, console_level

    section = config["logging"]

    log_dir = section.get("log_dir", log_dir).strip() or log_dir

    try:
        backup_count = max(1, int(section.get("backup_count", str(backup_count))))
    except ValueError:
        backup_count = 14

    root_level = _parse_level(section.get("root_level", ""), root_level)
    file_level = _parse_level(section.get("file_level", ""), file_level)
    console_level = _parse_level(section.get("console_level", ""), console_level)

    return log_dir, backup_count, root_level, file_level, console_level


def configure_logging(
    *,
    app_name: str,
    script_dir: str,
    var_ini_name: str = "var.ini",
    logger_name: Optional[str] = None,
) -> logging.Logger:
    """Configure project logging (daily rotation, standardized filenames).

    Call this once from the main entrypoint so that all imported modules inherit the
    same handlers.

    Args:
        app_name: Base filename for logs (e.g. ``scheduler`` or ``netbox_import``).
        script_dir: Directory containing ``var.ini`` and the log directory.
        var_ini_name: Configuration filename (default: ``var.ini``).
        logger_name:
            Which logger to configure.
            - ``None`` (default) configures the *root* logger. This is usually what you want
              for applications so every module inherits handlers.
            - A string configures a named logger only.

    Returns:
        The configured logger object (root logger if ``logger_name`` is None).

    Behavior:
        - Creates the log directory if missing.
        - Adds a daily rotating file handler for the main log.
        - Adds a daily rotating file handler for error log (ERROR+).
        - Adds a console stream handler.
        - Avoids duplicating handlers if called multiple times.
    """
    var_ini_path = os.path.join(script_dir, var_ini_name)
    log_dir_name, backup_count, root_level, file_level, console_level = _read_logging_config(var_ini_path)

    # Allow either an absolute log_dir in var.ini, or a path relative to script_dir.
    log_dir = log_dir_name if os.path.isabs(log_dir_name) else os.path.join(script_dir, log_dir_name)
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(logger_name)  # root if None
    logger.setLevel(root_level)

    # Prevent handler duplication:
    # We key on (type, baseFilename, level). For StreamHandler baseFilename is None.
    existing_keys = {(type(h), getattr(h, "baseFilename", None), h.level) for h in logger.handlers}

    file_fmt = logging.Formatter(
        "%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(name)s - %(message)s"
    )
    console_fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    main_path = os.path.join(log_dir, f"{app_name}.log")
    err_path = os.path.join(log_dir, f"{app_name}.error.log")

    def _add_timed_handler(path: str, level: int) -> None:
        """Attach a TimedRotatingFileHandler if not already present."""
        h = TimedRotatingFileHandler(
            filename=path,
            when="midnight",
            interval=1,
            backupCount=backup_count,
            encoding="utf-8",
            utc=False,
            delay=True,  # file opened on first emit; avoids creating empty logs at import time
        )
        h.setLevel(level)
        h.setFormatter(file_fmt)

        key = (type(h), getattr(h, "baseFilename", None), h.level)
        if key not in existing_keys:
            logger.addHandler(h)
            existing_keys.add(key)

    _add_timed_handler(main_path, file_level)
    _add_timed_handler(err_path, logging.ERROR)

    console = logging.StreamHandler()
    console.setLevel(console_level)
    console.setFormatter(console_fmt)
    key = (type(console), getattr(console, "baseFilename", None), console.level)
    if key not in existing_keys:
        logger.addHandler(console)
        existing_keys.add(key)

    return logger
