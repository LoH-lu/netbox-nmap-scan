#!/usr/bin/env python3
"""logging_utils.py

Centralized logging configuration for this project.

Standard log files (in <log_dir>):
- <app_name>.log        : INFO and above (rotated daily)
- <app_name>.error.log  : ERROR and above (rotated daily)

Rotation:
- TimedRotatingFileHandler, rotated at midnight (local time by default)
- backup_count controls how many rotated files are kept

Configuration source:
- var.ini [logging] section (optional). If missing, sensible defaults are used.

Recommended usage:
- In the main entrypoint (e.g., scheduler main.py), call configure_logging(...)
  once so that all imported modules inherit the same handlers.
- Library modules should *not* configure handlers; they should only do:
      logger = logging.getLogger(__name__)
"""

from __future__ import annotations

import configparser
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from typing import Optional, Tuple

_LEVEL_MAP = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET,
}


def _parse_level(value: str, default: int) -> int:
    if not value:
        return default
    return _LEVEL_MAP.get(value.strip().upper(), default)


def _read_logging_config(var_ini_path: str) -> Tuple[str, int, int, int, int]:
    """Read logging config from var.ini.

    Returns:
        (log_dir, backup_count, root_level, file_level, console_level)
    """
    # Defaults
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
    """Configure logging (daily rotation, standardized filenames).

    Args:
        app_name: base filename (e.g. 'scheduler', 'netbox_import').
        script_dir: directory containing var.ini and logs directory.
        var_ini_name: configuration filename (default: var.ini).
        logger_name: which logger to attach handlers to. None => root logger.

    Returns:
        The configured logger instance.

    Notes:
        - Safe to call multiple times; handlers are not duplicated.
        - When configuring the root logger, all modules will inherit handlers.
    """
    var_ini_path = os.path.join(script_dir, var_ini_name)
    log_dir_name, backup_count, root_level, file_level, console_level = _read_logging_config(var_ini_path)

    log_dir = log_dir_name if os.path.isabs(log_dir_name) else os.path.join(script_dir, log_dir_name)
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(logger_name)  # root if None
    logger.setLevel(root_level)

    # Avoid duplicate handlers (important when imported and called multiple times).
    existing_keys = {(type(h), getattr(h, "baseFilename", None), h.level) for h in logger.handlers}

    file_fmt = logging.Formatter(
        "%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(name)s - %(message)s"
    )
    console_fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    main_path = os.path.join(log_dir, f"{app_name}.log")
    err_path = os.path.join(log_dir, f"{app_name}.error.log")

    def _add_timed_handler(path: str, level: int) -> None:
        h = TimedRotatingFileHandler(
            filename=path,
            when="midnight",
            interval=1,
            backupCount=backup_count,
            encoding="utf-8",
            utc=False,
            delay=True,
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
