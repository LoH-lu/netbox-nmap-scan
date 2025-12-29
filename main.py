#!/usr/bin/env python3
"""Script runner that executes multiple Python modules in sequence.

This project keeps runnable modules in the `scripts/` package and uses this entry
point to execute them in a fixed order with consistent logging.
"""

import logging
import os
import subprocess
import sys
from datetime import datetime
from typing import List

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)


def setup_logging() -> logging.Logger:
    """Configure logging with both file and console handlers."""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # Avoid duplicate handlers if main() is called multiple times
    if logger.handlers:
        return logger

    file_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
    )
    console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    debug_handler = logging.FileHandler(
        os.path.join(LOG_DIR, f"script_execution_debug_{timestamp}.log")
    )
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(file_formatter)

    error_handler = logging.FileHandler(
        os.path.join(LOG_DIR, f"script_execution_error_{timestamp}.log")
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


def run_module(module_name: str, logger: logging.Logger) -> bool:
    """Execute a Python module (via -m) using the current interpreter."""
    logger.info(f"Running {module_name}...")

    try:
        result = subprocess.run(
            [sys.executable, "-m", module_name],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        logger.info(f"{module_name} completed successfully.")
        if result.stdout:
            logger.debug("Output:\n%s", result.stdout)
        if result.stderr:
            logger.debug("Stderr:\n%s", result.stderr)
        return True
    except subprocess.CalledProcessError as exc:
        logger.error("Error running %s:", module_name)
        if exc.stdout:
            logger.error("Stdout:\n%s", exc.stdout)
        if exc.stderr:
            logger.error("Stderr:\n%s", exc.stderr)
        return False


def main() -> None:
    """Run the pipeline sequentially; stop on first failure."""
    logger = setup_logging()

    modules: List[str] = [
        "scripts.netbox_export",
        "scripts.network_scan",
        "scripts.scan_processor",
        "scripts.netbox_import",
    ]

    for module in modules:
        if not run_module(module, logger):
            logger.error("Execution stopped due to an error in %s", module)
            break
    else:
        logger.info("All modules executed successfully.")


if __name__ == "__main__":
    main()
