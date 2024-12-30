"""Script runner that executes multiple Python scripts in sequence.

This module provides functionality to run multiple Python scripts in order,
with comprehensive logging of execution results.
"""

import subprocess
import sys
import os
import logging
from datetime import datetime
from typing import List

# Define the directory for logs and scripts
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, 'logs')

# Ensure the log directory exists
os.makedirs(LOG_DIR, exist_ok=True)


def setup_logging() -> logging.Logger:
    """Configure logging with both file and console handlers.

    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    debug_handler = logging.FileHandler(
        os.path.join(LOG_DIR, f'script_execution_debug_{timestamp}.log')
    )
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(file_formatter)

    error_handler = logging.FileHandler(
        os.path.join(LOG_DIR, f'script_execution_error_{timestamp}.log')
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


def run_script(script_name: str, logger: logging.Logger) -> bool:
    """Execute a Python script using the current Python interpreter.

    Args:
        script_name: The name of the script to run
        logger: The logger instance for logging messages

    Returns:
        bool: True if the script runs successfully, False otherwise
    """
    logger.info("Running %s...", script_name)

    try:
        result = subprocess.run(
            [sys.executable, script_name],
            capture_output=True,
            text=True,
            check=True
        )
        logger.info("%s completed successfully.", script_name)
        logger.debug("Output:\n%s", result.stdout)
        return True
    except subprocess.CalledProcessError as error:
        logger.error("Error running %s:", script_name)
        logger.error(error.stderr)
        return False


def main() -> None:
    """Run a list of Python scripts sequentially.

    The function will stop execution if any script fails, preventing subsequent
    scripts from running if an error is encountered.
    """
    logger = setup_logging()

    scripts: List[str] = [
        "netbox_export.py",
        "network_scan.py",
        "scan_processor.py",
        "netbox_import.py"
    ]

    for script in scripts:
        if not run_script(script, logger):
            logger.error("Execution stopped due to an error in %s", script)
            sys.exit(1)

    logger.info("All scripts executed successfully.")


if __name__ == "__main__":
    main()
