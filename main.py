#!/usr/bin/env python3
"""
Script runner that executes multiple Python scripts in sequence.
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
    """
    Configure logging with both file and console handlers.

    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )

    # Create file handlers
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

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    # Add handlers to logger
    logger.addHandler(debug_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)

    return logger

def run_script(script_name: str, logger: logging.Logger) -> bool:
    """
    Executes a Python script using the current Python interpreter.

    Args:
        script_name (str): The name of the script to run.
        logger (logging.Logger): The logger instance for logging messages.

    Returns:
        bool: True if the script runs successfully, False otherwise.
    """
    logger.info(f"Running {script_name}...")

    try:
        # Run the script using the current Python interpreter
        result = subprocess.run(
            [sys.executable, script_name],
            capture_output=True,
            text=True,
            check=True
        )
        logger.info(f"{script_name} completed successfully.")
        logger.debug(f"Output:\n{result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running {script_name}:")
        logger.error(e.stderr)  # Log the error message if the script fails
        return False

def main():
    """
    Main function to run a list of Python scripts sequentially.

    The function will stop execution if any script fails, preventing subsequent
    scripts from running if an error is encountered.
    """
    logger = setup_logging()

    # List of scripts to execute in order
    scripts: List[str] = [
        os.path.join("scripts", "netbox_export.py"),
        os.path.join("scripts", "network_scan.py"),
        os.path.join("scripts", "scan_processor.py"),
        os.path.join("scripts", "netbox_import.py"),
    ]

    # Iterate over the list of scripts and run each one
    for script in scripts:
        if not run_script(script, logger):
            logger.error(f"Execution stopped due to an error in {script}")
            break  # Stop execution if a script fails
    else:
        logger.info("All scripts executed successfully.")

if __name__ == "__main__":
    # Run the main function if the script is executed directly
    main()
