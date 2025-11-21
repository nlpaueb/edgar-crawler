#!/usr/bin/env python3
"""
Flexible Extractor for EDGAR Filings

This script provides a flexible way to extract specific items from EDGAR filings
using custom configuration files. It supports resume functionality to continue
extraction from where it was interrupted.

Usage:
    python flexible_extractor.py --config extraction_configs/mda_only.json
"""

import argparse
import json
import logging
import os
import sys
from typing import Dict, Any

import numpy as np
import pandas as pd
from pathos.pools import ProcessPool
from tqdm import tqdm

from __init__ import DATASET_DIR
from extract_items import ExtractItems
from logger import Logger

# Instantiate a logger object
LOGGER = Logger(name="FlexibleExtractor").get_logger()


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load the extraction configuration from a JSON file.

    Args:
        config_path (str): Path to the configuration JSON file

    Returns:
        Dict[str, Any]: The configuration dictionary

    Raises:
        FileNotFoundError: If the configuration file doesn't exist
        json.JSONDecodeError: If the configuration file is not valid JSON
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, 'r') as f:
        config = json.load(f)

    # Validate required fields
    required_fields = [
        'raw_filings_folder',
        'extracted_filings_folder',
        'filings_metadata_file',
        'filing_types',
        'skip_extracted_filings'
    ]

    missing_fields = [field for field in required_fields if field not in config]
    if missing_fields:
        raise ValueError(f"Configuration file missing required fields: {missing_fields}")

    return config


def count_already_processed(filings_metadata_df: pd.DataFrame,
                            extracted_filings_folder: str) -> int:
    """
    Count how many filings have already been processed.

    Args:
        filings_metadata_df (pd.DataFrame): DataFrame with filings metadata
        extracted_filings_folder (str): Path to the extracted filings folder

    Returns:
        int: Number of already processed filings
    """
    already_processed = 0
    for _, filing in filings_metadata_df.iterrows():
        json_filename = f'{filing["filename"].split(".")[0]}.json'
        absolute_json_filename = os.path.join(
            extracted_filings_folder,
            filing["Type"],
            json_filename
        )
        if os.path.exists(absolute_json_filename):
            already_processed += 1

    return already_processed


def main():
    """
    Main function to extract items from EDGAR filings using a flexible configuration.
    Supports resume functionality to continue from where the process was interrupted.
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description='Extract specific items from EDGAR filings using a custom configuration file.'
    )
    parser.add_argument(
        '--config',
        type=str,
        required=True,
        help='Path to the extraction configuration JSON file (e.g., extraction_configs/mda_only.json)'
    )
    parser.add_argument(
        '--processes',
        type=int,
        default=1,
        help='Number of parallel processes to use for extraction (default: 1)'
    )

    args = parser.parse_args()

    # Load configuration
    try:
        config = load_config(args.config)
        LOGGER.info(f"Loaded configuration from: {args.config}")
        if 'description' in config:
            LOGGER.info(f"Configuration description: {config['description']}")
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as e:
        LOGGER.error(f"Error loading configuration: {e}")
        sys.exit(1)

    # Construct paths
    filings_metadata_filepath = os.path.join(
        DATASET_DIR, config["filings_metadata_file"]
    )

    # Check if the filings metadata file exists
    if not os.path.exists(filings_metadata_filepath):
        LOGGER.error(f'Filings metadata file not found: "{filings_metadata_filepath}"')
        sys.exit(1)

    # Load filings metadata
    filings_metadata_df = pd.read_csv(filings_metadata_filepath, dtype=str)
    filings_metadata_df = filings_metadata_df.replace({np.nan: None})
    LOGGER.info(f"Loaded {len(filings_metadata_df)} filings from metadata file")

    # Filter by filing types if specified
    if config["filing_types"]:
        filings_metadata_df = filings_metadata_df[
            filings_metadata_df["Type"].isin(config["filing_types"])
        ]
        LOGGER.info(f"Filtered to {len(filings_metadata_df)} filings of types: {config['filing_types']}")

    if len(filings_metadata_df) == 0:
        LOGGER.warning(f"No filings to process for filing types {config['filing_types']}.")
        sys.exit(0)

    # Set up folders
    raw_filings_folder = os.path.join(DATASET_DIR, config["raw_filings_folder"])
    if not os.path.isdir(raw_filings_folder):
        LOGGER.error(f'Raw filings folder not found: "{raw_filings_folder}"')
        sys.exit(1)

    extracted_filings_folder = os.path.join(
        DATASET_DIR, config["extracted_filings_folder"]
    )

    # Create the extracted filings folder if it doesn't exist
    if not os.path.isdir(extracted_filings_folder):
        os.mkdir(extracted_filings_folder)
        LOGGER.info(f"Created extracted filings folder: {extracted_filings_folder}")

    # Check for already processed filings if resume is enabled
    if config.get("skip_extracted_filings", False):
        already_processed = count_already_processed(filings_metadata_df, extracted_filings_folder)
        if already_processed > 0:
            LOGGER.info(f"Resume mode enabled: Found {already_processed} already processed filings that will be skipped")
        remaining = len(filings_metadata_df) - already_processed
        if remaining > 0:
            LOGGER.info(f"Will process {remaining} remaining filings")
        else:
            LOGGER.info("All filings have already been processed!")
            sys.exit(0)

    # Initialize the extraction object
    extraction = ExtractItems(
        remove_tables=config.get("remove_tables", True),
        items_to_extract=config.get("items_to_extract", []),
        include_signature=config.get("include_signature", False),
        raw_files_folder=raw_filings_folder,
        extracted_files_folder=extracted_filings_folder,
        skip_extracted_filings=config.get("skip_extracted_filings", False),
    )

    # Log extraction parameters
    items_str = ", ".join(config.get("items_to_extract", [])) if config.get("items_to_extract") else "all items"
    LOGGER.info(f"Extracting items: {items_str}")
    LOGGER.info(f"Remove tables: {config.get('remove_tables', True)}")
    LOGGER.info(f"Include signature: {config.get('include_signature', False)}")

    LOGGER.info(f"\nStarting extraction from {len(filings_metadata_df)} EDGAR filings...")

    # Convert DataFrame to list of series for processing
    list_of_series = list(zip(*filings_metadata_df.iterrows()))[1]

    # Process filings in parallel using a process pool
    num_processes = max(1, min(args.processes, os.cpu_count() or 1))
    LOGGER.info(f"Using {num_processes} parallel process(es)")

    with ProcessPool(processes=num_processes) as pool:
        processed = list(
            tqdm(
                pool.imap(extraction.process_filing, list_of_series),
                total=len(list_of_series),
                ncols=100,
                desc="Extracting"
            )
        )

    # Report results
    num_processed = sum(processed)
    num_skipped = len(processed) - num_processed

    LOGGER.info("\n" + "="*60)
    LOGGER.info("Extraction completed successfully!")
    LOGGER.info(f"Total filings in metadata: {len(filings_metadata_df)}")
    LOGGER.info(f"Newly processed: {num_processed}")
    LOGGER.info(f"Skipped (already extracted): {num_skipped}")
    LOGGER.info(f"Extracted filings saved to: {extracted_filings_folder}")
    LOGGER.info("="*60)


if __name__ == "__main__":
    main()
