"""
Fast Multi-Process Item Extractor for Google Colab
This version uses multiple CPU cores to speed up extraction significantly.
"""
import json
import os
from extract_items import main, ExtractItems, DATASET_DIR, LOGGER
import pandas as pd
import numpy as np
from pathos.pools import ProcessPool
from tqdm import tqdm


def main_fast(num_processes=4):
    """
    Fast extraction using multiple processes

    Args:
        num_processes (int): Number of parallel processes to use (default: 4)
                            Colab typically has 2-4 CPU cores available
    """

    with open("config.json") as fin:
        config = json.load(fin)["extract_items"]

    filings_metadata_filepath = os.path.join(
        DATASET_DIR, config["filings_metadata_file"]
    )

    # Check if the filings metadata file exists
    if os.path.exists(filings_metadata_filepath):
        filings_metadata_df = pd.read_csv(filings_metadata_filepath, dtype=str)
        filings_metadata_df = filings_metadata_df.replace({np.nan: None})
    else:
        LOGGER.info(f'No such file "{filings_metadata_filepath}"')
        return

    # If the user provided filing types, filter out the filings that are not in the list
    if config["filing_types"]:
        filings_metadata_df = filings_metadata_df[
            filings_metadata_df["Type"].isin(config["filing_types"])
        ]
    if len(filings_metadata_df) == 0:
        LOGGER.info(f"No filings to process for filing types {config['filing_types']}.")
        return

    raw_filings_folder = os.path.join(DATASET_DIR, config["raw_filings_folder"])

    # Check if the raw filings folder exists
    if not os.path.isdir(raw_filings_folder):
        LOGGER.info(f'No such directory: "{raw_filings_folder}')
        return

    extracted_filings_folder = os.path.join(
        DATASET_DIR, config["extracted_filings_folder"]
    )

    # Create the extracted filings folder if it doesn't exist
    if not os.path.isdir(extracted_filings_folder):
        os.mkdir(extracted_filings_folder)

    extraction = ExtractItems(
        remove_tables=config["remove_tables"],
        items_to_extract=config["items_to_extract"],
        include_signature=config["include_signature"],
        raw_files_folder=raw_filings_folder,
        extracted_files_folder=extracted_filings_folder,
        skip_extracted_filings=config["skip_extracted_filings"],
        special_items_config=config.get("special_items", {"enabled": False}),
    )

    LOGGER.info(
        f"Starting FAST extraction from {len(filings_metadata_df)} filings using {num_processes} processes."
    )

    list_of_series = list(zip(*filings_metadata_df.iterrows()))[1]

    # Process filings in parallel using multiple processes
    with ProcessPool(processes=num_processes) as pool:
        processed = list(
            tqdm(
                pool.imap(extraction.process_filing, list_of_series),
                total=len(list_of_series),
                ncols=100,
            )
        )

    LOGGER.info("\nItem extraction is completed successfully.")
    LOGGER.info(f"{sum(processed)} files were processed.")
    LOGGER.info(f"Extracted filings are saved to: {extracted_filings_folder}")


if __name__ == "__main__":
    # Use 2-4 processes for Colab (adjust based on available CPU cores)
    main_fast(num_processes=2)
