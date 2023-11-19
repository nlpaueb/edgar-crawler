import os
import csv
import zipfile

import numpy as np
import pandas as pd
import unittest
from tqdm import tqdm

from extract_items import ExtractItems


def csv_to_list(csv_file):
    """Convert CSV file to a list of rows."""
    with open(csv_file, newline="") as file:
        reader = csv.reader(file)
        return list(reader)


def extract_zip(input_zip):
    zf = zipfile.ZipFile(input_zip)
    zf.extractall(path=os.path.join("/tmp", "edgar_crawler"))


class Test(unittest.TestCase):
    def test_extract_tables(self):
        extract_zip(os.path.join("tests", "fixtures", "RAW_FILINGS_TABLE.zip"))
        extract_zip(os.path.join("tests", "fixtures", "EXTRACTED_TABLES.zip"))

        filings_metadata_df = pd.read_csv(os.path.join("tests", "fixtures", "FILINGS_METADATA_TABLES_TEST.csv"), dtype=str)
        filings_metadata_df = filings_metadata_df.replace({np.nan: None})

        extraction = ExtractItems(
            remove_tables=True,
            items_to_extract=[
                "1",
                "1A",
                "1B",
                "2",
                "3",
                "4",
                "5",
                "6",
                "7",
                "7A",
                "8",
                "9",
                "9A",
                "9B",
                "10",
                "11",
                "12",
                "13",
                "14",
                "15",
            ],
            raw_files_folder="/tmp/edgar_crawler/",
            extracted_files_folder="",
            skip_extracted_filings=True,
        )

        for filing_metadata in tqdm(list(zip(*filings_metadata_df.iterrows()))[1], unit="filings", ncols=100):
            csv_content_pds = extraction.extract_tables(filing_metadata)

            tables_filepath = os.path.join(
                "/tmp/edgar_crawler",
                f"{filing_metadata['filename'].split('.')[0]}.csv"
            )
            with open(tables_filepath, "w") as f:
                for df in csv_content_pds:
                    df.to_csv(f, index=False)
                    f.write("\n")

            expected_filing_filepath = os.path.join(
                "/tmp/edgar_crawler",
                f"{filing_metadata['filename'].split('.')[0]}.csv"
            )
            tables = csv_to_list(tables_filepath)
            expected_tables = csv_to_list(expected_filing_filepath)

            assert tables == expected_tables, f"CSV file is different: {tables} != {expected_tables}"
