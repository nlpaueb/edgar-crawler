import os
import json
import tempfile
import zipfile

import numpy as np
import pandas as pd
from tqdm import tqdm
import unittest

from extract_items import ExtractItems


def extract_zip(input_zip):
    zf = zipfile.ZipFile(input_zip)
    zf.extractall(path=os.path.join("/tmp", "edgar_crawler"))


class Test(unittest.TestCase):
    def test_extract_items(self):
        extract_zip(os.path.join("tests", "fixtures", "RAW_FILINGS.zip"))
        extract_zip(os.path.join("tests", "fixtures", "EXTRACTED_FILINGS.zip"))

        filings_metadata_df = pd.read_csv(os.path.join("tests", "fixtures", "FILINGS_METADATA_TEST.csv"), dtype=str)
        filings_metadata_df = filings_metadata_df.replace({np.nan: None})

        extraction = ExtractItems(
            remove_tables=True,
            items_to_extract=[
                "1", "1A", "1B", "2", "3", "4", "5", "6", "7", "7A",
                "8", "9", "9A", "9B", "10", "11", "12", "13", "14", "15"
            ],
            raw_files_folder="/tmp/edgar_crawler/RAW_FILINGS",
            extracted_files_folder="",
            skip_extracted_filings=True,
        )

        for filing_metadata in tqdm(list(zip(*filings_metadata_df.iterrows()))[1], unit="filings", ncols=100):
            extracted_filing = extraction.extract_items(filing_metadata)

            expected_filing_filepath = os.path.join(
                "/tmp/edgar_crawler/EXTRACTED_FILINGS",
                f"{filing_metadata['filename'].split('.')[0]}.json"
            )
            with open(expected_filing_filepath) as f:
                expected_filing = json.load(f)

            assert extracted_filing == expected_filing
