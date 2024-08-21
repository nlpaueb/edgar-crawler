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
    if "RAW_FILINGS" in input_zip:
        folder_name = "RAW_FILINGS"
    elif "EXTRACTED_FILINGS" in input_zip:
        folder_name = "EXTRACTED_FILINGS"
    zf = zipfile.ZipFile(input_zip)
    zf.extractall(path=os.path.join("/tmp", "edgar_crawler", folder_name))


class Test(unittest.TestCase):
    def test_extract_items_10K(self):
        extract_zip(os.path.join("tests", "fixtures", "RAW_FILINGS", "10-K.zip"))
        extract_zip(os.path.join("tests", "fixtures", "EXTRACTED_FILINGS", "10-K.zip"))

        filings_metadata_df = pd.read_csv(os.path.join("tests", "fixtures", "FILINGS_METADATA_TEST.csv"), dtype=str)
        filings_metadata_df = filings_metadata_df[filings_metadata_df['Type'] == '10-K']
        filings_metadata_df = filings_metadata_df.replace({np.nan: None})

        extraction = ExtractItems(
            remove_tables=True,
            items_to_extract=[
                "1", "1A", "1B", "2", "3", "4", "5", "6", "7", "7A",
                "8", "9", "9A", "9B", "9C", "10", "11", "12", "13",
                "14", "15", "16", #"SIGNATURE"
            ],
            include_signature=False,
            raw_files_folder="/tmp/edgar_crawler/RAW_FILINGS/", #don't want 10-K here because this is added in extract_items.py
            extracted_files_folder="",
            skip_extracted_filings=True,
        )

        failed_items = {}
        for filing_metadata in tqdm(list(zip(*filings_metadata_df.iterrows()))[1], unit="filings", ncols=100):
            extraction.determine_items_to_extract(filing_metadata)
            extracted_filing = extraction.extract_items(filing_metadata)

            expected_filing_filepath = os.path.join(
                "/tmp/edgar_crawler/EXTRACTED_FILINGS/10-K",
                f"{filing_metadata['filename'].split('.')[0]}.json"
            )
            with open(expected_filing_filepath) as f:
                expected_filing = json.load(f)
            
            #instead of checking only the whole extracted filing, we should also check each item
            #and indicate how many items were extracted correctly
            item_correct_dict = {}
            for item in extraction.items_to_extract:
                if item == 'SIGNATURE':
                    current_item = 'SIGNATURE'
                else:
                    current_item = f'item_{item}'
                item_correct_dict[current_item] = extracted_filing[current_item] == expected_filing[current_item]

            try:
                assert extracted_filing == expected_filing
            except Exception as e:
                #If the test fails, check which items were not extracted correctly
                failed_items[filing_metadata["filename"]] = [item for item in item_correct_dict if not item_correct_dict[item]]
                exception = e
        if failed_items:
            for i in failed_items:
                #failed_items[i] can be empty if the extracted filing is exactly the same as the test filing but has more items (e.g. Item_16 might be shown as empty)
                print(f'{i}: {failed_items[i]}')
            raise exception
    
    def test_extract_items_8K(self):
        extract_zip(os.path.join("tests", "fixtures", "RAW_FILINGS", "8-K.zip"))
        extract_zip(os.path.join("tests", "fixtures", "EXTRACTED_FILINGS", "8-K.zip"))

        filings_metadata_df = pd.read_csv(os.path.join("tests", "fixtures", "FILINGS_METADATA_TEST.csv"), dtype=str)
        filings_metadata_df = filings_metadata_df[filings_metadata_df['Type'] == '8-K']
        filings_metadata_df = filings_metadata_df.replace({np.nan: None})

        extraction_new = ExtractItems(
            remove_tables=True,
            items_to_extract=[
                "1.01", "1.02", "1.03", "1.04", "2.01", "2.02", "2.03", "2.04", "2.05", "2.06",
                "3.01", "3.02", "3.03", "4.01", "4.02", "5.01", "5.02", "5.03", "5.04", "5.05",
                "5.06", "5.07", "5.08", "6.01", "6.02", "6.03", "6.04", "6.05", "7.01", "8.01",
                "9.01", #"SIGNATURE",
                ],
            include_signature=False,
            raw_files_folder="/tmp/edgar_crawler/RAW_FILINGS/",
            extracted_files_folder="",
            skip_extracted_filings=True,
        )

        # The 8-K items were named differently prior to August 23, 2004
        extraction_old = ExtractItems(
            remove_tables=True,
            items_to_extract=[
                "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", #"SIGNATURE",
                ],
            include_signature=False,
            raw_files_folder="/tmp/edgar_crawler/RAW_FILINGS/",
            extracted_files_folder="",
            skip_extracted_filings=True,
        )

        failed_items = {}
        for filing_metadata in tqdm(list(zip(*filings_metadata_df.iterrows()))[1], unit="filings", ncols=100):
            # Prior to August 23, 2004, the 8-K items were named differently
            obsolete_cutoff_date_8k = pd.to_datetime("2004-08-23")
            if pd.to_datetime(filing_metadata["Date"]) > obsolete_cutoff_date_8k:
                extraction = extraction_new
            else:
                extraction = extraction_old
            extraction.determine_items_to_extract(filing_metadata)
            extracted_filing = extraction.extract_items(filing_metadata)

            expected_filing_filepath = os.path.join(
                "/tmp/edgar_crawler/EXTRACTED_FILINGS/8-K",
                f"{filing_metadata['filename'].split('.')[0]}.json"
            )
            with open(expected_filing_filepath) as f:
                expected_filing = json.load(f)
            
            #instead of checking only the whole extracted filing, we should also check each item
            #and indicate how many items were extracted correctly
            item_correct_dict = {}
            for item in extraction.items_to_extract:
                if item == 'SIGNATURE':
                    current_item = 'SIGNATURE'
                else:
                    current_item = f'item_{item}'
                item_correct_dict[current_item] = extracted_filing[current_item] == expected_filing[current_item]

            try:
                assert extracted_filing == expected_filing
            except Exception as e:
                #If the test fails, check which items were not extracted correctly
                failed_items[filing_metadata["filename"]] = [item for item in item_correct_dict if not item_correct_dict[item]]
                exception = e
        if failed_items:
            for i in failed_items:
                #failed_items[i] can be empty if the extracted filing is exactly the same as the test filing but has more items (e.g. Item_16 might be shown as empty)
                print(f'{i}: {failed_items[i]}')
            raise exception

if __name__ == "__main__":
    test = Test()
    test.test_extract_items_10K()
    test.test_extract_items_8K()
