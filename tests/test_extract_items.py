import json
import os
import unittest
import zipfile

import numpy as np
import pandas as pd
from tqdm import tqdm

from extract_items import ExtractItems


def extract_zip(input_zip):
    """
    Extracts the contents of a zip file to a specific folder based on its name.

    Args:
        input_zip (str): Path to the zip file to be extracted.

    Raises:
        ValueError: If the input_zip does not contain a recognized folder name.
    """
    if "RAW_FILINGS" in input_zip:
        folder_name = "RAW_FILINGS"
    elif "EXTRACTED_FILINGS" in input_zip:
        folder_name = "EXTRACTED_FILINGS"
    else:
        raise ValueError(f"Unrecognized folder name in `input_zip`: {input_zip}")

    zf = zipfile.ZipFile(input_zip)
    zf.extractall(path=os.path.join("/tmp", "edgar-crawler", folder_name))


class TestExtractItems(unittest.TestCase):
    def test_extract_items_10K(self):
        extract_zip(os.path.join("tests", "fixtures", "RAW_FILINGS", "10-K.zip"))
        extract_zip(os.path.join("tests", "fixtures", "EXTRACTED_FILINGS", "10-K.zip"))

        filings_metadata_df = pd.read_csv(
            os.path.join("tests", "fixtures", "FILINGS_METADATA_TEST.csv"), dtype=str
        )
        filings_metadata_df = filings_metadata_df[filings_metadata_df["Type"] == "10-K"]
        filings_metadata_df = filings_metadata_df.replace({np.nan: None})

        extraction = ExtractItems(
            remove_tables=True,
            items_to_extract=[
                "1",
                "1A",
                "1B",
                "1C",
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
                "9C",
                "10",
                "11",
                "12",
                "13",
                "14",
                "15",
                "16",  # "SIGNATURE"
            ],
            include_signature=False,
            raw_files_folder="/tmp/edgar-crawler/RAW_FILINGS/",  # don't want 10-K here because this is added in extract_items.py
            extracted_files_folder="",
            skip_extracted_filings=True,
        )

        failed_items = {}
        for filing_metadata in tqdm(
            list(zip(*filings_metadata_df.iterrows()))[1], unit="filings", ncols=100
        ):
            extraction.determine_items_to_extract(filing_metadata)
            extracted_filing = extraction.extract_items(filing_metadata)

            expected_filing_filepath = os.path.join(
                "/tmp/edgar-crawler/EXTRACTED_FILINGS/10-K",
                f"{filing_metadata['filename'].split('.')[0]}.json",
            )
            with open(expected_filing_filepath) as f:
                expected_filing = json.load(f)

            # instead of checking only the whole extracted filing, we should also check each item
            # and indicate how many items were extracted correctly
            item_correct_dict = {}
            for item in extraction.items_to_extract:
                if item == "SIGNATURE":
                    current_item = "SIGNATURE"
                else:
                    current_item = f"item_{item}"
                if current_item not in expected_filing:
                    expected_filing[current_item] = ""
                item_correct_dict[current_item] = (
                    extracted_filing[current_item] == expected_filing[current_item]
                )

            try:
                self.assertEqual(extracted_filing, expected_filing)
            except Exception:
                # If the test fails, check which items were not extracted correctly
                failed_items[filing_metadata["filename"]] = [
                    item for item in item_correct_dict if not item_correct_dict[item]
                ]
        if failed_items:
            # Create a failure report with the failed items
            failure_report = "\n".join(
                f"{filename}: {items}" for filename, items in failed_items.items()
            )
            self.fail(f"Extraction failed for the following items:\n{failure_report}")

    def test_extract_items_10Q(self):
        extract_zip(os.path.join("tests", "fixtures", "RAW_FILINGS", "10-Q.zip"))
        extract_zip(os.path.join("tests", "fixtures", "EXTRACTED_FILINGS", "10-Q.zip"))

        filings_metadata_df = pd.read_csv(
            os.path.join("tests", "fixtures", "FILINGS_METADATA_TEST.csv"), dtype=str
        )
        filings_metadata_df = filings_metadata_df[filings_metadata_df["Type"] == "10-Q"]
        filings_metadata_df = filings_metadata_df.replace({np.nan: None})

        extraction = ExtractItems(
            remove_tables=False,
            items_to_extract=[
                "part_1__1",
                "part_1__2",
                "part_1__3",
                "part_1__4",
                "part_2__1",
                "part_2__1A",
                "part_2__2",
                "part_2__3",
                "part_2__4",
                "part_2__5",
                "part_2__6",  # "SIGNATURE",
            ],
            include_signature=False,
            raw_files_folder="/tmp/edgar-crawler/RAW_FILINGS/",  # don't want 10-Q here because this is added in extract_items.py
            extracted_files_folder="",
            skip_extracted_filings=True,
        )

        failed_items = {}
        for filing_metadata in tqdm(
            list(zip(*filings_metadata_df.iterrows()))[1], unit="filings", ncols=100
        ):
            extraction.determine_items_to_extract(filing_metadata)
            extracted_filing = extraction.extract_items(filing_metadata)

            expected_filing_filepath = os.path.join(
                "/tmp/edgar-crawler/EXTRACTED_FILINGS/10-Q",
                f"{filing_metadata['filename'].split('.')[0]}.json",
            )
            with open(expected_filing_filepath) as f:
                expected_filing = json.load(f)

            # instead of checking only the whole extracted filing, we should also check each item
            # and indicate how many items were extracted correctly
            item_correct_dict = {}
            for item in extraction.items_to_extract:
                if item == "SIGNATURE":
                    current_item = "SIGNATURE"
                else:
                    # special naming convention for 10-Qs
                    current_item = f"{item.split('__')[0]}_item_{item.split('__')[1]}"
                if current_item not in expected_filing:
                    expected_filing[current_item] = ""

                item_correct_dict[current_item] = (
                    extracted_filing[current_item] == expected_filing[current_item]
                )

            # For 10-Q we also extract the full parts in addition to the items - check if they are correct
            item_correct_dict["part_1"] = (
                extracted_filing["part_1"] == expected_filing["part_1"]
            )
            item_correct_dict["part_2"] = (
                extracted_filing["part_2"] == expected_filing["part_2"]
            )

            try:
                self.assertEqual(extracted_filing, expected_filing)
            except Exception:
                # If the test fails, check which items were not extracted correctly
                failed_items[filing_metadata["filename"]] = [
                    item for item in item_correct_dict if not item_correct_dict[item]
                ]
        if failed_items:
            # Create a failure report with the failed items
            failure_report = "\n".join(
                f"{filename}: {items}" for filename, items in failed_items.items()
            )
            self.fail(f"Extraction failed for the following items:\n{failure_report}")

    def test_extract_items_8K(self):
        extract_zip(os.path.join("tests", "fixtures", "RAW_FILINGS", "8-K.zip"))
        extract_zip(os.path.join("tests", "fixtures", "EXTRACTED_FILINGS", "8-K.zip"))

        filings_metadata_df = pd.read_csv(
            os.path.join("tests", "fixtures", "FILINGS_METADATA_TEST.csv"), dtype=str
        )
        filings_metadata_df = filings_metadata_df[filings_metadata_df["Type"] == "8-K"]
        filings_metadata_df = filings_metadata_df.replace({np.nan: None})

        extraction_new = ExtractItems(
            remove_tables=True,
            items_to_extract=[
                "1.01",
                "1.02",
                "1.03",
                "1.04",
                "1.05",
                "2.01",
                "2.02",
                "2.03",
                "2.04",
                "2.05",
                "2.06",
                "3.01",
                "3.02",
                "3.03",
                "4.01",
                "4.02",
                "5.01",
                "5.02",
                "5.03",
                "5.04",
                "5.05",
                "5.06",
                "5.07",
                "5.08",
                "6.01",
                "6.02",
                "6.03",
                "6.04",
                "6.05",
                "7.01",
                "8.01",
                "9.01",  # "SIGNATURE",
            ],
            include_signature=False,
            raw_files_folder="/tmp/edgar-crawler/RAW_FILINGS/",
            extracted_files_folder="",
            skip_extracted_filings=True,
        )

        # The 8-K items were named differently prior to August 23, 2004
        extraction_old = ExtractItems(
            remove_tables=True,
            items_to_extract=[
                "1",
                "2",
                "3",
                "4",
                "5",
                "6",
                "7",
                "8",
                "9",
                "10",
                "11",
                "12",  # "SIGNATURE",
            ],
            include_signature=False,
            raw_files_folder="/tmp/edgar-crawler/RAW_FILINGS/",
            extracted_files_folder="",
            skip_extracted_filings=True,
        )

        failed_items = {}
        for filing_metadata in tqdm(
            list(zip(*filings_metadata_df.iterrows()))[1], unit="filings", ncols=100
        ):
            # Prior to August 23, 2004, the 8-K items were named differently
            obsolete_cutoff_date_8k = pd.to_datetime("2004-08-23")
            if pd.to_datetime(filing_metadata["Date"]) > obsolete_cutoff_date_8k:
                extraction = extraction_new
            else:
                extraction = extraction_old
            extraction.determine_items_to_extract(filing_metadata)
            extracted_filing = extraction.extract_items(filing_metadata)

            expected_filing_filepath = os.path.join(
                "/tmp/edgar-crawler/EXTRACTED_FILINGS/8-K",
                f"{filing_metadata['filename'].split('.')[0]}.json",
            )
            with open(expected_filing_filepath) as f:
                expected_filing = json.load(f)

            # instead of checking only the whole extracted filing, we should also check each item
            # and indicate how many items were extracted correctly
            item_correct_dict = {}
            for item in extraction.items_to_extract:
                if item == "SIGNATURE":
                    current_item = "SIGNATURE"
                else:
                    current_item = f"item_{item}"
                if current_item not in expected_filing:
                    expected_filing[current_item] = ""
                item_correct_dict[current_item] = (
                    extracted_filing[current_item] == expected_filing[current_item]
                )

            try:
                self.assertEqual(extracted_filing, expected_filing)
            except Exception:
                # If the test fails, check which items were not extracted correctly
                failed_items[filing_metadata["filename"]] = [
                    item for item in item_correct_dict if not item_correct_dict[item]
                ]
        if failed_items:
            # Create a failure report with the failed items
            failure_report = "\n".join(
                f"{filename}: {items}" for filename, items in failed_items.items()
            )
            self.fail(f"Extraction failed for the following items:\n{failure_report}")


    def test_special_items_extraction(self):
        """Test special items extraction functionality with synthetic data."""

        # Test monetary amount extraction
        test_text = """
        The company recorded restructuring charges of $125.3 million in 2023.
        This includes severance costs of ($23.5 million) and facility closure costs.
        See Note 12 for additional details. Asset impairment charges totaled 450 million.
        Acquisition-related transaction costs were $75 million. Gain on sale of assets was $30 million.
        """

        amounts = ExtractItems.extract_monetary_amounts(test_text)
        self.assertGreater(len(amounts), 0, "Should extract at least one monetary amount")

        # Check that we found the $125.3 million
        found_125 = any(abs(amt['value'] - 125.3) < 0.1 and 'million' in amt['scale'] for amt in amounts)
        self.assertTrue(found_125, "Should extract $125.3 million")

        # Check negative amount (parenthetical)
        found_negative = any(amt['value'] < 0 for amt in amounts)
        self.assertTrue(found_negative, "Should extract negative amounts in parentheses")

        # Test footnote reference extraction
        footnotes = ExtractItems.extract_footnote_references(test_text)
        self.assertGreater(len(footnotes), 0, "Should extract at least one footnote reference")

        # Check that we found Note 12
        found_note = any('12' in ref['note_id'] for ref in footnotes)
        self.assertTrue(found_note, "Should extract Note 12 reference")

        # Test full special items extraction with config
        special_items_config = {
            'enabled': True,
            'scan_item_7_mda': False,
            'confidence_threshold': 0.3,
            'debug_logging': False,
            'keywords': {
                'restructuring': ['restructuring', 'severance', 'facility closure'],
                'impairment': ['impairment', 'asset impairment'],
                'acquisition': ['acquisition', 'transaction costs', 'M&A'],
                'asset_sale': ['gain on sale', 'loss on sale', 'asset disposal'],
            }
        }

        # Create a mock filing with special items
        mock_filing_html = """
        <html><body>
        ITEM 8. Financial Statements
        <table>
        <tr><td>Restructuring charges</td><td>$125.3 million</td></tr>
        </table>
        The company recorded restructuring charges of $125.3 million related to
        workforce reduction and facility closure costs. See Note 12.

        Asset impairment charges of $450 million were recorded in Q4 2023.

        Acquisition-related transaction costs totaled $75 million for the merger with XYZ Corp.

        The company recorded a gain on sale of manufacturing facility of $30 million.
        </body></html>
        """

        extraction = ExtractItems(
            remove_tables=True,
            items_to_extract=[],
            include_signature=False,
            raw_files_folder="",
            extracted_files_folder="",
            skip_extracted_filings=True,
            special_items_config=special_items_config,
        )

        from bs4 import BeautifulSoup
        doc_report = BeautifulSoup(mock_filing_html, "lxml")
        is_html = True
        filing_metadata = {
            'filename': 'test_filing.htm',
            'Type': '10-K',
            'CIK': '123456',
            'Company': 'Test Company',
        }

        special_items = extraction.extract_special_items(
            doc_report=doc_report,
            is_html=is_html,
            filing_metadata=filing_metadata,
            special_items_config=special_items_config
        )

        # Validate results
        self.assertGreater(len(special_items), 0, "Should extract at least one special item")

        # Check for restructuring item
        restructuring_items = [item for item in special_items if item['type'] == 'restructuring']
        self.assertGreater(len(restructuring_items), 0, "Should find restructuring items")

        # Check for impairment item
        impairment_items = [item for item in special_items if item['type'] == 'impairment']
        self.assertGreater(len(impairment_items), 0, "Should find impairment items")

        # Verify structure of extracted items
        for item in special_items:
            self.assertIn('type', item)
            self.assertIn('confidence', item)
            self.assertIn('keywords_matched', item)
            self.assertIn('context', item)
            self.assertIn('source_section', item)
            self.assertIn('amount_raw', item)
            self.assertIn('footnote_reference', item)

            # Confidence should be a number between 0 and 1
            self.assertGreaterEqual(item['confidence'], 0.0)
            self.assertLessEqual(item['confidence'], 1.0)

        print(f"\nExtracted {len(special_items)} special items:")
        for item in special_items:
            print(f"  - {item['type']}: {item.get('amount_raw', 'N/A')} "
                  f"(confidence: {item['confidence']:.2f})")


if __name__ == "__main__":
    test = TestExtractItems()
    test.test_extract_items_10K()
    test.test_extract_items_10Q()
    test.test_extract_items_8K()
    test.test_special_items_extraction()
