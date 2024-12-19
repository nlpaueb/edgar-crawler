import json
import logging
import os
import re
import sys
from html.parser import HTMLParser
from typing import Any, Dict, List, Optional, Tuple
import xml.etree.ElementTree as ET

import click
import cssutils
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from pathos.pools import ProcessPool
from tqdm import tqdm

from __init__ import DATASET_DIR
from item_lists import (
    item_list_8k,
    item_list_8k_obsolete,
    item_list_10k,
    item_list_10q,
    item_list_form3,
    item_list_form4,
    item_list_sc13d,
    item_list_sc13g,
)
from logger import Logger

# Change the default recursion limit
sys.setrecursionlimit(30000)

# Suppress cssutils warnings
cssutils.log.setLevel(logging.CRITICAL)

cli = click.Group()

regex_flags = re.IGNORECASE | re.DOTALL | re.MULTILINE

# Initialize logger
LOGGER = Logger(name="ExtractItems").get_logger()

class HtmlStripper(HTMLParser):
    """Strips HTML tags from text."""
    
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.fed = []

    def handle_data(self, data: str) -> None:
        self.fed.append(data)

    def get_data(self) -> str:
        return ''.join(self.fed)

    def strip_tags(self, html: str) -> str:
        self.feed(html)
        return self.get_data()

class ExtractItems:
    """Extracts items from SEC filings."""

    def __init__(
        self,
        remove_tables: bool,
        items_to_extract: List[str],
        include_signature: bool,
        raw_files_folder: str,
        extracted_files_folder: str,
        skip_extracted_filings: bool,
    ) -> None:
        self.remove_tables = remove_tables
        self.items_to_extract = items_to_extract
        self.include_signature = include_signature
        self.raw_files_folder = raw_files_folder
        self.extracted_files_folder = extracted_files_folder
        self.skip_extracted_filings = skip_extracted_filings
        self.items_list = []

    @staticmethod
    def strip_html(html_content: str) -> str:
        """Strip HTML tags from content."""
        html_content = re.sub(r"(<\s*/\s*(div|tr|p|li|)\s*>)", r"\1\n\n", html_content)
        html_content = re.sub(r"(<br\s*>|<br\s*/>)", r"\1\n\n", html_content)
        html_content = re.sub(r"(<\s*/\s*(th|td)\s*>)", r" \1 ", html_content)
        return HtmlStripper().strip_tags(html_content)

    @staticmethod
    def remove_multiple_lines(text: str) -> str:
        """Remove multiple consecutive lines and spaces."""
        text = re.sub(r"(( )*\n( )*){2,}", "#NEWLINE", text)
        text = re.sub(r"\n", " ", text)
        text = re.sub(r"(#NEWLINE)+", "\n", text).strip()
        text = re.sub(r"[ ]{2,}", " ", text)
        return text

    @staticmethod
    def clean_text(text: str) -> str:
        """Clean text by removing unnecessary blocks and normalizing special characters."""
        # Special character replacements
        special_chars = {
            '\xa0': ' ',
            '\u200b': ' ',
            '\x91': ''',
            '\x92': ''',
            '\x93': '"',
            '\x94': '"',
            '\x95': '•',
            '\x96': '-',
            '\x97': '-',
            '\x98': '˜',
            '\x99': '™',
            '\u2010': '-',
            '\u2011': '-',
            '\u2012': '-',
            '\u2013': '-',
            '\u2014': '-',
            '\u2015': '-',
            '\u2018': ''',
            '\u2019': ''',
            '\u2009': ' ',
            '\u00ae': '®',
            '\u201c': '"',
            '\u201d': '"',
        }
        
        for char, replacement in special_chars.items():
            text = text.replace(char, replacement)

        # Fix broken section headers
        def fix_section(match):
            ws = r"[^\S\r\n]"
            return f'{match[1]}{re.sub(ws, r"", match[2])}{match[3]}{match[4]}'

        text = re.sub(
            r"(\n[^\S\r\n]*)(P[^\S\r\n]*A[^\S\r\n]*R[^\S\r\n]*T)([^\S\r\n]+)((\d{1,2}|[IV]{1,2})[AB]?)",
            fix_section,
            text,
            flags=re.IGNORECASE,
        )
        
        text = re.sub(
            r"(\n[^\S\r\n]*)(I[^\S\r\n]*T[^\S\r\n]*E[^\S\r\n]*M)([^\S\r\n]+)(\d{1,2}[AB]?)",
            fix_section,
            text,
            flags=re.IGNORECASE,
        )

        # Remove unnecessary headers
        headers_to_remove = [
            r"TABLE\s+OF\s+CONTENTS",
            r"INDEX\s+TO\s+FINANCIAL\s+STATEMENTS",
            r"BACK\s+TO\s+CONTENTS",
            r"QUICKLINKS"
        ]
        
        for header in headers_to_remove:
            text = re.sub(
                rf"\n[^\S\r\n]*{header}[^\S\r\n]*\n",
                "\n",
                text,
                flags=regex_flags
            )

        # Remove page numbers and other formatting
        text = re.sub(r"\n[^\S\r\n]*[-‒–—]*\d+[-‒–—]*[^\S\r\n]*\n", "\n", text, flags=regex_flags)
        text = re.sub(r"\n[^\S\r\n]*\d+[^\S\r\n]*\n", "\n", text, flags=regex_flags)
        text = re.sub(r"[\n\s]F[-‒–—]*\d+", "", text, flags=regex_flags)
        text = re.sub(r"\n[^\S\r\n]*Page\s[\d*]+[^\S\r\n]*\n", "", text, flags=regex_flags)

        return text

    def determine_items_to_extract(self, filing_metadata: Dict[str, Any]) -> None:
        """Determine which items to extract based on filing type."""
        if filing_metadata["Type"] == "10-K":
            items_list = item_list_10k
        elif filing_metadata["Type"] == "8-K":
            obsolete_cutoff_date_8k = pd.to_datetime("2004-08-23")
            if pd.to_datetime(filing_metadata["Date"]) > obsolete_cutoff_date_8k:
                items_list = item_list_8k
            else:
                items_list = item_list_8k_obsolete
        elif filing_metadata["Type"] == "10-Q":
            items_list = item_list_10q
        elif filing_metadata["Type"] == "3":
            items_list = item_list_form3
        elif filing_metadata["Type"] == "4":
            items_list = item_list_form4
        elif filing_metadata["Type"] in ["SC13D", "SC13D/A"]:
            items_list = item_list_sc13d
        elif filing_metadata["Type"] in ["SC13G", "SC13G/A"]:
            items_list = item_list_sc13g
        else:
            raise Exception(f"Unsupported filing type: {filing_metadata['Type']}")

        self.items_list = items_list

        if self.items_to_extract:
            overlapping_items = [item for item in self.items_to_extract if item in items_list]
            if overlapping_items:
                self.items_to_extract = overlapping_items
            else:
                raise Exception(f"Items defined by user don't match items for {filing_metadata['Type']}")
        else:
            self.items_to_extract = items_list

    def _extract_xml_data(self, text: str, filing_type: str) -> Dict[str, Any]:
        """Extract data from XML section."""
        try:
            xml_match = re.search(r'<XML>\s*(.+?)\s*</XML>', text, re.DOTALL)
            if not xml_match:
                return {}
            
            # Clean up XML content
            xml_content = xml_match.group(1).strip()
            if '<?xml' in xml_content:
                xml_content = xml_content[xml_content.find('<?xml'):]
            
            root = ET.fromstring(xml_content)
            
            if filing_type == "3":
                return self._extract_form3_data(root)
            elif filing_type == "4":
                return self._extract_form4_data(root)
            elif filing_type in ["SC13D", "SC13D/A", "SC13G", "SC13G/A"]:
                return self._extract_schedule13_data(root)
                
        except ET.ParseError as e:
            LOGGER.error(f"XML parsing error: {str(e)}")
        except Exception as e:
            LOGGER.error(f"Error processing XML: {str(e)}")
            
        return {}

    def _extract_form3_data(self, root: ET.Element) -> Dict[str, Any]:
        """Extract Form 3 specific data."""
        data = {
            'derivative_securities': [],
            'non_derivative_securities': [],
            'footnotes': {},
            'remarks': None
        }

        # Extract derivative securities
        derivative_table = root.find('.//derivativeTable')
        if derivative_table is not None:
            for holding in derivative_table.findall('.//derivativeHolding'):
                security = {
                    'security_title': holding.findtext('.//securityTitle/value', '').strip(),
                    'conversion_price': holding.findtext('.//conversionOrExercisePrice/value', '').strip(),
                    'exercise_date': holding.findtext('.//exerciseDate/value', '').strip(),
                    'expiration_date': holding.findtext('.//expirationDate/value', '').strip(),
                    'underlying_security': {
                        'title': holding.findtext('.//underlyingSecurityTitle/value', '').strip(),
                        'shares': holding.findtext('.//underlyingSecurityShares/value', '').strip(),
                    },
                    'ownership_nature': holding.findtext('.//directOrIndirectOwnership/value', '').strip()
                }
                data['derivative_securities'].append(security)

        # Extract non-derivative securities
        non_derivative_table = root.find('.//nonDerivativeTable')
        if non_derivative_table is not None:
            for holding in non_derivative_table.findall('.//nonDerivativeHolding'):
                security = {
                    'security_title': holding.findtext('.//securityTitle/value', '').strip(),
                    'shares_owned': holding.findtext('.//sharesOwnedFollowingTransaction/value', '').strip(),
                    'ownership_nature': holding.findtext('.//directOrIndirectOwnership/value', '').strip()
                }
                data['non_derivative_securities'].append(security)

        # Extract footnotes
        footnotes_elem = root.find('.//footnotes')
        if footnotes_elem is not None:
            for footnote in footnotes_elem.findall('.//footnote'):
                footnote_id = footnote.get('id')
                if footnote_id and footnote.text:
                    data['footnotes'][footnote_id] = footnote.text.strip()

        # Extract remarks
        remarks = root.findtext('.//remarks', '').strip()
        if remarks:
            data['remarks'] = remarks

        # Extract signature if requested
        if self.include_signature:
            signature = root.find('.//ownerSignature')
            if signature is not None:
                data['signature'] = {
                    'name': signature.findtext('.//signatureName', '').strip(),
                    'date': signature.findtext('.//signatureDate', '').strip()
                }

        return data

    def _extract_form4_data(self, root: ET.Element) -> Dict[str, Any]:
        """Extract Form 4 specific data."""
        # Similar structure to Form 3 but with transaction-specific fields
        data = {
            'derivative_transactions': [],
            'non_derivative_transactions': [],
            'footnotes': {},
            'remarks': None
        }
        # Add Form 4 specific extraction logic here
        return data

    def _extract_schedule13_data(self, root: ET.Element) -> Dict[str, Any]:
        """Extract Schedule 13D/G specific data."""
        data = {
            'reporting_owners': [],
            'subject_company': {},
            'footnotes': {},
            'remarks': None
        }
        # Add Schedule 13D/G specific extraction logic here
        return data



    def extract_items(self, filing_metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            """Extract items from a filing."""
            try:
                absolute_filename = os.path.join(
                    self.raw_files_folder, filing_metadata["Type"], filing_metadata["filename"]
                )

                # Read file content
                with open(absolute_filename, "r", errors="backslashreplace") as file:
                    content = file.read()

                # Initialize JSON content
                json_content = {
                    "cik": filing_metadata["CIK"],
                    "company": filing_metadata["Company"],
                    "filing_type": filing_metadata["Type"],
                    "filing_date": filing_metadata["Date"],
                    "period_of_report": filing_metadata["Period of Report"],
                    "sic": filing_metadata["SIC"],
                    "state_of_inc": filing_metadata["State of Inc"],
                    "state_location": filing_metadata["State location"],
                    "fiscal_year_end": filing_metadata["Fiscal Year End"],
                    "filing_html_index": filing_metadata["html_index"],
                    "htm_filing_link": filing_metadata["htm_file_link"],
                    "complete_text_filing_link": filing_metadata["complete_text_file_link"],
                    "filename": filing_metadata["filename"],
                }

                # Remove embedded PDFs
                content = re.sub(r"<PDF>.*?</PDF>", "", content, flags=regex_flags)

                # Find document section
                documents = re.findall("<DOCUMENT>.*?</DOCUMENT>", content, flags=regex_flags)

                # Initialize variables
                doc_report = None
                found = False
                is_html = False

                # Process based on filing type
                if filing_metadata["Type"] in ["3", "4", "SC13D", "SC13D/A", "SC13G", "SC13G/A"]:
                    xml_data = self._extract_xml_data(content, filing_metadata["Type"])
                    if xml_data:
                        json_content.update(xml_data)
                    else:
                        LOGGER.warning(f"No XML data extracted for {filing_metadata['filename']}")
                    return json_content

                # For non-ownership filings
                for doc in documents:
                    doc_type = re.search(r"\n[^\S\r\n]*<TYPE>(.*?)\n", doc, flags=regex_flags)
                    doc_type = doc_type.group(1) if doc_type else None

                    if doc_type and doc_type.startswith(("10", "8")):
                        doc_report = BeautifulSoup(doc, "lxml")
                        is_html = bool(doc_report.find("td")) and bool(doc_report.find("tr"))
                        if not is_html:
                            doc_report = doc
                        found = True
                        break

                if not found:
                    if documents:
                        LOGGER.info(f'Could not find documents for {filing_metadata["filename"]}')
                    doc_report = BeautifulSoup(content, "lxml")
                    is_html = bool(doc_report.find("td")) and bool(doc_report.find("tr"))
                    if not is_html:
                        doc_report = content

                if self.remove_tables:
                    doc_report = self.remove_html_tables(doc_report, is_html=is_html)

                text = self.strip_html(str(doc_report))
                text = self.clean_text(text)

                positions = []
                all_items_null = True

                for i, item_index in enumerate(self.items_list):
                    next_item_list = self.items_list[i + 1:]
                    item_section, positions = self.parse_item(text, item_index, next_item_list, positions)
                    
                    if item_section:
                        all_items_null = False
                        if item_index == "SIGNATURE" and self.include_signature:
                            json_content["signature"] = self.remove_multiple_lines(item_section.strip())
                        else:
                            json_content[f"item_{item_index}"] = self.remove_multiple_lines(item_section.strip())

                if all_items_null:
                    LOGGER.info(f"\nCould not extract any item for {absolute_filename}")
                    return None

                return json_content

            except Exception as e:
                LOGGER.error(f"Error processing {filing_metadata['filename']}: {str(e)}")
                return None

    def process_filing(self, filing_metadata: Dict[str, Any]) -> int:
        """Process a single filing."""
        try:
            json_filename = f'{filing_metadata["filename"].split(".")[0]}.json'
            
            self.determine_items_to_extract(filing_metadata)
            
            absolute_json_filename = os.path.join(
                self.extracted_files_folder, 
                filing_metadata["Type"], 
                json_filename
            )

            if self.skip_extracted_filings and os.path.exists(absolute_json_filename):
                return 0

            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(absolute_json_filename), exist_ok=True)

            json_content = self.extract_items(filing_metadata)
            if json_content is not None:
                with open(absolute_json_filename, "w", encoding="utf-8") as filepath:
                    json.dump(json_content, filepath, indent=4, ensure_ascii=False)
                return 1

            return 0
            
        except Exception as e:
            LOGGER.error(f"Error in process_filing for {filing_metadata['filename']}: {str(e)}")
            return 0




def main() -> None:
    """Main function to run the extraction process."""
    with open("config.json") as fin:
        config = json.load(fin)["extract_items"]

    filings_metadata_filepath = os.path.join(DATASET_DIR, config["filings_metadata_file"])

    if not os.path.exists(filings_metadata_filepath):
        LOGGER.info(f'No such file "{filings_metadata_filepath}"')
        return

    filings_metadata_df = pd.read_csv(filings_metadata_filepath, dtype=str)
    filings_metadata_df = filings_metadata_df.replace({np.nan: None})

    if config["filing_types"]:
        filings_metadata_df = filings_metadata_df[
            filings_metadata_df["Type"].isin(config["filing_types"])
        ]

    if len(filings_metadata_df) == 0:
        LOGGER.info(f"No filings to process for filing types {config['filing_types']}.")
        return

    raw_filings_folder = os.path.join(DATASET_DIR, config["raw_filings_folder"])
    extracted_filings_folder = os.path.join(DATASET_DIR, config["extracted_filings_folder"])

    if not os.path.isdir(raw_filings_folder):
        LOGGER.info(f'No such directory: "{raw_filings_folder}"')
        return

    if not os.path.isdir(extracted_filings_folder):
        os.mkdir(extracted_filings_folder)

    extraction = ExtractItems(
        remove_tables=config["remove_tables"],
        items_to_extract=config["items_to_extract"],
        include_signature=config["include_signature"],
        raw_files_folder=raw_filings_folder,
        extracted_files_folder=extracted_filings_folder,
        skip_extracted_filings=config["skip_extracted_filings"],
    )

    LOGGER.info(f"Starting the JSON extraction from {len(filings_metadata_df)} unstructured EDGAR filings.")

    list_of_series = list(zip(*filings_metadata_df.iterrows()))[1]

    # Process filings in parallel using a process pool
    with ProcessPool(processes=config.get("num_processes", 1)) as pool:
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
    main()