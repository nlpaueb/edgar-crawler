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

# Change the default recursion limit of 1000 to 30000
sys.setrecursionlimit(30000)

# Suppress cssutils warnings
cssutils.log.setLevel(logging.CRITICAL)

cli = click.Group()

regex_flags = re.IGNORECASE | re.DOTALL | re.MULTILINE

# This map is needed for 10-Q reports. Until now they only have parts 1 and 2
roman_numeral_map = {
    "1": "I",
    "2": "II",
    "3": "III",
    "4": "IV",
    "5": "V",
    "6": "VI",
    "7": "VII",
    "8": "VIII",
    "9": "IX",
    "10": "X",
    "11": "XI",
    "12": "XII",
    "13": "XIII",
    "14": "XIV",
    "15": "XV",
    "16": "XVI",
    "17": "XVII",
    "18": "XVIII",
    "19": "XIX",
    "20": "XX",
}

# Instantiate a logger object
LOGGER = Logger(name="ExtractItems").get_logger()


class HtmlStripper(HTMLParser):
    """
    Class to strip HTML tags from a string.

    The class inherits from the HTMLParser class, and overrides some of its methods
    to facilitate the removal of HTML tags. It also uses the feed method of the parent class
    to parse the HTML.

    Attributes:
            strict (bool): Not used, but inherited from parent class.
            convert_charrefs (bool): Whether to convert all character references. By default, it is True.
            fed (list): List to hold the data during parsing.
    """

    def __init__(self):
        """
        Initializes HtmlStripper by calling the constructor of the parent class, resetting the parser,
        and initializing some attributes.
        """
        super().__init__()
        self.reset()
        self.strict = False  # Not used, but necessary for inheritance
        self.convert_charrefs = True  # Convert all character references
        self.fed = []  # List to hold the data

    def handle_data(self, data: str) -> None:
        """
        Append the raw data to the list.

        This method is called whenever raw data is encountered. In the context of
        this class, we just append the data to the fed list.

        Args:
            data (str): The data encountered.
        """
        self.fed.append(data)

    def get_data(self) -> str:
        """
        Join the list to get the data without HTML tags.

        Returns:
            str: The data as a single string.
        """
        return "".join(self.fed)

    def strip_tags(self, html: str) -> str:
        """
        Strip the HTML tags from the string.

        This method feeds the HTML to the parser and returns the data without
        HTML tags.

        Args:
            html (str): The HTML string.

        Returns:
            str: The string without HTML tags.
        """
        self.feed(html)
        return self.get_data()
    
class ExtractItems:
    """
    A class used to extract certain items from SEC filings.

    Attributes:
        remove_tables (bool): Flag to indicate if tables need to be removed.
        items_list (List[str]): List of all items that could be extracted.
        items_to_extract (List[str]): List of items to be extracted.
        raw_files_folder (str): Path of the directory containing raw files.
        extracted_files_folder (str): Path of the directory to save the extracted files.
        skip_extracted_filings (bool): Flag to indicate if already extracted filings should be skipped.
    """

    def __init__(
        self,
        remove_tables: bool,
        items_to_extract: List[str],
        include_signature: bool,
        raw_files_folder: str,
        extracted_files_folder: str,
        skip_extracted_filings: bool,
    ) -> None:
        """
        Constructs all necessary attributes for the ExtractItems object.

        Args:
            remove_tables (bool): Whether to remove tables.
            items_to_extract (List[str]): Items to be extracted. If None, all items are extracted.
            include_signature (bool): Whether to include signature section.
            raw_files_folder (str): Path of folder containing raw files.
            extracted_files_folder (str): Path of folder where extracted files should be saved.
            skip_extracted_filings (bool): Whether to skip already extracted filings.
        """
        self.remove_tables = remove_tables
        self.items_to_extract = items_to_extract
        self.include_signature = include_signature
        self.raw_files_folder = raw_files_folder
        self.extracted_files_folder = extracted_files_folder
        self.skip_extracted_filings = skip_extracted_filings
        self.items_list = []

    def determine_items_to_extract(self, filing_metadata: Dict[str, Any]) -> None:
        """
        Determine which items to extract based on filing type.

        Args:
            filing_metadata: Dictionary containing filing metadata.
        """
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

    @staticmethod
    def strip_html(html_content: str) -> str:
        """
        Strip HTML tags from content.

        Args:
            html_content (str): The HTML content.

        Returns:
            str: Content with HTML tags removed.
        """
        html_content = re.sub(r"(<\s*/\s*(div|tr|p|li|)\s*>)", r"\1\n\n", html_content)
        html_content = re.sub(r"(<br\s*>|<br\s*/>)", r"\1\n\n", html_content)
        html_content = re.sub(r"(<\s*/\s*(th|td)\s*>)", r" \1 ", html_content)
        return HtmlStripper().strip_tags(html_content)

    @staticmethod
    def remove_multiple_lines(text: str) -> str:
        """
        Replace consecutive new lines and spaces with a single new line or space.

        Args:
            text (str): Input text string.

        Returns:
            str: Text with multiple lines/spaces removed.
        """
        text = re.sub(r"(( )*\n( )*){2,}", "#NEWLINE", text)
        text = re.sub(r"\n", " ", text)
        text = re.sub(r"(#NEWLINE)+", "\n", text).strip()
        text = re.sub(r"[ ]{2,}", " ", text)
        return text

    @staticmethod
    def clean_text(text: str) -> str:
        """
        Clean text by removing unnecessary blocks and normalizing special characters.

        Args:
            text (str): The raw text string.

        Returns:
            str: The cleaned text.
        """
        # Special character replacements
        special_chars = {
            '\xa0': ' ', '\u200b': ' ', '\x91': ''', '\x92': ''',
            '\x93': '"', '\x94': '"', '\x95': '•', '\x96': '-',
            '\x97': '-', '\x98': '˜', '\x99': '™', '\u2010': '-',
            '\u2011': '-', '\u2012': '-', '\u2013': '-', '\u2014': '-',
            '\u2015': '-', '\u2018': ''', '\u2019': ''', '\u2009': ' ',
            '\u00ae': '®', '\u201c': '"', '\u201d': '"',
        }
        
        for char, replacement in special_chars.items():
            text = text.replace(char, replacement)

        def remove_whitespace(match):
            ws = r"[^\S\r\n]"
            return f'{match[1]}{re.sub(ws, r"", match[2])}{match[3]}{match[4]}'

        def remove_whitespace_signature(match):
            ws = r"[^\S\r\n]"
            return f'{match[1]}{re.sub(ws, r"", match[2])}{match[4]}{match[5]}'

        # Fix broken section headers
        text = re.sub(
            r"(\n[^\S\r\n]*)(P[^\S\r\n]*A[^\S\r\n]*R[^\S\r\n]*T)([^\S\r\n]+)((\d{1,2}|[IV]{1,2})[AB]?)",
            remove_whitespace,
            text,
            flags=re.IGNORECASE,
        )
        
        text = re.sub(
            r"(\n[^\S\r\n]*)(I[^\S\r\n]*T[^\S\r\n]*E[^\S\r\n]*M)([^\S\r\n]+)(\d{1,2}[AB]?)",
            remove_whitespace,
            text,
            flags=re.IGNORECASE,
        )
        
        text = re.sub(
            r"(\n[^\S\r\n]*)(S[^\S\r\n]*I[^\S\r\n]*G[^\S\r\n]*N[^\S\r\n]*A[^\S\r\n]*T[^\S\r\n]*U[^\S\r\n]*R[^\S\r\n]*E[^\S\r\n]*(S|\([^\S\r\n]*s[^\S\r\n]*\))?)([^\S\r\n]+)([^\S\r\n]?)",
            remove_whitespace_signature,
            text,
            flags=re.IGNORECASE,
        )

        text = re.sub(
            r"(ITEM|PART)(\s+\d{1,2}[AB]?)([\-•])",
            r"\1\2 \3 ",
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
    
    def _extract_xml_data(self, text: str, filing_type: str) -> Dict[str, Any]:
            """
            Extract data from XML section.

            Args:
                text (str): Raw text containing XML
                filing_type (str): Type of filing

            Returns:
                Dict[str, Any]: Extracted XML data
            """
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
            """
            Extract Form 3 specific data.

            Args:
                root (ET.Element): XML root element

            Returns:
                Dict[str, Any]: Extracted Form 3 data
            """
            data = {
                'derivative_securities': [],
                'non_derivative_securities': [],
                'footnotes': {},
                'remarks': None,
                'issuer_info': {},
                'reporting_owner': {},
                'owner_signature': {},
                'document_info': {}
            }

            # Extract document info
            data['document_info'] = {
                'schema_version': root.findtext('.//schemaVersion', '').strip(),
                'document_type': root.findtext('.//documentType', '').strip(),
                'period_of_report': root.findtext('.//periodOfReport', '').strip(),
                'no_securities_owned': root.findtext('.//noSecuritiesOwned', '').strip(),
            }

            # Extract issuer information
            issuer = root.find('.//issuer')
            if issuer is not None:
                data['issuer_info'] = {
                    'cik': issuer.findtext('.//issuerCik', '').strip(),
                    'name': issuer.findtext('.//issuerName', '').strip(),
                    'trading_symbol': issuer.findtext('.//issuerTradingSymbol', '').strip()
                }

            # Extract reporting owner information
            reporting_owner = root.find('.//reportingOwner')
            if reporting_owner is not None:
                data['reporting_owner'] = {
                    'id': {
                        'cik': reporting_owner.findtext('.//rptOwnerCik', '').strip(),
                        'name': reporting_owner.findtext('.//rptOwnerName', '').strip()
                    },
                    'address': {
                        'street1': reporting_owner.findtext('.//rptOwnerStreet1', '').strip(),
                        'street2': reporting_owner.findtext('.//rptOwnerStreet2', '').strip(),
                        'city': reporting_owner.findtext('.//rptOwnerCity', '').strip(),
                        'state': reporting_owner.findtext('.//rptOwnerState', '').strip(),
                        'zip_code': reporting_owner.findtext('.//rptOwnerZipCode', '').strip(),
                        'state_description': reporting_owner.findtext('.//rptOwnerStateDescription', '').strip()
                    },
                    'relationship': {
                        'is_director': reporting_owner.findtext('.//isDirector', '').strip(),
                        'is_officer': reporting_owner.findtext('.//isOfficer', '').strip(),
                        'is_ten_percent_owner': reporting_owner.findtext('.//isTenPercentOwner', '').strip(),
                        'is_other': reporting_owner.findtext('.//isOther', '').strip(),
                        'officer_title': reporting_owner.findtext('.//officerTitle', '').strip()
                    }
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

            # Extract footnotes and remarks
            data['footnotes'] = self._extract_footnotes(root)
            data['remarks'] = root.findtext('.//remarks', '').strip()

            # Extract signature information
            signature = root.find('.//ownerSignature')
            if signature is not None:
                data['owner_signature'] = {
                    'name': signature.findtext('.//signatureName', '').strip(),
                    'date': signature.findtext('.//signatureDate', '').strip()
                }

            return data

    def _extract_form4_data(self, root: ET.Element) -> Dict[str, Any]:
            """Extract Form 4 specific data."""
            data = {
                'derivative_transactions': [],
                'non_derivative_transactions': [],
                'footnotes': {},
                'remarks': None
            }

            # Extract derivative transactions
            derivative_table = root.find('.//derivativeTable')
            if derivative_table is not None:
                for transaction in derivative_table.findall('.//derivativeTransaction'):
                    trans = {
                        'security_title': transaction.findtext('.//securityTitle/value', '').strip(),
                        'conversion_price': transaction.findtext('.//conversionOrExercisePrice/value', '').strip(),
                        'transaction_date': transaction.findtext('.//transactionDate/value', '').strip(),
                        'transaction_coding': {
                            'form_type': transaction.findtext('.//transactionFormType', '').strip(),
                            'code': transaction.findtext('.//transactionCode', '').strip(),
                            'equity_swap_involved': transaction.findtext('.//equitySwapInvolved', '').strip()
                        },
                        'transaction_amounts': {
                            'shares': transaction.findtext('.//transactionShares/value', '').strip(),
                            'price_per_share': transaction.findtext('.//transactionPricePerShare/value', '').strip(),
                            'acquired_disposed_code': transaction.findtext('.//transactionAcquiredDisposedCode/value', '').strip()
                        },
                        'underlying_security': {
                            'title': transaction.findtext('.//underlyingSecurityTitle/value', '').strip(),
                            'shares': transaction.findtext('.//underlyingSecurityShares/value', '').strip()
                        },
                        'post_transaction': {
                            'shares_owned': transaction.findtext('.//sharesOwnedFollowingTransaction/value', '').strip()
                        },
                        'ownership_nature': transaction.findtext('.//directOrIndirectOwnership/value', '').strip(),
                        'exercise_date': transaction.findtext('.//exerciseDate/value', '').strip(),
                        'expiration_date': transaction.findtext('.//expirationDate/value', '').strip()
                    }
                    data['derivative_transactions'].append(trans)

            # Extract non-derivative transactions
            non_derivative_table = root.find('.//nonDerivativeTable')
            if non_derivative_table is not None:
                for transaction in non_derivative_table.findall('.//nonDerivativeTransaction'):
                    trans = {
                        'security_title': transaction.findtext('.//securityTitle/value', '').strip(),
                        'transaction_date': transaction.findtext('.//transactionDate/value', '').strip(),
                        'transaction_coding': {
                            'form_type': transaction.findtext('.//transactionFormType', '').strip(),
                            'code': transaction.findtext('.//transactionCode', '').strip(),
                            'equity_swap_involved': transaction.findtext('.//equitySwapInvolved', '').strip()
                        },
                        'transaction_amounts': {
                            'shares': transaction.findtext('.//transactionShares/value', '').strip(),
                            'price_per_share': transaction.findtext('.//transactionPricePerShare/value', '').strip(),
                            'acquired_disposed_code': transaction.findtext('.//transactionAcquiredDisposedCode/value', '').strip()
                        },
                        'post_transaction': {
                            'shares_owned': transaction.findtext('.//sharesOwnedFollowingTransaction/value', '').strip()
                        },
                        'ownership_nature': transaction.findtext('.//directOrIndirectOwnership/value', '').strip()
                    }
                    data['non_derivative_transactions'].append(trans)

            # Extract footnotes and remarks
            data['footnotes'] = self._extract_footnotes(root)
            data['remarks'] = root.findtext('.//remarks', '').strip()

            return data

    def _extract_schedule13_data(self, root: ET.Element) -> Dict[str, Any]:
        """
        Extract Schedule 13D/G specific data.

        Args:
            root (ET.Element): XML root element

        Returns:
            Dict[str, Any]: Extracted Schedule 13D/G data
        """
        data = {
            'subject_company': {},
            'reporting_owners': [],
            'holdings': {},
            'footnotes': {},
            'remarks': None
        }

        # Extract subject company info
        subject_company = root.find('.//subjectCompany')
        if subject_company is not None:
            data['subject_company'] = {
                'name': subject_company.findtext('.//companyName', '').strip(),
                'cik': subject_company.findtext('.//cik', '').strip(),
                'trading_symbol': subject_company.findtext('.//tradingSymbol', '').strip()
            }

        # Extract reporting owners
        reporting_owners = root.findall('.//reportingOwner')
        for owner in reporting_owners:
            owner_data = {
                'id': owner.findtext('.//reportingOwnerId/rptOwnerCik', '').strip(),
                'name': owner.findtext('.//reportingOwnerId/rptOwnerName', '').strip(),
                'address': {
                    'street1': owner.findtext('.//reportingOwnerAddress/rptOwnerStreet1', '').strip(),
                    'street2': owner.findtext('.//reportingOwnerAddress/rptOwnerStreet2', '').strip(),
                    'city': owner.findtext('.//reportingOwnerAddress/rptOwnerCity', '').strip(),
                    'state': owner.findtext('.//reportingOwnerAddress/rptOwnerState', '').strip(),
                    'zip': owner.findtext('.//reportingOwnerAddress/rptOwnerZipCode', '').strip()
                }
            }
            data['reporting_owners'].append(owner_data)

        # Extract holdings information
        holdings = root.find('.//holdings')
        if holdings is not None:
            data['holdings'] = {
                'shares_held': holdings.findtext('.//sharesHeld', '').strip(),
                'percent_class': holdings.findtext('.//percentClass', '').strip(),
                'investment_discretion': holdings.findtext('.//investmentDiscretion', '').strip()
            }

        # Extract footnotes and remarks
        data['footnotes'] = self._extract_footnotes(root)
        data['remarks'] = root.findtext('.//remarks', '').strip()

        return data

    def _extract_footnotes(self, root: ET.Element) -> Dict[str, str]:
        """
        Extract footnotes from XML.

        Args:
            root (ET.Element): XML root element

        Returns:
            Dict[str, str]: Dictionary of footnote ID to footnote text
        """
        footnotes = {}
        footnotes_elem = root.find('.//footnotes')
        if footnotes_elem is not None:
            for footnote in footnotes_elem.findall('.//footnote'):
                footnote_id = footnote.get('id')
                if footnote_id and footnote.text:
                    footnotes[footnote_id] = footnote.text.strip()
        return footnotes
    
    @staticmethod
    def calculate_table_character_percentages(table_text: str) -> Tuple[float, float]:
        """
        Calculate character type percentages contained in the table text.

        Args:
            table_text (str): The table text

        Returns:
            Tuple[float, float]: Percentage of non-blank digit characters, Percentage of space characters
        """
        digits = sum(c.isdigit() for c in table_text)
        spaces = sum(c.isspace() for c in table_text)

        if len(table_text) - spaces:
            non_blank_digits_percentage = digits / (len(table_text) - spaces)
        else:
            non_blank_digits_percentage = 0

        if len(table_text):
            spaces_percentage = spaces / len(table_text)
        else:
            spaces_percentage = 0

        return non_blank_digits_percentage, spaces_percentage

    def remove_html_tables(self, doc_report: str, is_html: bool) -> str:
        """
        Remove HTML tables that contain numerical data.
        Note that there are many corner-cases in the tables that have text data instead of numerical.

        Args:
            doc_report (str): The report html
            is_html (bool): Whether the document contains html code or just plain text

        Returns:
            str: The report html without numerical tables
        """
        if is_html:
            tables = doc_report.find_all("table")
            # Detect tables that have numerical data
            for tbl in tables:
                tbl_text = ExtractItems.clean_text(ExtractItems.strip_html(str(tbl)))
                item_index_found = False
                for item_index in self.items_list:
                    # Adjust the item index pattern
                    item_index_pattern = self.adjust_item_patterns(item_index)
                    if (
                        len(list(re.finditer(
                            rf"\n[^\S\r\n]*{item_index_pattern}[.*~\-:\s]",
                            tbl_text,
                            flags=regex_flags
                        ))) > 0
                    ):
                        item_index_found = True
                        break
                if item_index_found:
                    continue

                # Find all <tr> elements with style attribute and check for background color
                trs = (
                    tbl.find_all("tr", attrs={"style": True})
                    + tbl.find_all("td", attrs={"style": True})
                    + tbl.find_all("th", attrs={"style": True})
                )

                background_found = False
                for tr in trs:
                    # Parse given cssText which is assumed to be the content of a HTML style attribute
                    style = cssutils.parseStyle(tr["style"])

                    # Check for background color
                    if (
                        style["background"]
                        and style["background"].lower()
                        not in ["none", "transparent", "#ffffff", "#fff", "white"]
                    ) or (
                        style["background-color"]
                        and style["background-color"].lower()
                        not in ["none", "transparent", "#ffffff", "#fff", "white"]
                    ):
                        background_found = True
                        break

                # Find all <tr> elements with bgcolor attribute and check for background color
                trs = (
                    tbl.find_all("tr", attrs={"bgcolor": True})
                    + tbl.find_all("td", attrs={"bgcolor": True})
                    + tbl.find_all("th", attrs={"bgcolor": True})
                )

                bgcolor_found = False
                for tr in trs:
                    if tr["bgcolor"].lower() not in [
                        "none",
                        "transparent",
                        "#ffffff",
                        "#fff",
                        "white",
                    ]:
                        bgcolor_found = True
                        break

                # Remove the table if a background or bgcolor attribute with non-default color is found
                if bgcolor_found or background_found:
                    tbl.decompose()

        else:
            # If the input is plain text, remove the table tags using regex
            doc_report = re.sub(r"<TABLE>.*?</TABLE>", "", str(doc_report), flags=regex_flags)

        return doc_report

    def handle_spans(self, doc: str, is_html: bool) -> str:
        """
        The documents can contain different span types - some are used for formatting, others for margins.
        Sometimes these spans even appear in the middle of words. We need to handle them depending on their type.
        For spans without a margin, we simply remove them. For spans with a margin, we replace them with a space or newline.

        Args:
            doc (str): The document we want to process
            is_html (bool): Whether the document contains html code or just plain text

        Returns:
            str: The document with spans handled depending on span type
        """
        if is_html:
            # Handle spans in the middle of words
            for span in doc.find_all("span"):
                if span.get_text(strip=True):  # If the span contains text
                    span.unwrap()

            # Handle spans with margins
            for span in doc.find_all("span"):
                if "margin-left" in span.attrs.get("style", "") or "margin-right" in span.attrs.get("style", ""):
                    # If the span has a horizontal margin, replace it with a space
                    span.replace_with(" ")
                elif "margin-top" in span.attrs.get("style", "") or "margin-bottom" in span.attrs.get("style", ""):
                    # If the span has a vertical margin, replace it with a newline
                    span.replace_with("\n")

        else:
            # Define regex patterns for horizontal and vertical margins
            horizontal_margin_pattern = re.compile(
                r'<span[^>]*style="[^"]*(margin-left|margin-right):\s*[\d.]+pt[^"]*"[^>]*>.*?</span>',
                re.IGNORECASE,
            )
            vertical_margin_pattern = re.compile(
                r'<span[^>]*style="[^"]*(margin-top|margin-bottom):\s*[\d.]+pt[^"]*"[^>]*>.*?</span>',
                re.IGNORECASE,
            )

            # Replace horizontal margins with a single whitespace
            doc = re.sub(horizontal_margin_pattern, " ", doc)

            # Replace vertical margins with a single newline
            doc = re.sub(vertical_margin_pattern, "\n", doc)

        return doc

    def adjust_item_patterns(self, item_index: str) -> str:
        """
        Adjust the item_pattern for matching in the document text depending on the item index. 
        This is necessary on a case by case basis.

        Args:
            item_index (str): The item index to adjust the pattern for.
                           For 10-Q preprocessing, this can also be part_1 or part_2.

        Returns:
            str: The adjusted item pattern
        """
        # For 10-Q reports, we have two parts of items: part1 and part2
        if "part" in item_index:
            if "__" not in item_index:
                # We are searching for the general part, not a specific item (e.g. PART I)
                item_index_number = item_index.split("_")[1]
                item_index_pattern = rf"PART\s*(?:{roman_numeral_map[item_index_number]}|{item_index_number})"
                return item_index_pattern
            else:
                # We are working with an item, but we just consider the string after the part as the item_index
                item_index = item_index.split("__")[1]

        # Create a regex pattern from the item index
        item_index_pattern = item_index

        # Modify the item index format for matching in the text
        if item_index == "9A":
            item_index_pattern = item_index_pattern.replace("A", r"[^\S\r\n]*A(?:\(T\))?")
        elif item_index == "SIGNATURE":
            # Quit here so the A in SIGNATURE is not changed
            pass
        elif "A" in item_index:
            item_index_pattern = item_index_pattern.replace("A", r"[^\S\r\n]*A")
        elif "B" in item_index:
            item_index_pattern = item_index_pattern.replace("B", r"[^\S\r\n]*B")
        elif "C" in item_index:
            item_index_pattern = item_index_pattern.replace("C", r"[^\S\r\n]*C")

        # If the item is SIGNATURE, we don't want to look for ITEM
        if item_index == "SIGNATURE":
            # Some reports have SIGNATURES or Signature(s) instead of SIGNATURE
            item_index_pattern = rf"{item_index}(s|\(s\))?"
        else:
            if "." in item_index:
                # We need to escape the '.', otherwise it will be treated as a special character - for 8Ks
                item_index = item_index.replace(".", r"\.")
            if item_index in roman_numeral_map:
                # Rarely, reports use roman numerals for the item indexes. 
                # For 8-K, we assume this does not occur (due to their format - e.g. 5.01)
                item_index = f"(?:{roman_numeral_map[item_index]}|{item_index})"
            item_index_pattern = rf"ITEMS?\s*{item_index}"

        return item_index_pattern
    
    def parse_item(
            self,
            text: str,
            item_index: str,
            next_item_list: List[str],
            positions: List[int],
            ignore_matches: int = 0,
        ) -> Tuple[str, List[int]]:
            """
            Parses the specified item/section in a report text.

            Args:
                text (str): The report text.
                item_index (str): Number of the requested Item/Section of the report text.
                next_item_list (List[str]): List of possible next report item sections.
                positions (List[int]): List of the end positions of previous item sections.
                ignore_matches (int): Default is 0. If positive, we skip the first [value] matches.
                                    Only used for 10-Q part extraction.

            Returns:
                Tuple[str, List[int]]: The item/section as a text string and updated end positions.
            """
            regex_flags = re.IGNORECASE | re.DOTALL
            item_index_pattern = self.adjust_item_patterns(item_index)

            if "part" in item_index and "PART" not in item_index_pattern:
                item_index_part_number = item_index.split("__")[0]

            possible_sections_list = []  # possible list of (start, end) matches
            impossible_match = None  # list of matches where no possible section was found
            last_item = True

            for next_item_index in next_item_list:
                last_item = False
                if possible_sections_list:
                    break
                if next_item_index == next_item_list[-1]:
                    last_item = True

                next_item_index_pattern = self.adjust_item_patterns(next_item_index)

                if "part" in next_item_index and "PART" not in next_item_index_pattern:
                    next_item_index_part_number = next_item_index.split("__")[0]
                    if next_item_index_part_number != item_index_part_number:
                        last_item = True
                        break

                matches = list(re.finditer(
                    rf"\n[^\S\r\n]*{item_index_pattern}[.*~\-:\s\(]",
                    text,
                    flags=regex_flags,
                ))

                for i, match in enumerate(matches):
                    if i < ignore_matches:
                        continue
                    offset = match.start()

                    # First do a case-sensitive search
                    possible = list(re.finditer(
                        rf"\n[^\S\r\n]*{item_index_pattern}[.*~\-:\s\()].+?(\n[^\S\r\n]*{str(next_item_index_pattern)}[.*~\-:\s\(])",
                        text[offset:],
                        flags=re.DOTALL,
                    ))

                    if not possible:
                        # If no match, follow with case-insensitive search
                        possible = list(re.finditer(
                            rf"\n[^\S\r\n]*{item_index_pattern}[.*~\-:\s\()].+?(\n[^\S\r\n]*{str(next_item_index_pattern)}[.*~\-:\s\(])",
                            text[offset:],
                            flags=regex_flags,
                        ))

                    if possible:
                        possible_sections_list += [(offset, possible)]
                    elif next_item_index == next_item_list[-1] and not possible_sections_list and match:
                        impossible_match = match

            item_section, positions = self.get_item_section(possible_sections_list, text, positions)

            if positions:
                if item_index in self.items_list and item_section == "":
                    item_section = self.get_last_item_section(item_index, text, positions)
                if item_index == "SIGNATURE":
                    item_section = self.get_last_item_section(item_index, text, positions)
            elif impossible_match or last_item:
                if item_index in self.items_list:
                    item_section = self.get_last_item_section(item_index, text, positions)

            return item_section, positions

    @staticmethod
    def get_item_section(
        possible_sections_list: List[Tuple[int, List[re.Match]]],
        text: str,
        positions: List[int],
    ) -> Tuple[str, List[int]]:
        """
        Get correct section from list of possible sections.

        Args:
            possible_sections_list: List containing all possible sections between Item X and Item Y.
            text: The whole text.
            positions: List of the end positions of previous item sections.

        Returns:
            Tuple[str, List[int]]: The correct section and updated list of end positions.
        """
        item_section: str = ""
        max_match_length: int = 0
        max_match: Optional[re.Match] = None
        max_match_offset: Optional[int] = None

        # Find the match with the largest section
        for offset, matches in possible_sections_list:
            for match in matches:
                match_length = match.end() - match.start()
                if positions:
                    if match_length > max_match_length and offset + match.start() >= positions[-1]:
                        max_match = match
                        max_match_offset = offset
                        max_match_length = match_length
                elif match_length > max_match_length:
                    max_match = match
                    max_match_offset = offset
                    max_match_length = match_length

        if max_match:
            if positions:
                if max_match_offset + max_match.start() >= positions[-1]:
                    item_section = text[
                        max_match_offset + max_match.start() : max_match_offset + max_match.regs[1][0]
                    ]
            else:
                item_section = text[
                    max_match_offset + max_match.start() : max_match_offset + max_match.regs[1][0]
                ]
            positions.append(max_match_offset + max_match.end() - len(max_match[1]) - 1)

        return item_section, positions

    def get_last_item_section(
        self, item_index: str, text: str, positions: List[int]
    ) -> str:
        """
        Returns the text section starting from a given item.

        Args:
            item_index (str): The index of the item/section in the report
            text (str): The whole report text
            positions (List[int]): List of the end positions of previous item sections

        Returns:
            str: All remaining text until the end, starting from specified item_index
        """
        item_index_pattern = self.adjust_item_patterns(item_index)

        # Find all occurrences of the item/section using regex
        item_list = list(re.finditer(
            rf"\n[^\S\r\n]*{item_index_pattern}[.\-:\s].+?",
            text,
            flags=regex_flags,
        ))

        item_section = ""
        for item in item_list:
            if "SIGNATURE" in item_index:
                if item != item_list[-1]:
                    continue
            if positions:
                if item.start() >= positions[-1]:
                    item_section = text[item.start():].strip()
                    break
            else:
                item_section = text[item.start():].strip()
                break

        return item_section

    def parse_10q_parts(
        self, parts: List[str], text: str, ignore_matches: int = 0
    ) -> Tuple[Dict[str, str], List[int]]:
        """
        Iterate over different parts and parse their data from text.

        Args:
            parts (List[str]): The parts to parse
            text (str): The text of the document
            ignore_matches (int): Default is 0. If positive, skip the first [value] matches.

        Returns:
            Tuple[Dict[str, str], List[int]]: Content of each part and end-positions
        """
        texts = {}
        part_positions = []
        for i, part in enumerate(parts):
            next_part = parts[i + 1:]
            part_section, part_positions = self.parse_item(
                text, part, next_part, part_positions, ignore_matches
            )
            texts[part] = part_section

        return texts, part_positions

    def check_10q_parts_for_bugs(
        self,
        text: str,
        texts: Dict[str, str],
        part_positions: List[int],
        filing_metadata: Dict[str, Any],
    ) -> Dict[str, str]:
        """
        Check and fix common bugs in 10-Q reports.

        Args:
            text (str): Full text of report
            texts (Dict[str, str]): Dictionary with text for each part
            part_positions (List[int]): End-positions of parts in text
            filing_metadata (Dict[str, Any]): Metadata of file

        Returns:
            Dict[str, str]: Fixed Dictionary with text for each part
        """
        if not part_positions or not texts:
            LOGGER.debug(
                f'{filing_metadata["filename"]} - Could not detect positions/texts of parts.'
            )
        elif not texts["part_1"] and part_positions:
            LOGGER.debug(
                f'{filing_metadata["filename"]} - No PART I found. Extracting all text before PART II as PART I.'
            )
            texts["part_1"] = text[: part_positions[0] - len(texts["part_2"])]

        elif len(part_positions) > 1:
            if part_positions[1] - len(texts["part_2"]) - part_positions[0] > 200:
                separation = part_positions[1] - len(texts["part_2"]) - part_positions[0]
                LOGGER.debug(
                    f'{filing_metadata["filename"]} - End of PART I is {separation} chars from PART II. '
                    'Extracting all text between parts.'
                )
                texts["part_1"] = text[
                    part_positions[0] - len(texts["part_1"]) : part_positions[1] - len(texts["part_2"])
                ]

        return texts

    def get_10q_parts(
        self, text: str, filing_metadata: Dict[str, Any]
    ) -> Dict[str, str]:
        """
        Get parts from 10-Q reports.

        Args:
            text (str): Full text of report
            filing_metadata (Dict[str, Any]): Metadata of filing

        Returns:
            Dict[str, str]: Dictionary containing text of each part
        """
        parts = []
        for item in self.items_list:
            part = item.split("__")[0]
            if part not in parts:
                parts.append(part)
        self.items_list = parts

        texts, part_positions = self.parse_10q_parts(parts, text, ignore_matches=0)
        texts = self.check_10q_parts_for_bugs(text, texts, part_positions, filing_metadata)

        # Handle case where PART II starts in ToC & PART I only contains ToC text
        ignore_matches = 1
        length_difference = len(texts["part_2"]) - len(texts["part_1"])
        while length_difference > 5000:
            texts, part_positions = self.parse_10q_parts(parts, text, ignore_matches=ignore_matches)
            texts["part_1"] = ""
            texts = self.check_10q_parts_for_bugs(text, texts, part_positions, filing_metadata)

            new_length_difference = len(texts["part_2"]) - len(texts["part_1"])
            if new_length_difference == length_difference:
                texts, part_positions = self.parse_10q_parts(parts, text, ignore_matches=0)
                texts = self.check_10q_parts_for_bugs(text, texts, part_positions, filing_metadata)
                LOGGER.debug(
                    f'{filing_metadata["filename"]} - Could not separate PARTs correctly. '
                    'Likely PART I contains just ToC content.'
                )
                break
            length_difference = new_length_difference
            ignore_matches += 1

        self.items_list = item_list_10q
        return texts

    def extract_items(self, filing_metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            """
            Extracts all items/sections from a file.

            Args:
                filing_metadata (Dict[str, Any]): Filing metadata

            Returns:
                Optional[Dict[str, Any]]: Extracted JSON content or None if extraction fails
            """
            try:
                absolute_filename = os.path.join(
                    self.raw_files_folder, filing_metadata["Type"], filing_metadata["filename"]
                )

                # Read file content
                with open(absolute_filename, "r", errors="backslashreplace") as file:
                    content = file.read()

                # Remove embedded PDFs
                content = re.sub(r"<PDF>.*?</PDF>", "", content, flags=regex_flags)

                # Initialize JSON content with metadata
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

                # Process XML-based forms
                if filing_metadata["Type"] in ["3", "4", "SC13D", "SC13D/A", "SC13G", "SC13G/A"]:
                    xml_data = self._extract_xml_data(content, filing_metadata["Type"])
                    if xml_data:
                        json_content.update(xml_data)
                        return json_content
                    LOGGER.warning(f"No XML data extracted for {filing_metadata['filename']}")
                    return None

                # Find document section
                documents = re.findall("<DOCUMENT>.*?</DOCUMENT>", content, flags=regex_flags)
                doc_report = None
                found = False
                is_html = False

                # Process regular forms
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

                if filing_metadata["filename"].endswith("txt") and not documents:
                    LOGGER.info(f'No <DOCUMENT> tag for {filing_metadata["filename"]}')

                if self.remove_tables:
                    doc_report = self.remove_html_tables(doc_report, is_html=is_html)

                doc_report = self.handle_spans(doc_report, is_html=is_html)

                # Extract and clean text
                text = self.strip_html(str(doc_report))
                text = self.clean_text(text)

                # Handle 10-Q parts separately
                if filing_metadata["Type"] == "10-Q":
                    part_texts = self.get_10q_parts(text, filing_metadata)

                positions = []
                all_items_null = True

                # Process each item
                for i, item_index in enumerate(self.items_list):
                    next_item_list = self.items_list[i + 1:]

                    if "part" in item_index:
                        if i != 0 and self.items_list[i - 1].split("__")[0] != item_index.split("__")[0]:
                            positions = []
                        text = part_texts[item_index.split("__")[0]]

                        if item_index.split("__")[0] not in json_content:
                            parts_text = self.remove_multiple_lines(part_texts[item_index.split("__")[0].strip()])
                            json_content[item_index.split("__")[0]] = parts_text

                    if "part" in self.items_list[i - 1] and item_index == "SIGNATURE":
                        item_section = part_texts[item_index]
                    else:
                        item_section, positions = self.parse_item(text, item_index, next_item_list, positions)

                    item_section = self.remove_multiple_lines(item_section.strip())

                    if item_index in self.items_to_extract:
                        if item_section:
                            all_items_null = False

                        if item_index == "SIGNATURE" and self.include_signature:
                            json_content[f"{item_index}"] = item_section
                        else:
                            if "part" in item_index:
                                json_content[f"{item_index.split('__')[0]}_item_{item_index.split('__')[1]}"] = item_section
                            else:
                                json_content[f"item_{item_index}"] = item_section

                if all_items_null:
                    LOGGER.info(f"\nCould not extract any item for {absolute_filename}")
                    return None

                return json_content

            except Exception as e:
                LOGGER.error(f"Error processing {filing_metadata['filename']}: {str(e)}")
                return None

    def process_filing(self, filing_metadata: Dict[str, Any]) -> int:
        """
        Process a filing by extracting items and saving to JSON.

        Args:
            filing_metadata (Dict[str, Any]): Filing metadata

        Returns:
            int: 0 if processing skipped, 1 if successful
        """
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

            # Create output directory if needed
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
    """
    Main function to run the extraction process.
    
    Gets the list of supported files (10-K, 8-K, 10-Q, Form 3, Form 4, SC13D, SC13G) 
    and extracts all textual items/sections by calling the extract_items() function.
    """
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
    num_processes = config.get("num_processes", 1)
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
    main()
