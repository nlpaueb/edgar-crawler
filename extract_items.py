import json
import logging
import os
import re
import sys
from html.parser import HTMLParser
from typing import Any, Dict, List, Optional, Tuple

import click
import cssutils
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from pathos.pools import ProcessPool
from tqdm import tqdm

from __init__ import DATASET_DIR
from item_lists import item_list_8k, item_list_8k_obsolete, item_list_10k, item_list_10q
from logger import Logger

# Change the default recursion limit of 1000 to 30000
sys.setrecursionlimit(30000)

# Suppress cssutils stupid warnings
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
    A class used to extract certain items from the raw files.

    Attributes:
        remove_tables (bool): Flag to indicate if tables need to be removed.
        items_list (List[str]): List of all items that could be extracted.
        items_to_extract (List[str]): List of items to be extracted. If not provided, all items will be extracted.
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
        Constructs all the necessary attributes for the ExtractItems object.

        Args:
            remove_tables (bool): Whether to remove tables.
            items_to_extract (List[str]): Items to be extracted. If None, all items are extracted.
            raw_files_folder (str): Path of the folder containing raw files.
            extracted_files_folder (str): Path of the folder where extracted files should be saved.
            skip_extracted_filings (bool): Whether to skip already extracted filings.
        """

        self.remove_tables = remove_tables
        # Default list of items to extract
        self.items_to_extract = items_to_extract
        self.include_signature = include_signature
        self.raw_files_folder = raw_files_folder
        self.extracted_files_folder = extracted_files_folder
        self.skip_extracted_filings = skip_extracted_filings

    def determine_items_to_extract(self, filing_metadata) -> None:
        """
        Determine the items to extract based on the filing type.

        Sets the items_to_extract attribute based on the filing type and the items provided by the user.
        """
        if filing_metadata["Type"] == "10-K":
            items_list = item_list_10k
        elif filing_metadata["Type"] == "8-K":
            # Prior to August 23, 2004, the 8-K items were named differently
            obsolete_cutoff_date_8k = pd.to_datetime("2004-08-23")
            if pd.to_datetime(filing_metadata["Date"]) > obsolete_cutoff_date_8k:
                items_list = item_list_8k
            else:
                items_list = item_list_8k_obsolete
        elif filing_metadata["Type"] == "10-Q":
            items_list = item_list_10q
        else:
            raise Exception(
                f"Unsupported filing type: {filing_metadata['Type']}. No items_list defined."
            )

        self.items_list = items_list

        # Check which items the user provided and which items are available for the filing type
        if self.items_to_extract:
            overlapping_items_to_extract = [
                item for item in self.items_to_extract if item in items_list
            ]
            if overlapping_items_to_extract:
                self.items_to_extract = overlapping_items_to_extract
            else:
                raise Exception(
                    f"Items defined by user do not match the items for {filing_metadata['Type']} filings."
                )
        else:
            self.items_to_extract = items_list

    @staticmethod
    def strip_html(html_content: str) -> str:
        """
        Strip the HTML tags from the HTML content.

        Args:
            html_content (str): The HTML content.

        Returns:
            str: The stripped HTML content.
        """
        # Replace closing tags of certain elements with two newline characters
        html_content = re.sub(r"(<\s*/\s*(div|tr|p|li|)\s*>)", r"\1\n\n", html_content)
        # Replace <br> tags with two newline characters
        html_content = re.sub(r"(<br\s*>|<br\s*/>)", r"\1\n\n", html_content)
        # Replace closing tags of certain elements with a space
        html_content = re.sub(r"(<\s*/\s*(th|td)\s*>)", r" \1 ", html_content)
        # Use HtmlStripper to strip remaining HTML tags
        html_content = HtmlStripper().strip_tags(html_content)

        return html_content

    @staticmethod
    def remove_multiple_lines(text: str) -> str:
        """
        Replace consecutive new lines and spaces with a single new line or space.

        Args:
            text (str): The string containing the text.

        Returns:
            str: The string without multiple new lines or spaces.
        """
        # Replace multiple new lines and spaces with a temporary token
        text = re.sub(r"(( )*\n( )*){2,}", "#NEWLINE", text)
        # Replace all new lines with a space
        text = re.sub(r"\n", " ", text)
        # Replace temporary token with a single new line
        text = re.sub(r"(#NEWLINE)+", "\n", text).strip()
        # Replace multiple spaces with a single space
        text = re.sub(r"[ ]{2,}", " ", text)

        return text

    @staticmethod
    def clean_text(text: str) -> str:
        """
        Clean the text by removing unnecessary blocks of text and substituting special characters.

        Args:
            text (str): The raw text string.

        Returns:
            str: The normalized, clean text.
        """
        # Replace special characters with their corresponding substitutions
        text = re.sub(r"[\xa0]", " ", text)
        text = re.sub(r"[\u200b]", " ", text)
        text = re.sub(r"[\x91]", "‘", text)
        text = re.sub(r"[\x92]", "’", text)
        text = re.sub(r"[\x93]", "“", text)
        text = re.sub(r"[\x94]", "”", text)
        text = re.sub(r"[\x95]", "•", text)
        text = re.sub(r"[\x96]", "-", text)
        text = re.sub(r"[\x97]", "-", text)
        text = re.sub(r"[\x98]", "˜", text)
        text = re.sub(r"[\x99]", "™", text)
        text = re.sub(r"[\u2010\u2011\u2012\u2013\u2014\u2015]", "-", text)
        text = re.sub(r"[\u2018]", "‘", text)
        text = re.sub(r"[\u2019]", "’", text)
        text = re.sub(r"[\u2009]", " ", text)
        text = re.sub(r"[\u00ae]", "®", text)
        text = re.sub(r"[\u201c]", "“", text)
        text = re.sub(r"[\u201d]", "”", text)

        def remove_whitespace(match):
            ws = r"[^\S\r\n]"
            return f'{match[1]}{re.sub(ws, r"", match[2])}{match[3]}{match[4]}'

        def remove_whitespace_signature(match):
            ws = r"[^\S\r\n]"
            return f'{match[1]}{re.sub(ws, r"", match[2])}{match[4]}{match[5]}'

        # Fix broken section headers (PART, ITEM, SIGNATURE)
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
        regex_flags = re.IGNORECASE | re.MULTILINE
        text = re.sub(
            r"\n[^\S\r\n]*"
            r"(TABLE\s+OF\s+CONTENTS|INDEX\s+TO\s+FINANCIAL\s+STATEMENTS|BACK\s+TO\s+CONTENTS|QUICKLINKS)"
            r"[^\S\r\n]*\n",
            "\n",
            text,
            flags=regex_flags,
        )

        # Remove page numbers and headers
        text = re.sub(
            r"\n[^\S\r\n]*[-‒–—]*\d+[-‒–—]*[^\S\r\n]*\n", "\n", text, flags=regex_flags
        )
        text = re.sub(r"\n[^\S\r\n]*\d+[^\S\r\n]*\n", "\n", text, flags=regex_flags)

        text = re.sub(r"[\n\s]F[-‒–—]*\d+", "", text, flags=regex_flags)
        text = re.sub(
            r"\n[^\S\r\n]*Page\s[\d*]+[^\S\r\n]*\n", "", text, flags=regex_flags
        )

        return text

    @staticmethod
    def calculate_table_character_percentages(table_text: str) -> Tuple[float, float]:
        """
        Calculate character type percentages contained in the table text

        Args:
            table_text (str): The table text

        Returns:
            Tuple[float, float]: Percentage of non-blank digit characters, Percentage of space characters
        """
        digits = sum(
            c.isdigit() for c in table_text
        )  # Count the number of digit characters
        spaces = sum(
            c.isspace() for c in table_text
        )  # Count the number of space characters

        if len(table_text) - spaces:
            # Calculate the percentage of non-blank digit characters by dividing the count of digits
            # by the total number of non-space characters
            non_blank_digits_percentage = digits / (len(table_text) - spaces)
        else:
            # If there are no non-space characters, set the percentage to 0
            non_blank_digits_percentage = 0

        if len(table_text):
            # Calculate the percentage of space characters by dividing the count of spaces
            # by the total number of characters
            spaces_percentage = spaces / len(table_text)
        else:
            # If the table text is empty, set the percentage to 0
            spaces_percentage = 0

        return non_blank_digits_percentage, spaces_percentage

    def remove_html_tables(self, doc_report: str, is_html: bool) -> str:
        """
        Remove HTML tables that contain numerical data
        Note that there are many corner-cases in the tables that have text data instead of numerical

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
                        len(
                            list(
                                re.finditer(
                                    rf"\n[^\S\r\n]*{item_index_pattern}[.*~\-:\s]",
                                    tbl_text,
                                    flags=regex_flags,
                                )
                            )
                        )
                        > 0
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
            doc_report = re.sub(
                r"<TABLE>.*?</TABLE>", "", str(doc_report), flags=regex_flags
            )

        return doc_report

    def handle_spans(self, doc: str, is_html) -> str:
        """The documents can contain different span types - some are used for formatting, others for margins.
        Sometimes these spans even appear in the middle of words. We need to handle them depending on their type.
        For spans without a margin, we simply remove them. For spans with a margin, we replace them with a space or newline.

        Args:
            doc (str): The document we want to process
            is_html (bool): Whether the document contains html code or just plain text

        Returns:
            doc (str): The document with spans handled depending on span type
        _______________________________________________________________

        Example for a span with horizontal margin (between the item and the title of the item):
            Input:  Item\xa05.03<span style="font-weight:normal;margin-left:36pt;"></span><span style="color:#000000;">
                    Amendments to Articles of Incorporation or Bylaws
            Output: Item\xa05.03 Amendments to Articles of Incorporation or Bylaws
        Example for a span without margin:
            Input:  B</span><span style=\'background-color:rgba(0,0,0,0);color:rgba(0,0,0,1);white-space:pre-wrap;
                    font-weight:bold;font-size:10.0pt;font-family:"Times New Roman", serif;min-width:fit-content;\'>USINESS
            Output: BUSINESS
        """

        if is_html:
            # Handle spans in the middle of words
            for span in doc.find_all("span"):
                if span.get_text(strip=True):  # If the span contains text
                    span.unwrap()

            # Handle spans with margins
            for span in doc.find_all("span"):
                if "margin-left" or "margin-right" in span.attrs.get("style", ""):
                    # If the span has a horizontal margin, replace it with a space
                    span.replace_with(" ")
                elif "margin-top" or "margin-bottom" in span.attrs.get("style", ""):
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
        Adjust the item_pattern for matching in the document text depending on the item index. This is necessary on a case by case basis.

        Args:
            item_index (str): The item index to adjust the pattern for.
                              For 10-Q preprocessing, this can also be part_1 or part_2.

        Returns:
            item_index_pattern (str): The adjusted item pattern
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
            item_index_pattern = item_index_pattern.replace(
                "A", r"[^\S\r\n]*A(?:\(T\))?"
            )  # Regex pattern for item index "9A"
        elif item_index == "SIGNATURE":
            # Quit here so the A in SIGNATURE is not changed
            pass
        elif "A" in item_index:
            item_index_pattern = item_index_pattern.replace(
                "A", r"[^\S\r\n]*A"
            )  # Regex pattern for other "A" item indexes
        elif "B" in item_index:
            item_index_pattern = item_index_pattern.replace(
                "B", r"[^\S\r\n]*B"
            )  # Regex pattern for "B" item indexes
        elif "C" in item_index:
            item_index_pattern = item_index_pattern.replace(
                "C", r"[^\S\r\n]*C"
            )  # Regex pattern for "C" item indexes

        # If the item is SIGNATURE, we don't want to look for ITEM
        if item_index == "SIGNATURE":
            # Some reports have SIGNATURES or Signature(s) instead of SIGNATURE
            item_index_pattern = rf"{item_index}(s|\(s\))?"
        else:
            if "." in item_index:
                # We need to escape the '.', otherwise it will be treated as a special character - for 8Ks
                item_index = item_index.replace(".", "\.")
            if item_index in roman_numeral_map:
                # Rarely, reports use roman numerals for the item indexes. For 8-K, we assume this does not occur (due to their format - e.g. 5.01)
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
            ignore_matches (int): Default is 0. If positive, we skip the first [value] matches. Only used for 10-Q part extraction.

        Returns:
            Tuple[str, List[int]]: The item/section as a text string and the updated end positions of item sections.
        """

        # Set the regex flags
        regex_flags = re.IGNORECASE | re.DOTALL

        # Adjust the item index pattern
        item_index_pattern = self.adjust_item_patterns(item_index)

        # Determine the current part in case of 10-Q reports
        if "part" in item_index and "PART" not in item_index_pattern:
            item_index_part_number = item_index.split("__")[0]

        # Depending on the item_index, search for subsequent sections.
        # There might be many 'candidate' text sections between 2 Items.
        # For example, the Table of Contents (ToC) still counts as a match when searching text between 'Item 3' and 'Item 4'
        # But we do NOT want that specific text section; We want the detailed section which is *after* the ToC

        possible_sections_list = []  # possible list of (start, end) matches
        impossible_match = None  # list of matches where no possible section was found - (start, None) matches
        last_item = True
        for next_item_index in next_item_list:
            # Check if the next item is the last one
            last_item = False
            if possible_sections_list:
                break
            if next_item_index == next_item_list[-1]:
                last_item = True

            # Adjust the next item index pattern
            next_item_index_pattern = self.adjust_item_patterns(next_item_index)

            # Check if the next item is in a different part - in this case we exit the loop
            if "part" in next_item_index and "PART" not in next_item_index_pattern:
                next_item_index_part_number = next_item_index.split("__")[0]
                if next_item_index_part_number != item_index_part_number:
                    # If the next item is in a subsequent part, we won't find it in the text -> should simply extract the rest of the current part
                    last_item = True
                    break

            # Find all the text sections between the current item and the next item
            matches = list(
                re.finditer(
                    rf"\n[^\S\r\n]*{item_index_pattern}[.*~\-:\s\(]",
                    text,
                    flags=regex_flags,
                )
            )
            for i, match in enumerate(matches):
                if i < ignore_matches:
                    # In some cases, the first matches might capture longer sections because parts/items are mentioned in the ToC.
                    # We detect this in another place and then skip the first [ignore_matches] matches until we are more certain to have the correct section.
                    continue
                offset = match.start()

                # First we do a case-sensitive search. This is because in some reports, parts or items are mentioned in the content,
                # which we don't want to detect as a section header.
                # The section headers are usually in uppercase, so checking this first avoids some errors.
                possible = list(
                    re.finditer(
                        rf"\n[^\S\r\n]*{item_index_pattern}[.*~\-:\s\()].+?(\n[^\S\r\n]*{str(next_item_index_pattern)}[.*~\-:\s\(])",
                        text[offset:],
                        flags=re.DOTALL,
                    )
                )

                if not possible:
                    # If there is no match, follow with a case-insensitive search
                    possible = list(
                        re.finditer(
                            rf"\n[^\S\r\n]*{item_index_pattern}[.*~\-:\s\()].+?(\n[^\S\r\n]*{str(next_item_index_pattern)}[.*~\-:\s\(])",
                            text[offset:],
                            flags=regex_flags,
                        )
                    )

                # If there is a match, add it to the list of possible sections
                if possible:
                    possible_sections_list += [(offset, possible)]
                elif (
                    next_item_index == next_item_list[-1]
                    and not possible_sections_list
                    and match
                ):
                    # If there is no (start, end) section, there might only be a single item in the report (can happen for 8-K)
                    impossible_match = match

        # Extract the wanted section from the text
        item_section, positions = ExtractItems.get_item_section(
            possible_sections_list, text, positions
        )

        # If item is the last one (usual case when dealing with EDGAR's old .txt files), get all the text from its beginning until EOF.
        if positions:
            # If the item is the last one, get all the text from its beginning until EOF
            # This is needed in cases where the SIGNATURE section cannot be found
            if item_index in self.items_list and item_section == "":
                item_section = self.get_last_item_section(item_index, text, positions)
            # SIGNATURE is the last one, get all the text from its beginning until EOF
            if item_index == "SIGNATURE":
                item_section = self.get_last_item_section(item_index, text, positions)
        elif impossible_match or last_item:
            # If there is only a single item in a report and no SIGNATURE (can happen for 8-K reports),
            # 'possible_sections_list' and thus also 'positions' will always be empty.
            # In this case we just want to extract from the match until the end of the document
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
        Returns the correct section from a list of all possible item sections.

        Args:
            possible_sections_list: List containing all the possible sections between Item X and Item Y.
            text: The whole text.
            positions: List of the end positions of previous item sections.

        Returns:
            Tuple[str, List[int]]: The correct section and the updated list of end positions.
        """

        # Initialize variables
        item_section: str = ""
        max_match_length: int = 0
        max_match: Optional[re.Match] = None
        max_match_offset: Optional[int] = None

        # Find the match with the largest section
        for offset, matches in possible_sections_list:
            # Find the match with the largest section
            for match in matches:
                match_length = match.end() - match.start()
                # If there are previous item sections, check if the current match is after the last item section
                if positions:
                    if (
                        match_length > max_match_length
                        and offset + match.start() >= positions[-1]
                    ):
                        max_match = match
                        max_match_offset = offset
                        max_match_length = match_length
                # If there are no previous item sections, just get the first match
                elif match_length > max_match_length:
                    max_match = match
                    max_match_offset = offset
                    max_match_length = match_length

        # Return the text section inside that match
        if max_match:
            # If there are previous item sections, check if the current match is after the last item section and get it
            if positions:
                if max_match_offset + max_match.start() >= positions[-1]:
                    item_section = text[
                        max_match_offset + max_match.start() : max_match_offset
                        + max_match.regs[1][0]
                    ]
            else:  # If there are no previous item sections, just get the text section inside that match
                item_section = text[
                    max_match_offset + max_match.start() : max_match_offset
                    + max_match.regs[1][0]
                ]
            # Update the list of end positions
            positions.append(max_match_offset + max_match.end() - len(max_match[1]) - 1)

        return item_section, positions

    def get_last_item_section(
        self, item_index: str, text: str, positions: List[int]
    ) -> str:
        """
        Returns the text section starting through a given item. This is useful in cases where Item 15 is the last item
        and there is no Item 16 to indicate its ending (for 10-K reports). Also, it is useful in cases like EDGAR's old .txt files
        (mostly before 2005), where there is no Item 15; thus, ITEM 14 is the last one there.

        Args:
            item_index (str): The index of the item/section in the report
            text (str): The whole report text
            positions (List[int]): List of the end positions of previous item sections

        Returns:
            str: All the remaining text until the end, starting from the specified item_index
        """

        # Adjust the item index pattern
        item_index_pattern = self.adjust_item_patterns(item_index)

        # Find all occurrences of the item/section using regex
        item_list = list(
            re.finditer(
                rf"\n[^\S\r\n]*{item_index_pattern}[.\-:\s].+?", text, flags=regex_flags
            )
        )

        item_section = ""
        for item in item_list:
            if "SIGNATURE" in item_index:
                # For SIGNATURE we want to take the last match since it can also appear in the ToC and mess up the extraction
                if item != item_list[-1]:
                    continue
            # Check if the item starts after the last known position
            if positions:
                if item.start() >= positions[-1]:
                    # Extract the remaining text from the specified item_index
                    item_section = text[item.start() :].strip()
                    break
            else:
                # Extract the remaining text from the specified item_index
                item_section = text[item.start() :].strip()
                break

        return item_section

    def parse_10q_parts(
        self, parts: List[str], text: str, ignore_matches: int = 0
    ) -> Tuple[Dict[str, str], List[int]]:
        """Iterate over the different parts and parse their data from the text.

        Args:
            parts (List[str]): The parts we want to parse
            text (str): The text of the document
            ignore_matches (int): Default is 0. If positive, we skip the first [value] matches. Only used for 10-Q part extraction.

        Returns:
            Tuple[Dict[str, str], List[int]]: The content of each part and the end-positions of the parts in the text.
        """

        texts = {}
        part_positions = []
        for i, part in enumerate(parts):
            # Find the section of the text that corresponds to the current part
            next_part = parts[i + 1 :]
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
        """Since 10-Q reports fairly often contain bugs, we check for a series of cases in this function.

        Args:
            text (str): The full text of the report
            texts (Dict[str, str]): Dictionary with the text for each part
            positions (List[int]): End-positions of the parts in the text
            filing_metadata (Dict[str, Any]): Metadata of the file

        Returns:
            texts (dict): The fixed Dictionary with the text for each part
        """

        # In some cases (mainly older .txt reports), part I is not mentioned in the text, only part II
        # Here, we can instead extract all the text before the position of part II and set it as part I
        if not part_positions or not texts:
            LOGGER.debug(
                f'{filing_metadata["filename"]} - Could not detect positions/texts of parts.'
            )
        elif not texts["part_1"] and part_positions:
            LOGGER.debug(
                f'{filing_metadata["filename"]} - Detected error in part separation - No PART I found. Changing Extraction to extract all text before PART II as PART I.'
            )
            # The positions indicate the end of the part. So we need to substract the length of the second part to get the end of the first part
            texts["part_1"] = text[: part_positions[0] - len(texts["part_2"])]

        # In some cases, PART I is only mentioned in the ToC while PART II is mentioned as normal
        # Then, we would only extract the ToC content for PART I
        # By checking the distance between the two parts, we can detect this error
        elif len(part_positions) > 1:
            if part_positions[1] - len(texts["part_2"]) - part_positions[0] > 200:
                separation = (
                    part_positions[1] - len(texts["part_2"]) - part_positions[0]
                )
                LOGGER.debug(
                    f'{filing_metadata["filename"]} - Detected error in part separation - End of PART I is {separation} chars from start of PART II. Changing Extraction to extract all text between the two parts.'
                )
                # LOGGER.warning('This is likely due to an error in the report formatting. Be careful when working with the extracted Items. The text might not be separated correctly.')
                # If the distance is very large, we instead simply extract all text between the two parts
                texts["part_1"] = text[
                    part_positions[0] - len(texts["part_1"]) : part_positions[1]
                    - len(texts["part_2"])
                ]

        return texts

    def get_10q_parts(
        self, text: str, filing_metadata: Dict[str, Any]
    ) -> Dict[str, str]:
        """
        For 10-Q reports, we have two parts with items which can have the same name (e.g. an item 1 in part 1 and an item 1 in part 2).
        Because of this, we need to separate the report text according to the different parts before extracting the items.

        Sometimes we get problems with the part extraction. Because of this, we check a few heuristics:
            1. If not part-texts or part-positions are found, we cannot extract anything
            2. If we don't have text for part I but have positions, we extract all text before part II as part I
            3. If the distance between part I and part II is too large, we extract all text between the two parts and add it to part I
            4. If part II is much longer than part I, we extract part II again but ignore the first [ignore_matches] matches for the
               parts until it is not much longer
            In all four cases, we raise log warnings to inform the user about potential problems in the report.

        Args:
            text (str): the full text of the report.
            filing_metadata: Dict[str, Any]: a dictionary containing the metadata of the filing.

        Returns:
            texts (Dict[str, str]): a dictionary containing the text of each part.
        """

        # Detect all existing parts in the item_list - use loop to not have duplicates but keep order
        parts = []
        for item in self.items_list:
            part = item.split("__")[0]
            if part not in parts:
                parts.append(part)
        # Need to re-set items_list to parts for this step
        self.items_list = parts

        texts, part_positions = self.parse_10q_parts(parts, text, ignore_matches=0)

        ### Check for potential problems in 10-Q reports - see docstring ###
        texts = self.check_10q_parts_for_bugs(
            text, texts, part_positions, filing_metadata
        )

        # In some cases, PART II already starts in the ToC & PART I only contains ToC text. PART II is then noticably longer than PART I
        # However, usually PART I is the much longer part.
        # In this case, we extract only PART II again but ignore the first [ignore_matches] matches for the parts until we find the correct PARTs
        ignore_matches = 1
        length_difference = len(texts["part_2"]) - len(texts["part_1"])
        while length_difference > 5000:
            texts, part_positions = self.parse_10q_parts(
                parts, text, ignore_matches=ignore_matches
            )
            # Remove text of part 1 - we will later extract all text before part 2 for part 1
            texts["part_1"] = ""

            # Check for bugs again
            texts = self.check_10q_parts_for_bugs(
                text, texts, part_positions, filing_metadata
            )

            # Recalculate the length difference
            new_length_difference = len(texts["part_2"]) - len(texts["part_1"])
            if new_length_difference == length_difference:
                # If the difference did not change, we stop here and extract as normal again
                texts, part_positions = self.parse_10q_parts(
                    parts, text, ignore_matches=0
                )
                texts = self.check_10q_parts_for_bugs(
                    text, texts, part_positions, filing_metadata
                )
                LOGGER.debug(
                    f'{filing_metadata["filename"]} - Could not separate PARTs correctly. Likely PART I contains just ToC content.'
                )
                break
            length_difference = new_length_difference

            # If we still have a large difference, we need to ignore more matches
            ignore_matches += 1

        ### End of checking for the 4 heuristics mentioned in docstring ###

        # Set items_list back to 10q items
        self.items_list = item_list_10q

        return texts

    def extract_items(self, filing_metadata: Dict[str, Any]) -> Any:
        """
        Extracts all items/sections for a file and writes it to a CIK_TYPE_YEAR.json file (eg. 1384400_10K_2017.json)

        Args:
            filing_metadata (Dict[str, Any]): a pandas series containing all filings metadata

        Returns:
            Any: The extracted JSON content
        """

        absolute_filename = os.path.join(
            self.raw_files_folder, filing_metadata["Type"], filing_metadata["filename"]
        )

        # Read the content of the file
        with open(absolute_filename, "r", errors="backslashreplace") as file:
            content = file.read()

        # Remove all embedded pdfs that might be seen in few old txt annual reports
        content = re.sub(r"<PDF>.*?</PDF>", "", content, flags=regex_flags)

        # Find all <DOCUMENT> tags within the content
        documents = re.findall("<DOCUMENT>.*?</DOCUMENT>", content, flags=regex_flags)

        # Initialize variables
        doc_report = None
        found, is_html = False, False

        # Find the document
        for doc in documents:
            # Find the <TYPE> tag within each <DOCUMENT> tag to identify the type of document
            doc_type = re.search(r"\n[^\S\r\n]*<TYPE>(.*?)\n", doc, flags=regex_flags)
            doc_type = doc_type.group(1) if doc_type else None

            # Check if the document is an allowed document type
            if doc_type.startswith(("10", "8")):
                # For 10-K, 10-Q and 8-K filings. We only check for the number in case it is e.g. '10K' instead of '10-K'
                # Check if the document is HTML or plain text
                doc_report = BeautifulSoup(doc, "lxml")
                is_html = (True if doc_report.find("td") else False) and (
                    True if doc_report.find("tr") else False
                )
                if not is_html:
                    doc_report = doc
                found = True
                # break

        if not found:
            if documents:
                LOGGER.info(
                    f'\nCould not find documents for {filing_metadata["filename"]}'
                )
            # If no document is found, parse the entire content as HTML or plain text
            doc_report = BeautifulSoup(content, "lxml")
            is_html = (True if doc_report.find("td") else False) and (
                True if doc_report.find("tr") else False
            )
            if not is_html:
                doc_report = content

        # Check if the document is plain text without <DOCUMENT> tags (e.g., old TXT format)
        if filing_metadata["filename"].endswith("txt") and not documents:
            LOGGER.info(f'\nNo <DOCUMENT> tag for {filing_metadata["filename"]}')

        # For non-HTML documents, clean all table items
        if self.remove_tables:
            doc_report = self.remove_html_tables(doc_report, is_html=is_html)

        # Detect span elements and handle them depending on span type
        doc_report = self.handle_spans(doc_report, is_html=is_html)

        # Prepare the JSON content with filing metadata
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

        # Initialize item sections as empty strings in the JSON content
        # for item_index in self.items_to_extract:
        #     if item_index == "SIGNATURE":
        #         if self.include_signature:
        #             json_content[f"{item_index}"] = ""
        #     else:
        #         json_content[f"item_{item_index}"] = ""

        # Extract the text from the document and clean it
        text = ExtractItems.strip_html(str(doc_report))
        text = ExtractItems.clean_text(text)

        # For 10-Qs, need to separate the text into Part 1 and Part 2
        if filing_metadata["Type"] == "10-Q":
            part_texts = self.get_10q_parts(text, filing_metadata)

        positions = []
        all_items_null = True
        for i, item_index in enumerate(self.items_list):
            next_item_list = self.items_list[i + 1 :]

            # If the text is divided in parts, we just take the text from the corresponding part
            if "part" in item_index:
                if i != 0:
                    # We need to reset the positions to [] for each new part
                    if (
                        self.items_list[i - 1].split("__")[0]
                        != item_index.split("__")[0]
                    ):
                        positions = []
                text = part_texts[item_index.split("__")[0]]

                # We want to add a separate key for each full part in the JSON content, which should be placed before the items of that part
                if item_index.split("__")[0] not in json_content:
                    parts_text = ExtractItems.remove_multiple_lines(
                        part_texts[item_index.split("__")[0].strip()]
                    )
                    json_content[item_index.split("__")[0]] = parts_text

            if "part" in self.items_list[i - 1] and item_index == "SIGNATURE":
                # We are working with a 10-Q but the above if-statement is not triggered
                # We can just take the detected part_text for the signature - but we do not want to run parse_item again below
                item_section = part_texts[item_index]
            else:
                ### Parse each item/section and get its content and positions - For 10-K and 8-K we will just run this! ###
                item_section, positions = self.parse_item(
                    text, item_index, next_item_list, positions
                )

            # Remove multiple lines from the item section
            item_section = ExtractItems.remove_multiple_lines(item_section.strip())

            if item_index in self.items_to_extract:
                if item_section != "":
                    all_items_null = False

                # Add the item section to the JSON content
                if item_index == "SIGNATURE":
                    if self.include_signature:
                        json_content[f"{item_index}"] = item_section
                else:
                    if "part" in item_index:
                        # special naming convention for 10-Qs
                        json_content[
                            item_index.split("__")[0]
                            + "_item_"
                            + item_index.split("__")[1]
                        ] = item_section
                    else:
                        json_content[f"item_{item_index}"] = item_section

        if all_items_null:
            LOGGER.info(f"\nCould not extract any item for {absolute_filename}")
            return None

        return json_content

    def process_filing(self, filing_metadata: Dict[str, Any]) -> int:
        """
        Process a filing by extracting items/sections and saving the content to a JSON file.

        Args:
            filing_metadata (Dict[str, Any]): A dictionary containing the filing metadata.

        Returns:
            int: 0 if the processing is skipped, 1 if the processing is performed.
        """

        # Generate the JSON filename based on the original filename
        json_filename = f'{filing_metadata["filename"].split(".")[0]}.json'

        # Determine which items to extract based on the filing type and the items provided by the user
        self.determine_items_to_extract(filing_metadata)

        # Create the absolute path for the JSON file
        absolute_json_filename = os.path.join(
            self.extracted_files_folder, filing_metadata["Type"], json_filename
        )

        # Skip processing if the extracted JSON file already exists and skip flag is enabled
        if self.skip_extracted_filings and os.path.exists(absolute_json_filename):
            return 0

        # Extract items from the filing
        json_content = self.extract_items(filing_metadata)

        # First, Check if the filing type specific folder exists and create it if it doesn't
        if not os.path.isdir(
            os.path.join(self.extracted_files_folder, filing_metadata["Type"])
        ):
            os.mkdir(os.path.join(self.extracted_files_folder, filing_metadata["Type"]))
        # Write the JSON content to the file if it's not None
        if json_content is not None:
            with open(absolute_json_filename, "w", encoding="utf-8") as filepath:
                json.dump(json_content, filepath, indent=4, ensure_ascii=False)

        return 1


def main() -> None:
    """
    Gets the list of supported (10K, 8K, 10Q) files and extracts all textual items/sections by calling the extract_items() function.
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

    # For debugging one report
    # debug_file_name = "1002135_10Q_1998_0000914760-99-000052.txt"
    # filings_metadata_df = filings_metadata_df[filings_metadata_df["filename"] == debug_file_name]

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
    )

    LOGGER.info(
        f"Starting the structured JSON extraction from {len(filings_metadata_df)} unstructured EDGAR filings."
    )

    list_of_series = list(zip(*filings_metadata_df.iterrows()))[1]

    # Process filings in parallel using a process pool
    with ProcessPool(processes=1) as pool:
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
