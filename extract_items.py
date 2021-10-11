import click
import cssutils
import json
import logging
import os
import re
import sys

from bs4 import BeautifulSoup
from html.parser import HTMLParser
from pathos.multiprocessing import ProcessPool
from tqdm import tqdm
from typing import List

from logger import Logger

from __init__ import DATASET_DIR

# Change the default recursion limit of 1000 to 30000
sys.setrecursionlimit(30000)

# Supress cssutils stupid warnings
cssutils.log.setLevel(logging.CRITICAL)

cli = click.Group()

regex_flags = re.IGNORECASE | re.DOTALL | re.MULTILINE

# Instantiate a logger object
LOGGER = Logger(name='ExtractItems').get_logger()


class HtmlStripper(HTMLParser):
    """
    Strips HTML tags
    """

    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.fed = []

    def handle_data(self, d):
        self.fed.append(d)

    def get_data(self):
        return ''.join(self.fed)

    def strip_tags(self, html):
        self.feed(html)
        return self.get_data()


class ExtractItems:
    def __init__(
            self,
            items_to_extract: List,
            raw_files_folder: str,
            extracted_files_folder: str
    ):
        # Read from config
        self.raw_files_folder = raw_files_folder
        self.extracted_files_folder = extracted_files_folder

        self.items_list = ['1', '1A', '1B', '2', '3', '4', '5', '6', '7', '7A', '8', '9', '9A', '9B', '10', '11', '12', '13', '14', '15']
        self.items_to_extract = items_to_extract

    @staticmethod
    def strip_html(html_content):
        """
        Strips the html content to get clean text
        :param html_content: The HTML content
        :return: The clean HTML content
        """

        # TODO: Check if flags are required in the following regex
        html_content = re.sub(r'(<\s*/\s*(div|tr|p|li|)\s*>)', r'\1\n\n', html_content)
        html_content = re.sub(r'(<br\s*>|<br\s*/>)', r'\1\n\n', html_content)
        html_content = re.sub(r'(<\s*/\s*(th|td)\s*>)', r' \1 ', html_content)
        html_content = HtmlStripper().strip_tags(html_content)

        return html_content

    @staticmethod
    def remove_multiple_lines(text):
        """
        Replaces consecutive new lines with a single new line
        and consecutive whitespace characters with a single whitespace
        :param text: String containing the financial text
        :return: String without multiple newlines
        """

        text = re.sub(r'(( )*\n( )*){2,}', '#NEWLINE', text)
        text = re.sub(r'\n', ' ', text)
        text = re.sub(r'(#NEWLINE)+', '\n', text).strip()
        text = re.sub(r'[ ]{2,}', ' ', text)

        return text

    @staticmethod
    def clean_text(text):
        """
        Clean the text of various unnecessary blocks of text
        Substitute various special characters

        :param text: Raw text string
        :return: String containing normalized, clean text
        """

        text = re.sub(r'[\xa0]', ' ', text)
        text = re.sub(r'[\u200b]', ' ', text)

        text = re.sub(r'[\x91]', '‘', text)
        text = re.sub(r'[\x92]', '’', text)
        text = re.sub(r'[\x93]', '“', text)
        text = re.sub(r'[\x94]', '”', text)
        text = re.sub(r'[\x95]', '•', text)
        text = re.sub(r'[\x96]', '-', text)
        text = re.sub(r'[\x97]', '-', text)
        text = re.sub(r'[\x98]', '˜', text)
        text = re.sub(r'[\x99]', '™', text)

        text = re.sub(r'[\u2010\u2011\u2012\u2013\u2014\u2015]', '-', text)

        def remove_whitespace(match):
            ws = r'[^\S\r\n]'
            return f'{match[1]}{re.sub(ws, r"", match[2])}{match[3]}{match[4]}'

        # Fix broken section headers
        text = re.sub(r'(\n[^\S\r\n]*)(P[^\S\r\n]*A[^\S\r\n]*R[^\S\r\n]*T)([^\S\r\n]+)((\d{1,2}|[IV]{1,2})[AB]?)',
                      remove_whitespace, text, flags=re.IGNORECASE)
        text = re.sub(r'(\n[^\S\r\n]*)(I[^\S\r\n]*T[^\S\r\n]*E[^\S\r\n]*M)([^\S\r\n]+)(\d{1,2}[AB]?)',
                      remove_whitespace, text, flags=re.IGNORECASE)

        text = re.sub(r'(ITEM|PART)(\s+\d{1,2}[AB]?)([\-•])', r'\1\2 \3 ', text, flags=re.IGNORECASE)

        # Remove unnecessary headers
        text = re.sub(r'\n[^\S\r\n]*'
                      r'(TABLE\s+OF\s+CONTENTS|INDEX\s+TO\s+FINANCIAL\s+STATEMENTS|BACK\s+TO\s+CONTENTS|QUICKLINKS)'
                      r'[^\S\r\n]*\n',
                      '\n', text, flags=regex_flags)

        # Remove page numbers and headers
        text = re.sub(r'\n[^\S\r\n]*[-‒–—]*\d+[-‒–—]*[^\S\r\n]*\n', '\n', text, flags=regex_flags)
        text = re.sub(r'\n[^\S\r\n]*\d+[^\S\r\n]*\n', '\n', text, flags=regex_flags)

        text = re.sub(r'[\n\s]F[-‒–—]*\d+', '', text, flags=regex_flags)
        text = re.sub(r'\n[^\S\r\n]*Page\s[\d*]+[^\S\r\n]*\n', '', text, flags=regex_flags)

        return text

    @staticmethod
    def calculate_table_character_percentages(table_text):
        """
        Calculate character type percentages contained in the table text

        :param table_text: The table text
        :return non_blank_digits_percentage: Percentage of digit characters
        :return spaces_percentage: Percentage of space characters
        """
        digits = sum(c.isdigit() for c in table_text)
        # letters   = sum(c.isalpha() for c in table_text)
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

    @staticmethod
    def remove_html_tables(doc_10k):
        """
        Remove HTML tables that contain numerical data
        Note that there are many corner-cases in the tables that have text data instead of numerical

        :param doc_10k: The 10-K html
        :return: doc_10k: The 10-K html without numerical tables
        """

        tables = doc_10k.find_all('table')

        # Detect tables that have numerical data
        for tbl in tables:
            trs = tbl.find_all('tr', attrs={'style': True}) + \
                  tbl.find_all('td', attrs={'style': True}) + \
                  tbl.find_all('th', attrs={'style': True})

            background_found = False

            for tr in trs:
                # Parse given cssText which is assumed to be the content of a HTML style attribute
                style = cssutils.parseStyle(tr['style'])

                if (style['background']
                    and style['background'].lower() not in ['none', 'transparent', '#ffffff', '#fff', 'white']) \
                    or (style['background-color']
                        and style['background-color'].lower() not in ['none', 'transparent', '#ffffff', '#fff', 'white']):
                    background_found = True
                    break

            trs = tbl.find_all('tr', attrs={'bgcolor': True}) + tbl.find_all('td', attrs={
                'bgcolor': True}) + tbl.find_all('th', attrs={'bgcolor': True})

            bgcolor_found = False
            for tr in trs:
                if tr['bgcolor'].lower() not in ['none', 'transparent', '#ffffff', '#fff', 'white']:
                    bgcolor_found = True
                    break

            if bgcolor_found or background_found:
                tbl.decompose()

        return doc_10k

    def parse_item(self, text, item_index, next_item_list, positions):
        """
        Parses Item N for a 10-K text

        :param text: The 10-K text
        :param item_index: Number of the requested Item/Section of the 10-K text
        :param next_item_list: List of possible next 10-K item sections
        :param positions: List of the end positions of previous item sections
        :return: item_section: The item/section as a text string
        """

        if item_index == '9A':
            item_index = item_index.replace('A', r'[^\S\r\n]*A(?:\(T\))?')
        elif 'A' in item_index:
            item_index = item_index.replace('A', r'[^\S\r\n]*A')
        elif 'B' in item_index:
            item_index = item_index.replace('B', r'[^\S\r\n]*B')

        # Depending on the item_index, search for subsequent sections.

        # There might be many 'candidate' text sections between 2 Items.
        # For example, the Table of Contents (ToC) still counts as a match when searching text between 'Item 3' and 'Item 4'
        # But we do NOT want that specific text section; We want the detailed section which is *after* the ToC

        possible_sections_list = []
        for next_item_index in next_item_list:
            if possible_sections_list:
                break
            if next_item_index == '9A':
                next_item_index = next_item_index.replace('A', r'[^\S\r\n]*A(?:\(T\))?')
            elif 'A' in next_item_index:
                next_item_index = next_item_index.replace('A', r'[^\S\r\n]*A')
            elif 'B' in next_item_index:
                next_item_index = next_item_index.replace('B', r'[^\S\r\n]*B')

            for match in list(re.finditer(rf'\n[^\S\r\n]*ITEM\s+{item_index}[.*~\-:\s]', text, flags=regex_flags)):
                offset = match.start()

                possible = list(re.finditer(
                    rf'\n[^\S\r\n]*ITEM\s+{item_index}[.*~\-:\s].+?([^\S\r\n]*ITEM\s+{str(next_item_index)}[.*~\-:\s])',
                    text[offset:], flags=regex_flags))
                if possible:
                    possible_sections_list += [(offset, possible)]

        # Extract the wanted section from the text
        item_section, positions = ExtractItems.get_item_section(possible_sections_list, text, positions)

        # If item is the last one (usual case when dealing with EDGAR's old .txt files), get all the text from its beginning until EOF.
        if positions:
            if item_index in self.items_list and item_section == '':
                item_section = ExtractItems.get_last_item_section(item_index, text, positions)
            elif item_index == '15':  # Item 15 is the last one, get all the text from its beginning until EOF
                item_section = ExtractItems.get_last_item_section(item_index, text, positions)

        return item_section.strip(), positions

    @staticmethod
    def get_item_section(possible_sections_list, text, positions):
        """
        Throughout a list of all the possible item sections, it returns the biggest one, which (probably) is the correct one.

        :param possible_sections_list: List containing all the possible sections betweewn Item X and Item Y
        :param text: The whole text
        :param positions: List of the end positions of previous item sections
        :return: The correct section
        """

        item_section = ''
        max_match_length = 0
        max_match = None
        max_match_offset = None

        # Find the match with the largest section
        for (offset, matches) in possible_sections_list:
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

        # Return the text section inside that match
        if max_match:
            if positions:
                if max_match_offset + max_match.start() >= positions[-1]:
                    item_section = text[max_match_offset + max_match.start(): max_match_offset + max_match.regs[1][0]]
            else:
                item_section = text[max_match_offset + max_match.start(): max_match_offset + max_match.regs[1][0]]
            positions.append(max_match_offset + max_match.end() - len(max_match[1]) - 1)

        return item_section, positions

    @staticmethod
    def get_last_item_section(item_index, text, positions):
        """
        Returns the text section starting through a given item. This is useful in cases where Item 15 is the last item
        and there is no Item 16 to indicate its ending. Also, it is useful in cases like EDGAR's old .txt files
        (mostly before 2005), where there there is no Item 15; thus, ITEM 14 is the last one there.

        :param item_index: The index of the item/section in the 10-K ('14' or '15')
        :param text: The whole 10-K text
        :param positions: List of the end positions of previous item sections
        :return: All the remaining text until the end, starting from the specified item_index
        """

        item_list = list(re.finditer(rf'\n[^\S\r\n]*ITEM\s+{item_index}[.\-:\s].+?', text, flags=regex_flags))

        item_section = ''
        for item in item_list:
            if item.start() >= positions[-1]:
                item_section = text[item.start():].strip()
                break

        return item_section

    def extract_items(self, form10k_file):
        """
        Extracts all items/sections for a 10-K file and writes it to a CIK_10K_YEAR.json file (eg. 1384400_10K_2017.json)

        :param form10k_file: Each htm/txt 10-K file
        """

        absolute_10k_filename = os.path.join(self.raw_files_folder, form10k_file)

        with open(absolute_10k_filename, 'r', errors='backslashreplace') as file:
            content = file.read()

        # Remove all embedded pdfs that might be seen in few old 10-K txt annual reports
        content = re.sub(r'<PDF>.*?</PDF>', '', content, flags=regex_flags)

        documents = re.findall('<DOCUMENT>.*?</DOCUMENT>', content, flags=regex_flags)

        doc_10k = None
        found_10k, is_html = False, False
        for doc in documents:
            doc_type = re.search(r'\n[^\S\r\n]*<TYPE>(.*?)\n', doc, flags=regex_flags)
            doc_type = doc_type.group(1) if doc_type else None
            if doc_type.startswith('10'):
                doc_10k = BeautifulSoup(doc, 'lxml')
                is_html = (True if doc_10k.find('td') else False) and (True if doc_10k.find('tr') else False)
                if not is_html:
                    doc_10k = doc
                found_10k = True
                break

        if not found_10k:
            if documents:
                LOGGER.info(f'Could not find document type 10K for {form10k_file}')
            doc_10k = BeautifulSoup(content, 'lxml')
            is_html = (True if doc_10k.find('td') else False) and (True if doc_10k.find('tr') else False)
            if not is_html:
                doc_10k = content

        # if not is_html and not documents:
        if form10k_file.endswith('txt') and not documents:
            LOGGER.info(f'No <DOCUMENT> tag for {form10k_file}')

        # For non html clean all table items
        if is_html:
            doc_10k = self.remove_html_tables(doc_10k)
        else:
            doc_10k = re.sub(r'<TABLE>.*?</TABLE>', '', str(doc_10k), flags=regex_flags)

        splits = form10k_file.split('.')[:-1][0].split('_')

        json_content = {
            'filename': form10k_file,
            'cik': splits[0],
            'filing_type': splits[1],
            'year': splits[2],
            'accession_number': splits[3]
        }
        for item_index in self.items_to_extract:
            json_content[f'section_{item_index}'] = ''

        text = ExtractItems.strip_html(str(doc_10k))
        text = ExtractItems.clean_text(text)

        positions = []
        all_sections_null = True
        for i, item_index in enumerate(self.items_list):
            next_item_list = self.items_list[i+1:]
            item_section, positions = self.parse_item(text, item_index, next_item_list, positions)
            item_section = ExtractItems.remove_multiple_lines(item_section)

            if item_index in self.items_to_extract:
                if item_section != '':
                    all_sections_null = False
                json_content[f'section_{item_index}'] = item_section

        if all_sections_null:
            json_content = None
            LOGGER.info(f'Could not extract any item for {absolute_10k_filename}')

        return json_content

    def process_filing(self, form10k_file):
        json_filename = f'{form10k_file.split(".")[0]}.json'
        absolute_json_filename = os.path.join(self.extracted_files_folder, json_filename)
        if os.path.exists(absolute_json_filename):
            return 0

        json_content = self.extract_items(form10k_file)

        if json_content is not None:
            with open(absolute_json_filename, 'w') as filepath:
                json.dump(json_content, filepath, indent=4)

        return 1


@cli.command()
@click.option('--raw_filings_folder', default='RAW_FILINGS')
@click.option('--extracted_filings_folder', default='EXTRACTED_FILINGS')
@click.option('--items_to_extract', default=['1', '1A', '1B', '2', '3', '4', '5', '6', '7', '7A',
                                             '8', '9', '9A', '9B', '10', '11', '12', '13', '14', '15'])
def main(
    raw_filings_folder: str,
    extracted_filings_folder: str,
    items_to_extract: List
):
    """
    Gets the list of 10K files and extracts all textual items/sections by calling the extract_items() function
    """

    raw_filings_folder = os.path.join(DATASET_DIR, raw_filings_folder)
    if not os.path.isdir(raw_filings_folder):
        LOGGER.info(f'No such directory: "{raw_filings_folder}')
        exit()

    extracted_filings_folder = os.path.join(DATASET_DIR, extracted_filings_folder)

    if not os.path.isdir(extracted_filings_folder):
        os.mkdir(extracted_filings_folder)

    extraction = ExtractItems(
        items_to_extract=items_to_extract,
        raw_files_folder=raw_filings_folder,
        extracted_files_folder=extracted_filings_folder
    )

    # Get list of 10K files
    list_of_files = [form10k_file for form10k_file in os.listdir(raw_filings_folder)
                     if form10k_file.endswith('.txt') or form10k_file.endswith('.htm') or form10k_file.endswith('.html')]

    LOGGER.info(f'Starting extraction...\n')

    with ProcessPool(processes=1) as pool:
        processed = list(tqdm(pool.imap(extraction.process_filing, list_of_files), total=len(list_of_files), ncols=100))

    LOGGER.info(f'\nItem extraction is completed successfully.')
    LOGGER.info(f'{sum(processed)} files were processed.')
    LOGGER.info(f'Extracted filings are saved to: {extracted_filings_folder}')


if __name__ == '__main__':
    main()
