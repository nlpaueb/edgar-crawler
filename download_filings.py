import itertools
import json
import logging
import math
import os
import re
import shutil
import tempfile
import zipfile
from datetime import datetime
from typing import List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.exceptions import (
    ConnectionError,
    HTTPError,
    RequestException,
    RetryError,
    Timeout,
)
from tqdm import tqdm
from urllib3.util import Retry

from logger import Logger

# Python version compatibility for HTML parser
try:
    from html.parser.HTMLParser import HTMLParseError
except ImportError:  # Python 3.5+

    class HTMLParseError(Exception):
        pass


# Import constants from the project's __init__ file
from __init__ import DATASET_DIR, LOGGING_DIR

# Set urllib3 logging level to critical to reduce noise
urllib3_log = logging.getLogger("urllib3")
urllib3_log.setLevel(logging.CRITICAL)

# Instantiate a logger object to use for logging messages throughout this module
LOGGER = Logger(
    name=os.path.splitext(os.path.basename(os.path.abspath(__file__)))[0]
).get_logger()

# Log where the logs are being saved
LOGGER.info(f"Saving log to {os.path.join(LOGGING_DIR)}\n")


def main():
    """
    Orchestrates the entire flow of crawling and downloading filings from SEC EDGAR.

    This function performs the following steps:
    1. Loads the configuration file.
    2. Creates necessary directories.
    3. Filters out the unnecessary years.
    4. Downloads the indices.
    5. Gets specific indices according to the provided filing types and CIKs/tickers.
    6. Compares the new indices with the old ones to download only the new filings.
    7. Crawls through each index to download (.tsv files) and save the filing.

    Raises:
            SystemExit: If no filing types are provided or if there are no new filings to download.
    """

    # Load the configuration file
    with open("config.json") as fin:
        config = json.load(fin)["download_filings"]

    # Define the directories and filepaths
    raw_filings_folder = os.path.join(DATASET_DIR, config["raw_filings_folder"])
    indices_folder = os.path.join(DATASET_DIR, config["indices_folder"])
    filings_metadata_filepath = os.path.join(
        DATASET_DIR, config["filings_metadata_file"]
    )

    # Check if at least one filing type is provided
    if len(config["filing_types"]) == 0:
        LOGGER.info("Please provide at least one filing type")
        exit()

    # If the indices and/or download folder doesn't exist, create them
    if not os.path.isdir(indices_folder):
        os.mkdir(indices_folder)
    if not os.path.isdir(raw_filings_folder):
        os.mkdir(raw_filings_folder)

    # We also create subfolders for each filing type in the raw_filings_folder for better organization
    for filing_type in config["filing_types"]:
        filing_type_folder = os.path.join(raw_filings_folder, filing_type)
        if not os.path.isdir(filing_type_folder):
            os.mkdir(filing_type_folder)

    # If companies_info.json doesn't exist, create it with empty JSON
    if not os.path.isfile(os.path.join(DATASET_DIR, "companies_info.json")):
        with open(os.path.join(DATASET_DIR, "companies_info.json"), "w") as f:
            json.dump(obj={}, fp=f)

    # Download the indices for the given years and quarters
    download_indices(
        start_year=config["start_year"],
        end_year=config["end_year"],
        quarters=config["quarters"],
        skip_present_indices=config["skip_present_indices"],
        indices_folder=indices_folder,
        user_agent=config["user_agent"],
    )

    # Filter out the indices of years that are not in the provided range
    tsv_filenames = []
    for year in range(config["start_year"], config["end_year"] + 1):
        for quarter in config["quarters"]:
            filepath = os.path.join(indices_folder, f"{year}_QTR{quarter}.tsv")

            if os.path.isfile(filepath):
                tsv_filenames.append(filepath)

    # Get the indices that are specific to your needs
    df = get_specific_indices(
        tsv_filenames=tsv_filenames,
        filing_types=config["filing_types"],
        cik_tickers=config["cik_tickers"],
        user_agent=config["user_agent"],
    )

    # Initialize list for old filings metadata
    old_df = []
    if os.path.exists(filings_metadata_filepath):
        # Initialize list for the filings to be downloaded
        series_to_download = []
        LOGGER.info("\nReading filings metadata...\n")

        # Read the old filings metadata and filter out the filings that already exist in the download folder
        for _, series in pd.read_csv(filings_metadata_filepath, dtype=str).iterrows():
            if os.path.exists(
                os.path.join(raw_filings_folder, series["Type"], series["filename"])
            ):
                old_df.append((series.to_frame()).T)

        # Concatenate the old filings metadata
        if len(old_df) == 1:
            old_df = old_df[0]
        elif len(old_df) > 1:
            old_df = pd.concat(old_df)

        # Check if each filing in the new indices already exists in the old metadata
        # If it doesn't, add it to the list of filings to be downloaded
        for _, series in tqdm(df.iterrows(), total=len(df), ncols=100):
            if (
                len(old_df) == 0
                or len(old_df[old_df["html_index"] == series["html_index"]]) == 0
            ):
                series_to_download.append((series.to_frame()).T)

        # If there are no new filings to download, exit
        if len(series_to_download) == 0:
            LOGGER.info(
                "\nThere are no more filings to download for the given years, quarters and companies"
            )
            exit()

        # Concatenate the series to be downloaded
        df = (
            pd.concat(series_to_download)
            if (len(series_to_download) > 1)
            else series_to_download[0]
        )

    # Create a list for each series in the dataframe
    list_of_series = []
    for i in range(len(df)):
        list_of_series.append(df.iloc[i])

    LOGGER.info(f"\nDownloading {len(df)} filings directly from EDGAR...\n")

    # Initialize list for final series
    final_series = []
    for series in tqdm(list_of_series, ncols=100):
        # Crawl each series to download and save the filing
        series = crawl(
            series=series,
            filing_types=config["filing_types"],
            raw_filings_folder=raw_filings_folder,
            user_agent=config["user_agent"],
        )

        # If the series was successfully downloaded, append it to the final series
        if series is not None:
            final_series.append((series.to_frame()).T)
            # Concatenate the final series and export it to the metadata file
            final_df = (
                pd.concat(final_series) if (len(final_series) > 1) else final_series[0]
            )
            if len(old_df) > 0:
                final_df = pd.concat([old_df, final_df])

            # Write to a temporary file first, in order to avoid possible data loss (issue #19)
            temp_filepath = f"{filings_metadata_filepath}.tmp"
            try:
                final_df.to_csv(temp_filepath, index=False, header=True)

                # Move the temporary file to the final file
                shutil.move(temp_filepath, filings_metadata_filepath)
            except KeyboardInterrupt:
                final_df.to_csv(temp_filepath, index=False, header=True)
                shutil.move(temp_filepath, filings_metadata_filepath)
                LOGGER.info(
                    f"Keyboard interrupt by the user detected (Ctrl + C). Saving filings metadata to {filings_metadata_filepath} and exiting."
                )
                exit(0)

    LOGGER.info(f"\nFilings metadata exported to {filings_metadata_filepath}")
    # If some filings failed to download, notify to rerun the script
    if len(final_series) < len(list_of_series):
        LOGGER.info(
            f"\nDownloaded {len(final_series)} / {len(list_of_series)} filings. "
            f"Rerun the script to retry downloading the failed filings."
        )


def download_indices(
    start_year: int,
    end_year: int,
    quarters: List[str],
    skip_present_indices: bool,
    indices_folder: str,
    user_agent: str,
) -> None:
    """
    Downloads EDGAR Index files for the specified years and quarters.

    Args:
            start_year (int): The first year of the indices to be downloaded.
            end_year (int): The last year of the indices to be downloaded.
            quarters (List[str]): A list of quarters (in the format 'Q1', 'Q2', etc.) for which the indices will be downloaded.
            skip_present_indices (bool): If True, the function will skip downloading indices that are already present in the directory.
            indices_folder (str): Directory where the indices will be saved.
            user_agent (str): The User-Agent string that will be declared to SEC EDGAR.

    Raises:
            ValueError: If an invalid quarter is passed.
    """

    base_url = "https://www.sec.gov/Archives/edgar/full-index/"

    LOGGER.info("Downloading index files from SEC...")

    # Validate quarters
    for quarter in quarters:
        if quarter not in [1, 2, 3, 4]:
            raise Exception(f'Invalid quarter "{quarter}"')

    first_iteration = True
    # Loop over the years and quarters to download the indices
    while True:
        failed_indices = []
        for year in range(start_year, end_year + 1):
            for quarter in quarters:
                if year == datetime.now().year and quarter > math.ceil(
                    datetime.now().month / 3
                ):  # Skip future quarters
                    break

                index_filename = f"{year}_QTR{quarter}.tsv"

                # Check if the index file is already present
                if skip_present_indices and os.path.exists(
                    os.path.join(indices_folder, index_filename)
                ):
                    if first_iteration:
                        LOGGER.info(f"Skipping {index_filename}")
                    continue

                # If not, download the index file
                url = f"{base_url}/{year}/QTR{quarter}/master.zip"

                # Retry the download in case of failures
                with tempfile.TemporaryFile(mode="w+b") as tmp:
                    session = requests.Session()
                    try:
                        request = requests_retry_session(
                            retries=5, backoff_factor=0.2, session=session
                        ).get(url=url, headers={"User-agent": user_agent})
                    except requests.exceptions.RetryError as e:
                        LOGGER.info(f'Failed downloading "{index_filename}" - {e}')
                        failed_indices.append(index_filename)
                        continue

                    tmp.write(request.content)

                    # Process the downloaded index file
                    with zipfile.ZipFile(tmp).open("master.idx") as f:
                        lines = [
                            line.decode("latin-1")
                            for line in itertools.islice(f, 11, None)
                        ]
                        lines = [
                            line.strip()
                            + "|"
                            + line.split("|")[-1].replace(".txt", "-index.html")
                            for line in lines
                        ]

                    # Save the processed index file
                    with open(
                        os.path.join(indices_folder, index_filename),
                        "w+",
                        encoding="utf-8",
                    ) as f:
                        f.write("".join(lines))
                        LOGGER.info(f"{index_filename} downloaded")

        first_iteration = False
        # Handle failed downloads
        if len(failed_indices) > 0:
            LOGGER.info(f"Could not download the following indices:\n{failed_indices}")
            user_input = input("Retry (Y/N): ")
            if user_input in ["Y", "y", "yes"]:
                LOGGER.info("Retry downloading failed indices")
            else:
                break
        else:
            break


def get_specific_indices(
    tsv_filenames: List[str],
    filing_types: List[str],
    user_agent: str,
    cik_tickers: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Loops through all the indexes and keeps only the rows/Series for the specific filing types.

    Args:
            tsv_filenames (List[str]): The filenames of the indices.
            filing_types (List[str]): The filing types to download, e.g., ['10-K', '8-K'].
            user_agent (str): The User-Agent string that will be declared to SEC EDGAR.
            cik_tickers (Optional[List[str]]): List of CIKs or Tickers. If None, the function processes all CIKs in the provided indices.

    Returns:
            pd.DataFrame: A dataframe which contains series only for the specific indices.
    """

    # Initialize list for CIKs
    ciks = []

    # If cik_tickers is provided
    if cik_tickers is not None:
        # Check if the cik_tickers is a file path
        if isinstance(cik_tickers, str):
            if os.path.exists(cik_tickers) and os.path.isfile(cik_tickers):
                # If it is a valid filepath, load the CIKs or tickers
                with open(cik_tickers) as f:
                    cik_tickers = [
                        line.strip() for line in f.readlines() if line.strip() != ""
                    ]
            else:
                # If it is not a valid filepath, log the error and exit
                LOGGER.error("Please provide a valid cik_ticker file path")
                exit()

    # Check if cik_tickers is a list and not empty
    if isinstance(cik_tickers, List) and len(cik_tickers):
        # Define the company_tickers_url
        company_tickers_url = "https://www.sec.gov/files/company_tickers.json"

        # Initialize a session for requests
        session = requests.Session()
        try:
            # Try to download the company_tickers data
            request = requests_retry_session(
                retries=5, backoff_factor=0.2, session=session
            ).get(url=company_tickers_url, headers={"User-agent": user_agent})
        except (
            RequestException,
            HTTPError,
            ConnectionError,
            Timeout,
            RetryError,
        ) as err:
            # If download fails, log the error and exit
            LOGGER.info(f'Failed downloading "{company_tickers_url}" - {err}')
            exit()

        # Load the company tickers data
        company_tickers = json.loads(request.content)

        # Create a mapping from ticker to CIK
        ticker2cik = {
            company["ticker"]: company["cik_str"]
            for company in company_tickers.values()
        }
        ticker2cik = dict(sorted(ticker2cik.items(), key=lambda item: item[0]))

        # Convert all tickers in the cik_tickers list to CIKs
        for c_t in cik_tickers:
            if isinstance(c_t, int) or c_t.isdigit():  # If it is a CIK
                ciks.append(str(c_t))
            else:  # If it is a ticker
                if c_t in ticker2cik:
                    # If the ticker exists in the mapping, convert it to CIK
                    ciks.append(str(ticker2cik[c_t]))
                else:
                    # If the ticker does not exist in the mapping, log the error
                    LOGGER.debug(f'Could not find CIK for ticker "{c_t}"')

    # Initialize list for dataframes
    dfs_list = []

    # For each file in the provided filenames
    for filepath in tsv_filenames:
        # Load the index file into a dataframe
        df = pd.read_csv(
            filepath,
            sep="|",
            header=None,
            dtype=str,
            names=[
                "CIK",
                "Company",
                "Type",
                "Date",
                "complete_text_file_link",
                "html_index",
                "Filing Date",
                "Period of Report",
                "SIC",
                "htm_file_link",
                "State of Inc",
                "State location",
                "Fiscal Year End",
                "filename",
            ],
        )

        # Prepend the URL for SEC Archives to the links
        df["complete_text_file_link"] = "https://www.sec.gov/Archives/" + df[
            "complete_text_file_link"
        ].astype(str)
        df["html_index"] = "https://www.sec.gov/Archives/" + df["html_index"].astype(
            str
        )

        # Filter the dataframe by filing type
        df = df[df.Type.isin(filing_types)]

        # If CIKs were provided, filter the dataframe by CIK
        if len(ciks):
            df = df[(df.CIK.isin(ciks))]

        # Add the filtered dataframe to the list
        dfs_list.append(df)

    # Return the concatenated dataframe if there are multiple dataframes in the list, else return the single dataframe
    return pd.concat(dfs_list) if (len(dfs_list) > 1) else dfs_list[0]


def crawl(
    filing_types: List[str], series: pd.Series, raw_filings_folder: str, user_agent: str
) -> pd.Series:
    """
    Crawls the EDGAR HTML indexes and extracts required details.

    Such details include the Filing Date, the Period of Report, the State location, the Fiscal Year End, and many more.

    Args:
            filing_types (List[str]): List of filing types to download.
            series (pd.Series): A single series with info for specific filings.
            raw_filings_folder (str): Raw filings folder path.
            user_agent (str): The User-agent string that will be declared to SEC EDGAR.

    Returns:
            pd.Series: The series with the extracted data.
    """

    html_index = series["html_index"]

    # Retries for making the request if not successful at first attempt
    try:
        # Exponential backoff retry logic
        retries_exceeded = True
        for _ in range(5):
            session = requests.Session()
            request = requests_retry_session(
                retries=5, backoff_factor=0.2, session=session
            ).get(url=html_index, headers={"User-agent": user_agent})

            if (
                "will be managed until action is taken to declare your traffic."
                not in request.text
            ):
                retries_exceeded = False
                break

        if retries_exceeded:
            LOGGER.debug(f'Retries exceeded, could not download "{html_index}"')
            return None

    except (RequestException, HTTPError, ConnectionError, Timeout, RetryError) as err:
        LOGGER.debug(
            f"Request for {html_index} failed due to network-related error: {err}"
        )
        return None

    soup = BeautifulSoup(request.content, "lxml")

    # Parsing HTML to extract required details
    try:
        list_of_forms = soup.find_all("div", {"class": ["infoHead", "info"]})
    except (HTMLParseError, Exception):
        list_of_forms = None

    # Extraction of 'Filing Date' and 'Period of Report'
    period_of_report = None
    for form in list_of_forms:
        if form.attrs["class"][0] == "infoHead" and form.text == "Filing Date":
            series["Filing Date"] = form.nextSibling.nextSibling.text

        if form.attrs["class"][0] == "infoHead" and form.text == "Period of Report":
            period_of_report = form.nextSibling.nextSibling.text
            series["Period of Report"] = period_of_report

    if period_of_report is None:
        LOGGER.debug(f'Can not crawl "Period of Report" for {html_index}')
        return None

    # Extracting the company info
    try:
        company_info = (
            soup.find("div", {"class": ["companyInfo"]})
            .find("p", {"class": ["identInfo"]})
            .text
        )
    except (HTMLParseError, Exception):
        company_info = None

    # Parsing company info to extract details like 'State of Incorporation', 'State location'
    try:
        for info in company_info.split("|"):
            info_splits = info.split(":")
            if info_splits[0].strip() in [
                "State of Incorp.",
                "State of Inc.",
                "State of Incorporation.",
            ]:
                series["State of Inc"] = info_splits[1].strip()
            if info_splits[0].strip() == ["State location"]:
                series["State location"] = info_splits[1].strip()
    except (ValueError, Exception):
        pass

    # Extracting 'Fiscal Year End'
    fiscal_year_end_regex = re.search(r"Fiscal Year End: *(\d{4})", company_info)
    if fiscal_year_end_regex is not None:
        series["Fiscal Year End"] = fiscal_year_end_regex.group(1)

    # Crawl for the Sector Industry Code (SIC)
    try:
        sic = soup.select_one('.identInfo a[href*="SIC"]')
        if sic is not None:
            series["SIC"] = sic.text
    except (HTMLParseError, Exception):
        pass

    # Loading previously stored companies info
    with open(os.path.join(DATASET_DIR, "companies_info.json")) as f:
        company_info_dict = json.load(fp=f)

    # Ensuring info of current company is in the companies info dictionary
    cik = series["CIK"]
    if cik not in company_info_dict:
        company_url = f"https://www.sec.gov/cgi-bin/browse-edgar?CIK={cik}"

        # Similar retry logic for fetching the company info
        try:
            retries_exceeded = True
            for _ in range(5):
                session = requests.Session()
                request = requests_retry_session(
                    retries=5, backoff_factor=0.2, session=session
                ).get(url=company_url, headers={"User-agent": user_agent})

                if (
                    "will be managed until action is taken to declare your traffic."
                    not in request.text
                ):
                    retries_exceeded = False
                    break

            if retries_exceeded:
                LOGGER.debug(f'Retries exceeded, could not download "{company_url}"')
                return None

        except (
            RequestException,
            HTTPError,
            ConnectionError,
            Timeout,
            RetryError,
        ) as err:
            LOGGER.debug(
                f"Request for {company_url} failed due to network-related error: {err}"
            )
            return None

        # Storing the extracted company info into the dictionary
        company_info_dict[cik] = {
            "Company Name": None,
            "SIC": None,
            "State location": None,
            "State of Inc": None,
            "Fiscal Year End": None,
        }
        company_info_soup = BeautifulSoup(request.content, "lxml")

        # Parsing the company_info_soup to extract required details
        company_info = company_info_soup.find("div", {"class": ["companyInfo"]})
        if company_info is not None:
            company_info_dict[cik]["Company Name"] = str(
                company_info.find("span", {"class": ["companyName"]}).contents[0]
            ).strip()
            company_info_contents = company_info.find(
                "p", {"class": ["identInfo"]}
            ).contents

            for idx, content in enumerate(company_info_contents):
                if ";SIC=" in str(content):
                    company_info_dict[cik]["SIC"] = content.text
                if ";State=" in str(content):
                    company_info_dict[cik]["State location"] = content.text
                if "State of Inc" in str(content):
                    company_info_dict[cik]["State of Inc"] = company_info_contents[
                        idx + 1
                    ].text
                if "Fiscal Year End" in str(content):
                    company_info_dict[cik]["Fiscal Year End"] = str(content).split()[-1]

        # Updating the json file with the latest data
        with open(os.path.join(DATASET_DIR, "companies_info.json"), "w") as f:
            json.dump(obj=company_info_dict, fp=f, indent=4)

    # Filling series data with information from company_info_dict if they are missing in the series
    if pd.isna(series["SIC"]):
        series["SIC"] = company_info_dict[cik]["SIC"]
    if pd.isna(series["State of Inc"]):
        series["State of Inc"] = company_info_dict[cik]["State of Inc"]
    if pd.isna(series["State location"]):
        series["State location"] = company_info_dict[cik]["State location"]
    if pd.isna(series["Fiscal Year End"]):
        series["Fiscal Year End"] = company_info_dict[cik]["Fiscal Year End"]

    # Crawl the soup for the financial files
    try:
        all_tables = soup.find_all("table")
    except (HTMLParseError, Exception):
        return None

    """
    Tables are of 2 kinds. 
    The 'Document Format Files' table contains all the htms, jpgs, pngs and txts for the reports.
    The 'Data Format Files' table contains all the xml instances that contain structured information.
    """
    for table in all_tables:
        # Get the htm/html/txt files
        if table.attrs["summary"] == "Document Format Files":
            htm_file_link, complete_text_file_link, link_to_download = None, None, None
            filing_type = None

            # Iterate through rows to identify required links
            for tr in table.find_all("tr")[1:]:
                # If it's the specific document type (e.g. 10-K)
                if tr.contents[7].text in filing_types:
                    filing_type = tr.contents[7].text
                    if tr.contents[5].contents[0].attrs["href"].split(".")[-1] in [
                        "htm",
                        "html",
                    ]:
                        htm_file_link = (
                            "https://www.sec.gov"
                            + tr.contents[5].contents[0].attrs["href"]
                        )
                        series["htm_file_link"] = str(htm_file_link)
                        break

                # Else get the complete submission text file
                elif tr.contents[3].text == "Complete submission text file":
                    filing_type = series["Type"]
                    complete_text_file_link = (
                        "https://www.sec.gov" + tr.contents[5].contents[0].attrs["href"]
                    )
                    series["complete_text_file_link"] = str(complete_text_file_link)
                    break

            # Prepare final link to download
            if htm_file_link is not None:
                # In case of iXBRL documents, a slight URL modification is required
                if "ix?doc=/" in htm_file_link:
                    link_to_download = htm_file_link.replace("ix?doc=/", "")
                    series["htm_file_link"] = link_to_download
                    file_extension = "htm"
                else:
                    link_to_download = htm_file_link
                    file_extension = htm_file_link.split(".")[-1]

            elif complete_text_file_link is not None:
                link_to_download = complete_text_file_link
                file_extension = link_to_download.split(".")[-1]

            # If a valid link is available, initiate download
            if link_to_download is not None:
                # In the filename, we remove any special characters from the filing type
                filing_type_name = re.sub(r"[\-/\\]", "", filing_type)
                accession_num = os.path.splitext(
                    os.path.basename(series["complete_text_file_link"])
                )[0]
                filename = f"{str(series['CIK'])}_{filing_type_name}_{period_of_report[:4]}_{accession_num}.{file_extension}"

                # Download the file
                success = download(
                    url=link_to_download,
                    filename=filename,
                    download_folder=os.path.join(raw_filings_folder, filing_type),
                    user_agent=user_agent,
                )
                if success:
                    series["filename"] = filename
                else:
                    return None
            else:
                return None

    return series


def download(url: str, filename: str, download_folder: str, user_agent: str) -> bool:
    """
    Downloads a file from the given URL and saves it to the specified directory.

    The downloaded file will be named according to the following convention:
    <CIK-KEY_YEAR_FILING-TYPE.EXTENSION_TYPE> (e.g., 1000229_2018_10K.html).

    Args:
            url (str): The URL of the file to download.
            filename (str): The name to give to the downloaded file. This should include the file extension.
            download_folder (str): The directory to save the downloaded file in.
            user_agent (str): The User-Agent string to use when making the request.

    Returns:
            bool: True if the download was successful, False otherwise.
    """

    # Create the full file path
    filepath = os.path.join(download_folder, filename)

    try:
        # Initialize a flag to track if retries are exceeded
        retries_exceeded = True

        # Attempt to download the file up to 5 times
        for _ in range(5):
            # Create a new requests session
            session = requests.Session()

            # Make a GET request to the URL with retries and backoff
            request = requests_retry_session(
                retries=5, backoff_factor=0.2, session=session
            ).get(url=url, headers={"User-agent": user_agent})

            # If the response does not contain a specific error message, break the loop
            if (
                "will be managed until action is taken to declare your traffic."
                not in request.text
            ):
                retries_exceeded = False
                break

        # If retries are exceeded, log a debug message and return False
        if retries_exceeded:
            LOGGER.debug(f'Retries exceeded, could not download "{filename}" - "{url}"')
            return False

    except (RequestException, HTTPError, ConnectionError, Timeout, RetryError) as err:
        # If a network-related error occurs, log a debug message and return False
        LOGGER.debug(f"Request for {url} failed due to network-related error: {err}")
        return False

    # If the download was successful, save the file
    with open(filepath, "wb") as f:
        f.write(request.content)

    # Uncomment the following lines to check the MD5 hash of the downloaded file
    # if hashlib.md5(open(filepath, 'rb').read()).hexdigest() != headers._headers[1][1].strip('"'):
    # 	LOGGER.info(f'Wrong MD5 hash for file: {abs_filename} - {url}')

    # If the function has not returned False by this point, the download was successful
    return True


def requests_retry_session(
    retries: int = 5,
    backoff_factor: float = 0.5,
    status_forcelist: tuple = (400, 401, 403, 500, 502, 503, 504, 505),
    session: requests.Session = None,
) -> requests.Session:
    """
    Creates a new requests session that automatically retries failed requests.

    Args:
            retries (int): The number of times to retry a failed request. Default is 5.
            backoff_factor (float): The delay factor to apply between retry attempts. Default is 0.5.
            status_forcelist (tuple): A tuple of HTTP status codes that should force a retry.
                    A retry is initiated if the HTTP status code of the response is in this list.
                    Default is a tuple of common server error codes.
            session (requests.Session): An existing requests session to use. If not provided, a new session will be created.

    Returns:
            requests.Session: A requests session configured with retry behavior.
    """

    # If no session provided, create a new one
    session = session or requests.Session()

    # Create a Retry object
    # It will specify how many times to retry a failed request and what HTTP status codes should force a retry
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )

    # Create an HTTPAdapter with the Retry object
    # HTTPAdapter is a built-in requests Adapter that sends HTTP requests
    adapter = HTTPAdapter(max_retries=retry)

    # Mount the HTTPAdapter to the session for both HTTP and HTTPS requests
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Return the session
    return session


if __name__ == "__main__":
    main()
