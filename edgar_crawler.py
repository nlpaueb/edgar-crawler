import itertools
import json
import math
import os
import pandas as pd
import re
import requests
import tempfile
import zipfile

import logging

from bs4 import BeautifulSoup
from datetime import datetime
from logger import Logger
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, HTTPError, ConnectionError, Timeout, RetryError
from tqdm import tqdm
from typing import List
from urllib3.util import Retry

try:
	from html.parser.HTMLParser import HTMLParseError
except ImportError:  # Python 3.5+
	class HTMLParseError(Exception):
		pass

from __init__ import DATASET_DIR, LOGGING_DIR

urllib3_log = logging.getLogger("urllib3")
urllib3_log.setLevel(logging.CRITICAL)

# Instantiate a logger object
LOGGER = Logger(name=os.path.splitext(os.path.basename(os.path.abspath(__file__)))[0]).get_logger()
LOGGER.info(f'Saving log to {os.path.join(LOGGING_DIR)}\n')


def main():
	"""
	The main method iterates all over the tsv index files that are generated
	and calls a crawler method for each one of them.
	"""

	with open('config.json') as fin:
		config = json.load(fin)['edgar_crawler']

	raw_filings_folder = os.path.join(DATASET_DIR, config['raw_filings_folder'])
	indices_folder = os.path.join(DATASET_DIR, config['indices_folder'])
	filings_metadata_filepath = os.path.join(DATASET_DIR, config['filings_metadata_file'])

	if len(config['filing_types']) == 0:
		LOGGER.info(f'Please provide at least one filing type')
		exit()

	# If the indices and/or download folder doesn't exist, create them
	if not os.path.isdir(indices_folder):
		os.mkdir(indices_folder)
	if not os.path.isdir(raw_filings_folder):
		os.mkdir(raw_filings_folder)

	if not os.path.isfile(os.path.join(DATASET_DIR, 'companies_info.json')):
		with open(os.path.join(DATASET_DIR, 'companies_info.json'), 'w') as f:
			json.dump(obj={}, fp=f)

	download_indices(
		start_year=config['start_year'],
		end_year=config['end_year'],
		quarters=config['quarters'],
		skip_present_indices=config['skip_present_indices'],
		indices_folder=indices_folder,
		user_agent=config['user_agent']
	)

	# Filter out years that are not related
	tsv_filenames = []
	for year in range(config['start_year'], config['end_year'] + 1):
		for quarter in config['quarters']:
			filepath = os.path.join(indices_folder, f'{year}_QTR{quarter}.tsv')

			if os.path.isfile(filepath):
				tsv_filenames.append(filepath)

	# Get the indices that are specific to your needs
	df = get_specific_indices(
		tsv_filenames=tsv_filenames,
		filing_types=config['filing_types'],
		cik_tickers=config['cik_tickers'],
		user_agent=config['user_agent']
	)

	old_df = []
	if os.path.exists(filings_metadata_filepath):
		old_df = []
		series_to_download = []
		LOGGER.info(f'\nReading filings metadata...\n')

		for _, series in pd.read_csv(filings_metadata_filepath, dtype=str).iterrows():
			if os.path.exists(os.path.join(raw_filings_folder, series['filename'])):
				old_df.append((series.to_frame()).T)
		if len(old_df) == 1:
			old_df = old_df[0]
		elif len(old_df) > 1:
			old_df = pd.concat(old_df)

		for _, series in tqdm(df.iterrows(), total=len(df), ncols=100):
			if len(old_df) == 0 or len(old_df[old_df['html_index'] == series['html_index']]) == 0:
				series_to_download.append((series.to_frame()).T)

		if len(series_to_download) == 0:
			LOGGER.info(f'\nThere are no more filings to download for the given years, quarters and companies')
			exit()

		df = pd.concat(series_to_download) if (len(series_to_download) > 1) else series_to_download[0]

	# Make a list for each series of them
	list_of_series = []
	for i in range(len(df)):
		list_of_series.append(df.iloc[i])

	LOGGER.info(f'\nDownloading {len(df)} filings...\n')

	final_series = []
	for series in tqdm(list_of_series, ncols=100):
		series = crawl(
			series=series,
			filing_types=config['filing_types'],
			raw_filings_folder=raw_filings_folder,
			user_agent=config['user_agent']
		)
		if series is not None:
			final_series.append((series.to_frame()).T)
			final_df = pd.concat(final_series) if (len(final_series) > 1) else final_series[0]
			if len(old_df) > 0:
				final_df = pd.concat([old_df, final_df])
			final_df.to_csv(filings_metadata_filepath, index=False, header=True)

	LOGGER.info(f'\nFilings metadata exported to {filings_metadata_filepath}')

	if len(final_series) < len(list_of_series):
		LOGGER.info(
			f'\nDownloaded {len(final_series)} / {len(list_of_series)} filings. '
			f'Rerun the script to retry downloading the failed filings.'
		)


def download_indices(
		start_year: int,
		end_year: int,
		quarters: List,
		skip_present_indices: bool,
		indices_folder: str,
		user_agent: str
):
	base_url = "https://www.sec.gov/Archives/edgar/full-index/"

	LOGGER.info('Downloading EDGAR Index files')

	for quarter in quarters:
		if quarter not in [1, 2, 3, 4]:
			raise Exception(f'Invalid quarter "{quarter}"')

	first_iteration = True
	while True:
		failed_indices = []
		for year in range(start_year, end_year + 1):
			for quarter in quarters:
				if year == datetime.now().year and quarter > math.ceil(datetime.now().month / 3):
					break
				index_filename = f'{year}_QTR{quarter}.tsv'
				if skip_present_indices and os.path.exists(os.path.join(indices_folder, index_filename)):
					if first_iteration:
						LOGGER.info(f'Skipping {index_filename}')
					continue

				url = f'{base_url}/{year}/QTR{quarter}/master.zip'

				with tempfile.TemporaryFile(mode="w+b") as tmp:
					session = requests.Session()
					try:
						request = requests_retry_session(
							retries=5, backoff_factor=0.2, session=session
						).get(url=url, headers={'User-agent': user_agent})
					except requests.exceptions.RetryError as e:
						LOGGER.info(f'Failed downloading "{index_filename}" - {e}')
						failed_indices.append(index_filename)
						continue

					tmp.write(request.content)
					with zipfile.ZipFile(tmp).open("master.idx") as f:
						lines = [line.decode('latin-1') for line in itertools.islice(f, 11, None)]
						lines = [line.strip() + '|' + line.split('|')[-1].replace('.txt', '-index.html') for line in lines]

					with open(os.path.join(indices_folder, index_filename), 'w+', encoding='utf-8') as f:
						f.write(''.join(lines))
						LOGGER.info(f'{index_filename} downloaded')

		first_iteration = False
		if len(failed_indices) > 0:
			LOGGER.info(f'Could not download the following indices:\n{failed_indices}')
			user_input = input('Retry (Y/N): ')
			if user_input in ['Y', 'y', 'yes']:
				LOGGER.info(f'Retry downloading failed indices')
			else:
				break
		else:
			break


def get_specific_indices(
		tsv_filenames,
		filing_types,
		user_agent,
		cik_tickers=None,
):
	"""
	Loops through all the indexes and keeps only the rows/Series for the specific filing types
	:param tsv_filenames: the indices filenames
	:param filing_types: list of filing types to download. e.g. ['10-K', '10-K405', '10-KT']
	:param user_agent: the User-agent that will be declared to SEC EDGAR
	:param cik_tickers: list of CIKs or Tickers
	:return: a final dataframe which has Series only for the specific indices
	"""

	ciks = []

	if cik_tickers is not None:
		if isinstance(cik_tickers, str):
			if os.path.exists(cik_tickers) and os.path.isfile(cik_tickers):  # If filepath
				with open(cik_tickers) as f:
					cik_tickers = [line.strip() for line in f.readlines() if line.strip() != '']
			else:
				LOGGER.debug(f'Please provide a valid cik_ticker file path')
				exit()

	if isinstance(cik_tickers, List) and len(cik_tickers):
		company_tickers_url = 'https://www.sec.gov/files/company_tickers.json'

		session = requests.Session()
		try:
			request = requests_retry_session(
				retries=5, backoff_factor=0.2, session=session
			).get(url=company_tickers_url, headers={'User-agent': user_agent})
		except (RequestException, HTTPError, ConnectionError, Timeout, RetryError) as err:
			LOGGER.info(f'Failed downloading "{company_tickers_url}" - {err}')
			exit()

		company_tickers = json.loads(request.content)
		ticker2cik = {company['ticker']: company['cik_str'] for company in company_tickers.values()}
		ticker2cik = dict(sorted(ticker2cik.items(), key=lambda item: item[0]))

		for c_t in cik_tickers:
			if isinstance(c_t, int) or c_t.isdigit():  # If CIK
				ciks.append(str(c_t))
			else:
				if c_t in ticker2cik:
					ciks.append(str(ticker2cik[c_t]))  # If Ticker
				else:
					LOGGER.debug(f'Could not find CIK for "{c_t}"')

	dfs_list = []

	for filepath in tsv_filenames:

		# Load the index file
		df = pd.read_csv(
			filepath,
			sep='|',
			header=None,
			dtype=str,
			names=[
				'CIK', 'Company', 'Type', 'Date', 'complete_text_file_link', 'html_index',
				'Filing Date', 'Period of Report', 'SIC', 'htm_file_link',
				'State of Inc', 'State location', 'Fiscal Year End', 'filename'
			]
		)

		df['complete_text_file_link'] = 'https://www.sec.gov/Archives/' + df['complete_text_file_link'].astype(str)
		df['html_index'] = 'https://www.sec.gov/Archives/' + df['html_index'].astype(str)

		# Filter by filing type
		df = df[df.Type.isin(filing_types)]

		# Filter by CIK
		if len(ciks):
			df = df[(df.CIK.isin(ciks))]

		dfs_list.append(df)

	return pd.concat(dfs_list) if (len(dfs_list) > 1) else dfs_list[0]


def crawl(
		filing_types,
		series,
		raw_filings_folder,
		user_agent
):
	"""
	Crawls the EDGAR HTML indexes
	:param filing_types: list of filing types to download
	:param series: A single series with info for specific filings
	:param raw_filings_folder: Raw filings folder path
	:param user_agent: the User-agent that will be declared to SEC EDGAR
	:return: the .htm or .txt files
	"""

	html_index = series['html_index']

	# Create a BeautifulSoup instance using the 'lxml' parser
	try:
		retries_exceeded = True
		for _ in range(5):
			session = requests.Session()
			request = requests_retry_session(
				retries=5, backoff_factor=0.2, session=session
			).get(url=html_index, headers={'User-agent': user_agent})

			if 'will be managed until action is taken to declare your traffic.' not in request.text:
				retries_exceeded = False
				break

		if retries_exceeded:
			LOGGER.debug(f'Retries exceeded, could not download "{html_index}"')
			return None

	except (RequestException, HTTPError, ConnectionError, Timeout, RetryError) as err:
		LOGGER.debug(f'Request for {html_index} failed due to network-related error: {err}')
		return None

	soup = BeautifulSoup(request.content, 'lxml')

	# Crawl the soup and search it later for the Period of Report
	try:
		list_of_forms = soup.find_all('div', {'class': ['infoHead', 'info']})
	except (HTMLParseError, Exception) as e:
		list_of_forms = None

	period_of_report = None
	for form in list_of_forms:
		if form.attrs['class'][0] == 'infoHead' and form.text == 'Filing Date':
			series['Filing Date'] = form.nextSibling.nextSibling.text

		if form.attrs['class'][0] == 'infoHead' and form.text == 'Period of Report':
			period_of_report = form.nextSibling.nextSibling.text
			series['Period of Report'] = period_of_report

	if period_of_report is None:
		LOGGER.debug(f'Can not crawl "Period of Report" for {html_index}')
		return None

	# Assign metadata to dataframe
	try:
		company_info = soup.find('div', {'class': ['companyInfo']}).find('p', {'class': ['identInfo']}).text
	except (HTMLParseError, Exception) as e:
		company_info = None

	try:
		for info in company_info.split('|'):
			info_splits = info.split(':')
			if info_splits[0].strip() in ['State of Incorp.', 'State of Inc.', 'State of Incorporation.']:
				series['State of Inc'] = info_splits[1].strip()
			if info_splits[0].strip() == ['State location']:
				series['State location'] = info_splits[1].strip()
	except (ValueError, Exception) as e:
		pass

	fiscal_year_end_regex = re.search(r'Fiscal Year End: *(\d{4})', company_info)
	if fiscal_year_end_regex is not None:
		series['Fiscal Year End'] = fiscal_year_end_regex.group(1)

	# Crawl for the Sector Industry Code (SIC)
	try:
		sic = soup.select_one('.identInfo a[href*="SIC"]')
		if sic is not None:
			series['SIC'] = sic.text
	except (HTMLParseError, Exception) as e:
		pass

	# https://www.sec.gov/cgi-bin/browse-edgar?CIK=0001000228
	# https://data.sec.gov/submissions/CIK0001000228.json
	with open(os.path.join(DATASET_DIR, 'companies_info.json')) as f:
		company_info_dict = json.load(fp=f)

	cik = series['CIK']
	if cik not in company_info_dict:
		company_url = f"https://www.sec.gov/cgi-bin/browse-edgar?CIK={cik}"
		try:
			retries_exceeded = True
			for _ in range(5):
				session = requests.Session()
				request = requests_retry_session(
					retries=5, backoff_factor=0.2, session=session
				).get(url=company_url, headers={'User-agent': user_agent})

				if 'will be managed until action is taken to declare your traffic.' not in request.text:
					retries_exceeded = False
					break

			if retries_exceeded:
				LOGGER.debug(f'Retries exceeded, could not download "{company_url}"')
				return None

		except (RequestException, HTTPError, ConnectionError, Timeout, RetryError) as err:
			LOGGER.debug(f'Request for {company_url} failed due to network-related error: {err}')
			return None

		company_info_dict[cik] = {
			'Company Name': None,
			'SIC': None,
			'State location': None,
			'State of Inc': None,
			'Fiscal Year End': None
		}
		company_info_soup = BeautifulSoup(request.content, 'lxml')

		company_info = company_info_soup.find('div', {'class': ['companyInfo']})
		if company_info is not None:
			company_info_dict[cik]['Company Name'] = str(company_info.find('span', {'class': ['companyName']}).contents[0]).strip()
			company_info_contents = company_info.find('p', {'class': ['identInfo']}).contents

			for idx, content in enumerate(company_info_contents):
				if ';SIC=' in str(content):
					company_info_dict[cik]['SIC'] = content.text
				if ';State=' in str(content):
					company_info_dict[cik]['State location'] = content.text
				if 'State of Inc' in str(content):
					company_info_dict[cik]['State of Inc'] = company_info_contents[idx + 1].text
				if 'Fiscal Year End' in str(content):
					company_info_dict[cik]['Fiscal Year End'] = str(content).split()[-1]

		with open(os.path.join(DATASET_DIR, 'companies_info.json'), 'w') as f:
			json.dump(obj=company_info_dict, fp=f, indent=4)

	if pd.isna(series['SIC']):
		series['SIC'] = company_info_dict[cik]['SIC']
	if pd.isna(series['State of Inc']):
		series['State of Inc'] = company_info_dict[cik]['State of Inc']
	if pd.isna(series['State location']):
		series['State location'] = company_info_dict[cik]['State location']
	if pd.isna(series['Fiscal Year End']):
		series['Fiscal Year End'] = company_info_dict[cik]['Fiscal Year End']

	# Crawl the soup for the financial files
	try:
		all_tables = soup.find_all('table')
	except (HTMLParseError, Exception) as e:
		return None

	'''
	Tables are of 2 kinds. 
	The 'Document Format Files' table contains all the htms, jpgs, pngs and txts for the reports.
	The 'Data Format Files' table contains all the xml instances that contain structured information.
	'''
	for table in all_tables:

		# Get the htm/html/txt files
		if table.attrs['summary'] == 'Document Format Files':
			htm_file_link, complete_text_file_link, link_to_download = None, None, None
			filing_type = None

			for tr in table.find_all('tr')[1:]:
				# If it's the specific document type (e.g. 10-K)
				if tr.contents[7].text in filing_types:
					filing_type = tr.contents[7].text
					if tr.contents[5].contents[0].attrs['href'].split('.')[-1] in ['htm', 'html']:
						htm_file_link = 'https://www.sec.gov' + tr.contents[5].contents[0].attrs['href']
						series['htm_file_link'] = str(htm_file_link)
						break

				# Else get the complete submission text file
				elif tr.contents[3].text == 'Complete submission text file':
					filing_type = series['Type']
					complete_text_file_link = 'https://www.sec.gov' + tr.contents[5].contents[0].attrs['href']
					series['complete_text_file_link'] = str(complete_text_file_link)
					break

			if htm_file_link is not None:
				# In case of iXBRL documents, a slight URL modification is required
				if 'ix?doc=/' in htm_file_link:
					link_to_download = htm_file_link.replace('ix?doc=/', '')
					series['htm_file_link'] = link_to_download
					file_extension = "htm"
				else:
					link_to_download = htm_file_link
					file_extension = htm_file_link.split('.')[-1]

			elif complete_text_file_link is not None:
				link_to_download = complete_text_file_link
				file_extension = link_to_download.split('.')[-1]

			if link_to_download is not None:
				filing_type = re.sub(r"[\-/\\]", '', filing_type)
				accession_num = os.path.splitext(os.path.basename(series['complete_text_file_link']))[0]
				filename = f"{str(series['CIK'])}_{filing_type}_{period_of_report[:4]}_{accession_num}.{file_extension}"

				# Download the file
				success = download(
					url=link_to_download,
					filename=filename,
					download_folder=raw_filings_folder,
					user_agent=user_agent
				)
				if success:
					series['filename'] = filename
				else:
					return None
			else:
				return None

	return series


def download(
		url,
		filename,
		download_folder,
		user_agent
):
	"""
	Downloads the filing to the specified directory with the naming convention below:
	<CIK-KEY_YEAR_FILING-TYPE.EXTENSION_TYPE> (e.g.: 1000229_2018_10K.html)
	:param url: The URL to download
	:param filename: The Central Index Key (CIK) of the company
	:param download_folder:
	:param user_agent: the User-agent that will be declared to SEC EDGAR

	Note that we save files based on the years that they report to
	Most companies submit their reports on the end of December of the current year (2021 for example)
	However, if a company submits its report on the start of the next year (2022), then
	this will be saved as COMPANY_CIK_FILING-TYPE_2022.htm
	"""

	filepath = os.path.join(download_folder, filename)

	try:
		retries_exceeded = True
		for _ in range(5):
			session = requests.Session()
			request = requests_retry_session(
				retries=5, backoff_factor=0.2, session=session
			).get(url=url, headers={'User-agent': user_agent})
			# request = requests.get(html_index, headers={'User-Agent': ua.random})

			if 'will be managed until action is taken to declare your traffic.' not in request.text:
				retries_exceeded = False
				break

		if retries_exceeded:
			LOGGER.debug(f'Retries exceeded, could not download "{filename}" - "{url}"')
			return False

	except (RequestException, HTTPError, ConnectionError, Timeout, RetryError) as err:
		LOGGER.debug(f'Request for {url} failed due to network-related error: {err}')
		return False

	with open(filepath, 'wb') as f:
		f.write(request.content)

	# Check that MD5 hash is correct
	# if hashlib.md5(open(filepath, 'rb').read()).hexdigest() != headers._headers[1][1].strip('"'):
	# 	LOGGER.info(f'Wrong MD5 hash for file: {abs_filename} - {url}')

	return True


def requests_retry_session(
		retries=5,
		backoff_factor=0.5,
		status_forcelist=(400, 401, 403, 500, 502, 503, 504, 505),
		session=None
):
	"""
	Retries the HTTP GET method in case of some specific HTTP errors.

	:param retries: Time of retries
	:param backoff_factor: The amount of delay after each retry
	:param status_forcelist: The error codes that the script should retry; Otherwise, it won't retry
	:param session: the requests session
	:return: the new session
	"""
	session = session or requests.Session()
	retry = Retry(
		total=retries,
		read=retries,
		connect=retries,
		backoff_factor=backoff_factor,
		status_forcelist=status_forcelist,
	)
	adapter = HTTPAdapter(max_retries=retry)
	session.mount('http://', adapter)
	session.mount('https://', adapter)
	return session


if __name__ == '__main__':
	main()
