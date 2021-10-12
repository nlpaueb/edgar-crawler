# EDGAR-CRAWLER
Crawl and fetch all publicly-traded companies annual reports from [SEC's EDGAR](https://www.sec.gov/edgar.shtml) database.

`edgar-crawler` is a an optimized toolkit that retrieves textual information from financial reports, such as 10-K, 10-Q or 8-K filings.

More specifically, it can:
- Crawl and download financial reports for each publicly-traded company, for specified years 
- Extract and clean specific text sections, such as Risk Factors, MD&A, and others. **Currently, we only support extraction of 10-K filings (i.e., annual reports).**

The toolkit's purpose is to speed up research and experiments that rely on financial information, as they are widely seen in the research literature of economics, finance, business and management.

## Table of Contents
- [Install](#install)
- [Usage](#usage)
- [Citation](#citation)
- [Contributing](#contributing)
- [License](#license)

## Install
- Before starting, ideally, it's recommended to switch to a virtual environment first via `conda` or `virtualenv` or Python's `venv` module.
- Install dependencies via `pip install -r requirements.txt`

## Usage
- To download annual reports from EDGAR, run `python edgar_crawler.py` with the following arguments:
  - `--start_year XXXX`: the year range to start from
  - `--end_year YYYY`: the year range to end to
  - `--quarters` (Optional): the quarters that you want to download filings from (List). Default value is: [1, 2, 3, 4]
  - `--filing_types` (Optional): list of filing types to download. Default value is: ['10-K', '10-K405', '10-KT']
  - `--cik_tickers` (Optional): list or path of file containing CIKs or Tickers. e.g. ['AAPL', 'GOOG', '789019', 1018724, '1550120'] <br>
    In case of file, provide each CIK or Ticker in a different line.  <br>
  If this argument is not provided, then the toolkit will download annual reports for all the U.S. publicly traded companies.
  - `--user_agent` (Optional): the User-agent that will be declared to SEC EDGAR
  - `--raw_filings_folder` (Optional): the name of the folder where downloaded filings will be stored. Default value is `'RAW_FILINGS'`.
  - `--indices_folder` (Optional): the name of the folder where EDGAR TSV files will be stored. These are used to locate the annual reports. Default value is `'INDICES'`.
  - `--filings_csv_filepath` (Optional): CSV filename to save metadata from the reports. e.g 'filename', 'CIK', 'year'
  - `--skip_present_indices` (Optional): Whether to skip already downloaded EDGAR indices or download them nonetheless. Default value is `True`.


- To clean and extract specific item sections from the already-downloaded documents, run `python extract_items.py` with the following arguments: 
  - `--raw_filings_folder`: the name of the folder where the downloaded documents are stored. Default is `'RAW_FILINGS'`.
  - `--extracted_filings_folder`: the name of the folder where extracted documents will be stored. Default is `'EXTRACTED_FILINGS'`. For each downloaded report, a corresponding JSON file will be created containing the item sections as key-pair values.
  - `--items_to_extract`: a list with the certain item sections to extract. e.g. ['7','8'] to extract 'Management’s Discussion and Analysis' and 'Financial Statements' section items<br>
    The default list contains all item sections.
  - Reminder: We currently support the extraction of 10-K documents.

## Citation
If this work inspires you in any way, please consider citing the relevant paper, published at the [3rd Economics and Natural Language Processing (ECONLP) workshop](https://lt3.ugent.be/econlp/) at EMNLP 2021 (Punta Cana, Dominican Republic & Online):
```
@inproceedings{loukas2021edgarcorpus,
      title={EDGAR-CORPUS: Billions of Tokens Make The World Go Round}, 
      author={Lefteris Loukas and Manos Fergadiotis and Ion Androutsopoulos and Prodromos Malakasiotis},
      year={2021},
      eprint={2109.14394},
      archivePrefix={arXiv},
      primaryClass={cs.CL}
}
```
## Contributing
PRs and contributions are accepted.
 
Please use the [Feature Branch Workflow](https://www.atlassian.com/git/tutorials/comparing-workflows/feature-branch-workflow).

## License
Please see the [GNU General Public License v3.0](https://github.com/nlpaueb/edgar-crawler/blob/main/LICENSE)
