# EDGAR-CRAWLER
Crawl and fetch all publicly-traded companies annual reports from [SEC's EDGAR](https://www.sec.gov/edgar.shtml) database.

`edgar-crawler` is an optimized toolkit that retrieves textual information from financial reports, such as 10-K, 10-Q or 8-K filings.

More specifically, it can:
- Crawl and download financial reports for each publicly-traded company, for specified years, through the `edgar_crawler.py` module.
- Extract and clean specific text sections, such as Risk Factors, MD&A, and others, through the `extract_items.py` module. **Currently, we only support extraction of 10-K filings (i.e., annual reports).**

The purpose of EDGAR-CRAWLER is to speed up research and experiments that rely on financial information, as they are widely seen in the research literature of economics, finance, business and management.

## Table of Contents
- [Install](#install)
- [Usage](#usage)
- [Citation](#citation)
- [Accompanying Resources](#accompanying-resources)
- [Contributing](#contributing)
- [License](#license)

## Install
- Before starting, ideally, it's recommended to switch to a virtual environment first via `conda`, using Python 3.6+.
- Install dependencies via `pip install -r requirements.txt`

## Usage
- Before running any script, you can edit the `config.json` file.
  - Arguments for `edgar_crawler.py`, the module to download financial reports:
      - `--start_year XXXX`: the year range to start from
      - `--end_year YYYY`: the year range to end to
      - `--quarters` (Optional): the quarters that you want to download filings from (List).<br> Default value is: `[1, 2, 3, 4]`.
      - `--filing_types` (Optional): list of filing types to download.<br> Default value is: `['10-K', '10-K405', '10-KT']`.
      - `--cik_tickers` (Optional): list or path of file containing CIKs or Tickers. e.g. `[789019, "1018724", "TWTR"]` <br>
        In case of file, provide each CIK or Ticker in a different line.  <br>
      If this argument is not provided, then the toolkit will download annual reports for all the U.S. publicly traded companies.
      - `--user_agent` (Optional): the User-agent that will be declared to SEC EDGAR.
      - `--raw_filings_folder` (Optional): the name of the folder where downloaded filings will be stored.<br> Default value is `'RAW_FILINGS'`.
      - `--indices_folder` (Optional): the name of the folder where EDGAR TSV files will be stored. These are used to locate the annual reports. Default value is `'INDICES'`.
      - `--filings_metadata_file` (Optional): CSV filename to save metadata from the reports. e.g 'filename', 'CIK', 'year'
      - `--skip_present_indices` (Optional): Whether to skip already downloaded EDGAR indices or download them nonetheless.<br> Default value is `True`.
  - Arguments for `extract_items.py`, the module to clean and extract textual data from already-downloaded 10-K reports:
    - `--raw_filings_folder`: the name of the folder where the downloaded documents are stored.<br> Default value s `'RAW_FILINGS'`.
    - `--extracted_filings_folder`: the name of the folder where extracted documents will be stored.<br> Default value is `'EXTRACTED_FILINGS'`.<br> For each downloaded report, a corresponding JSON file will be created containing the item sections as key-pair values.
    - `--items_to_extract`: a list with the certain item sections to extract. e.g. `['7','8']` to extract 'Management’s Discussion and Analysis' and 'Financial Statements' section items.<br>
      The default list contains all item sections.

- To download financial reports from EDGAR, run `python edgar_crawler.py`
- To clean and extract specific item sections from already-downloaded 10-K documents, run `python extract_items.py`.
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
Read the paper here: [https://arxiv.org/abs/2109.14394](https://arxiv.org/abs/2109.14394)

## Accompanying Resources
- [corpus] EDGAR-CORPUS: A corpus for financial NLP research, built from SEC's EDGAR - [https://zenodo.org/record/5528490](https://zenodo.org/record/5528490)
- [embeddings] EDGAR-W2V: Word2vec Embeddings trained on EDGAR-CORPUS - [https://zenodo.org/record/5524358](https://zenodo.org/record/5524358)

## Contributing
PRs and contributions are accepted.
 
Please use the [Feature Branch Workflow](https://www.atlassian.com/git/tutorials/comparing-workflows/feature-branch-workflow).

## License
Please see the [GNU General Public License v3.0](https://github.com/nlpaueb/edgar-crawler/blob/main/LICENSE)
