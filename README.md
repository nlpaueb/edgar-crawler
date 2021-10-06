# EDGAR-CRAWLER
Crawl and fetch all publicly-traded companies annual reports from [SEC's EDGAR](https://www.sec.gov/edgar.shtml) database.

`edgar-crawler` is a an optimized toolkit that retrieves textual information from financial annual reports (10-K filings).

More specifically, it can:
- Crawl and download 10-K annual reports for each publicly-traded company, for specified years 
- Extract and clean the text sections from annual reports (e.g. Risk Factors, MD&A, and others)    

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
  - `--cik` (Optional): a given Company Index Key (CIK) for a specific company. Alternatively, for multiple specific companies, you can specify an absolute filepath to an .CSV file containing each CIK in the first column of each row.
  If this argument is not provided, then the toolkit will download annual reports for all the U.S. publicly traded companies.
  - `--download_folder` (Optional): the name of the folder where downloaded documents will be stored. Default value is `'DOWNLOADED_FILINGS'`.
  - `--indices_folder` (Optional): the name of the folder where EDGAR .idx files will be stored. These are used to locate the annual reports. Default value is `'INDICES'`.
  - `--num_threads` (Optional): the number of threads for multi-processing. Default is `2`.  
- To clean and extract specific item sections from the already-downloaded documents, run `python extract_items.py` with the following arguments: 
  - `--download_folder`: the name of the folder where documents are stored. Default is `'DOWNLOADED_FILINGS'`.
  - `--extraction_folder`: the name of the folder where extracted documents will be stored. Default is `'EXTRACTED_FILINGS'`. For each downloaded report, a corresponding JSON file will be created containing the item sections as key-pair values.
  - `--items`: a list with the certain item sections to extract. The default list contains all item sections.
  - `--num_threads` (Optional): the number of threads for multi-processing. Default is `2`.

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
