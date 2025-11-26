# EDGAR-CRAWLER: Extract Key Financial Data from SEC Filings Effortlessly 🚀
![EDGAR-CRAWLER-LOGO](images/edgar-crawler-logo-white-bg.jpeg)


**EDGAR-CRAWLER** is the only open-source toolkit that **downloads** **raw** and unstructured financial **SEC filings** from EDGAR and parses them **into** **structured JSON files** in order to **bootstrap financial NLP** experiments.

---

**EDGAR-CRAWLER** has 2 core functionalities:
- 📥 **Seamless downloading**: Retrieve and download financial filings from all US publicly-traded companies based on your specified filters, like year, quarters, filing type, etc.
- 🔍 **Structured JSON parsing**: Extract and parse specific key item sections from 10-K, 10-Q, and 8-K filings into a nice-and-clean standardized JSON format.  *(filings supported: 10-K, 10-Q, 8-K)*


## 🚨 News
- 2025/05: We presented EDGAR-CRAWLER and its paper at WWW 2025, which was held in Sydney. (https://dl.acm.org/doi/10.1145/3701716.3715289)
- 2024/10: We now support **structured JSON output for 10-Q and 8-K** filings. ([@Bailefan](https://github.com/Bailefan))
- 2023/12: We had a Lightning Talk about EDGAR-CRAWLER at the 3rd Workshop for Natural Language Processing Open Source Software [(NLP-OSS)](https://nlposs.github.io/2023/), hosted at EMNLP 2023, in Singapore.
- 2023/01: EDGAR-CORPUS, the biggest financial NLP corpus (generated from EDGAR-CRAWLER!), is available as a HuggingFace 🤗 dataset card. See [Accompanying Resources](#Accompanying-Resources).
- 2022/10: Updated documentation and fixed a minor import bug affecting older Python versions.
- 2022/04: EDGAR-CRAWLER is available for Windows systems too.
- 2021/11: We presented EDGAR-CORPUS, our "parent" project that started it all, at [ECONLP 2021](https://lt3.ugent.be/econlp/) (EMNLP Workshop) at the Dominican Republic. See [Accompanying Resources](#Accompanying-Resources) for more details. An early/alpha version of EDGAR-CRAWLER was built for that project.

## Table of Contents
- [Example JSON Outputs](#example-json-outputs)
- [Install](#install)
- [Usage](#usage)
- [Citation](#citation)
- [Accompanying Resources](#accompanying-resources)
- [Feedback](#feedback)
- [Contributing](#contributing)
- [Issues](#issues)

## Example JSON Outputs
Other than downloading the raw filings, **EDGAR-CRAWLER** is the only open-source toolkit that converts the complex and unstructured SEC filings to **structured JSON outputs** for easier integration to your research and development. Below are examples of such outputs for each supported filing type:

### 10-K filing (Annual Report)


Original report: [Apple 10-K from 2022](https://www.sec.gov/Archives/edgar/data/320193/000032019322000108/aapl-20220924.htm)

  ```json
  {
    "cik": "320193",
    "company": "Apple Inc.",
    "filing_type": "10-K",
    "filing_date": "2022-10-28",
    "period_of_report": "2022-09-24",
    "sic": "3571",
    "state_of_inc": "CA",
    "state_location": "CA",
    "fiscal_year_end": "0924",
    "filing_html_index": "https://www.sec.gov/Archives/edgar/data/320193/0000320193-22-000108-index.html",
    "htm_filing_link": "https://www.sec.gov/Archives/edgar/data/320193/000032019322000108/aapl-20220924.htm",
    "complete_text_filing_link": "https://www.sec.gov/Archives/edgar/data/320193/0000320193-22-000108.txt",
    "filename": "320193_10K_2022_0000320193-22-000108.htm",
    "item_1": "Item 1. Business\nCompany Background\nThe Company designs, manufactures ...",
    "item_1A": "Item 1A. Risk Factors\nThe Company’s business, reputation, results of ...",
    "item_1B": "Item 1B. Unresolved Staff Comments\nNone.",
    "item_1C": "",
    "item_2": "Item 2. Properties\nThe Company’s headquarters are located in Cupertino, California. ...",
    "item_3": "Item 3. Legal Proceedings\nEpic Games\nEpic Games, Inc. (“Epic”) filed a lawsuit ...",
    "item_4": "Item 4. Mine Safety Disclosures\nNot applicable. ...",
    "item_5": "Item 5. Market for Registrant’s Common Equity, Related Stockholder ...",
    "item_6": "Item 6. [Reserved]\nApple Inc. | 2022 Form 10-K | 19",
    "item_7": "Item 7. Management’s Discussion and Analysis of Financial Condition ...",
    "item_8": "Item 8. Financial Statements and Supplementary Data\nAll financial ...",
    "item_9": "Item 9. Changes in and Disagreements with Accountants on Accounting and Financial Disclosure\nNone.",
    "item_9A": "Item 9A. Controls and Procedures\nEvaluation of Disclosure Controls and ...",
    "item_9B": "Item 9B. Other Information\nRule 10b5-1 Trading Plans\nDuring the three months ...",
    "item_9C": "Item 9C. Disclosure Regarding Foreign Jurisdictions that Prevent Inspections\nNot applicable. ...",
    "item_10": "Item 10. Directors, Executive Officers and Corporate Governance\nThe information required ...",
    "item_11": "Item 11. Executive Compensation\nThe information required by this Item will be included ...",
    "item_12": "Item 12. Security Ownership of Certain Beneficial Owners and Management and ...",
    "item_13": "Item 13. Certain Relationships and Related Transactions, and Director Independence ...",
    "item_14": "Item 14. Principal Accountant Fees and Services\nThe information required ...",
    "item_15": "Item 15. Exhibit and Financial Statement Schedules\n(a)Documents filed as part ...",
    "item_16": "Item 16. Form 10-K Summary\nNone.\nApple Inc. | 2022 Form 10-K | 57",
    "special_items": [
      {
        "type": "restructuring",
        "keywords_matched": ["restructuring charges"],
        "confidence": 0.90,
        "source_section": "item_8",
        "context": "...recorded restructuring charges of $125.3 million...",
        "amount_raw": "$125.3 million",
        "amount_value": 125.3,
        "amount_scale": "million",
        "footnote_reference": "Note 12",
        "footnote_id": "12",
        "anchor_hash": 12345678
      }
    ]
  }
```

**Note**: The `special_items` array is optional and only included when special items extraction is enabled in the configuration.

### Special Items Extraction

**EDGAR-CRAWLER** now supports automated extraction of special/nonrecurring items from financial statements. Special items are typically disclosed in the "Consolidated Statements of Operations" (income statement) with details in the footnotes of 10-K reports.

**What are Special Items?**
Special items (also called nonrecurring or unusual items) are one-time or infrequent charges/gains that companies report separately because they don't reflect normal ongoing operations. Examples include:
- Restructuring charges (severance, facility closures)
- Asset impairment charges
- Legal settlements and litigation costs
- Discontinued operations
- Merger and acquisition-related expenses (transaction costs, integration costs, advisory fees)
- Gains or losses from asset sales and divestitures

**How it works:**
1. Scans Item 8 (Financial Statements) and optionally Item 7 (MD&A)
2. Uses keyword-based pattern matching to identify special items
3. Extracts monetary amounts and links to footnote references
4. Calculates confidence scores based on context
5. Outputs structured JSON with all findings

**Output Structure:**
Each special item in the `special_items` array contains:
- `type`: Category (restructuring, impairment, litigation, discontinued_ops, acquisition, asset_sale, unusual, other)
- `keywords_matched`: List of matched keywords
- `confidence`: Confidence score (0.0-1.0)
- `source_section`: Source item (item_7 or item_8)
- `context`: Surrounding text excerpt
- `amount_raw`: Raw amount text (e.g., "$125.3 million")
- `amount_value`: Parsed numeric value (e.g., 125.3)
- `amount_scale`: Scale indicator (million, billion, dollars)
- `footnote_reference`: Reference to related footnote (e.g., "Note 12")
- `footnote_id`: Extracted footnote number/letter
- `anchor_hash`: Lightweight hash for source traceability

**Configuration:**
See the `special_items` section in `config.json` to enable/disable extraction, adjust confidence thresholds, customize keywords, and enable debug logging.

### 10-Q (Quarterly Report)

<details>
  <summary>Click to see a full structured output example of a 10-Q filing.</summary>

Original report: [Apple 10-Q from Q1 2024](https://www.sec.gov/Archives/edgar/data/320193/000032019324000069/aapl-20240330.htm)

```json
{
  "cik": "320193",
  "company": "Apple Inc.",
  "filing_type": "10-Q",
  "filing_date": "2024-05-03",
  "period_of_report": "2024-03-30",
  "sic": "3571",
  "state_of_inc": "CA",
  "state_location": "CA",
  "fiscal_year_end": "0928",
  "filing_html_index": "https://www.sec.gov/Archives/edgar/data/320193/0000320193-24-000069-index.html",
  "htm_filing_link": "https://www.sec.gov/Archives/edgar/data/320193/000032019324000069/aapl-20240330.htm",
  "complete_text_filing_link": "https://www.sec.gov/Archives/edgar/data/320193/0000320193-24-000069.txt",
  "filename": "320193_10Q_2024_0000320193-24-000069.htm",
  "part_1": "PART I - FINANCIAL INFORMATION\nItem 1. Financial Statements\nApple Inc.\nCONDENSED CONSOLIDATED STATEMENTS ...",
  "part_1_item_1": "Item 1. Financial Statements\nApple Inc.\nCONDENSED CONSOLIDATED STATEMENTS ...",
  "part_1_item_2": "Item 2. Management’s Discussion and Analysis of Financial Condition and ...",
  "part_1_item_3": "Item 3. Quantitative and Qualitative Disclosures About Market Risk\nThere have ...",
  "part_1_item_4": "Item 4. Controls and Procedures\nEvaluation of Disclosure Controls and ...",
  "part_2": "PART II - OTHER INFORMATION\nItem 1. Legal Proceedings\nDigital Markets Act Investigations\nOn ...",
  "part_2_item_1": "Item 1. Legal Proceedings\nDigital Markets Act Investigations\nOn March 25, 2024, ...",
  "part_2_item_1A": "Item 1A. Risk Factors\nThe Company’s business, reputation, ...",
  "part_2_item_2": "Item 2. Unregistered Sales of Equity Securities and Use of ...",
  "part_2_item_3": "Item 3. Defaults Upon Senior Securities\nNone.",
  "part_2_item_4": "Item 4. Mine Safety Disclosures\nNot applicable.",
  "part_2_item_5": "Item 5. Other Information\nInsider Trading Arrangements\nNone.",
  "part_2_item_6": "Item 6. Exhibits\nIncorporated by Reference\nExhibit\nNumber\nExhibit Description ..."
}
```
**Note:** `part_1` and `part_2` contain the full detected text for that Part. We provide that, since in some old 10-Q files, it is not possible to extract the information in item level.
</details>

### 8-K (Important Current Report)

<details>
  <summary>Click to see a full structured output example of an 8-K filing.</summary>

  Original report: [Apple 8-K from 2022-08-19](https://www.sec.gov/Archives/edgar/data/320193/000119312522225365/d366128d8k.htm)

  ```json
  {
    "cik": "320193",
    "company": "Apple Inc.",
    "filing_type": "8-K",
    "filing_date": "2022-08-19",
    "period_of_report": "2022-08-17",
    "sic": "3571",
    "state_of_inc": "CA",
    "state_location": "CA",
    "fiscal_year_end": "0924",
    "filing_html_index": "https://www.sec.gov/Archives/edgar/data/320193/0001193125-22-225365-index.html",
    "htm_filing_link": "https://www.sec.gov/Archives/edgar/data/320193/000119312522225365/d366128d8k.htm",
    "complete_text_filing_link": "https://www.sec.gov/Archives/edgar/data/320193/0001193125-22-225365.txt",
    "filename": "320193_8K_2022_0001193125-22-225365.htm",
    "item_1.01": "",
    "item_1.02": "",
    "item_1.03": "",
    "item_1.04": "",
    "item_1.05": "",
    "item_2.01": "",
    "item_2.02": "",
    "item_2.03": "",
    "item_2.04": "",
    "item_2.05": "",
    "item_2.06": "",
    "item_3.01": "",
    "item_3.02": "",
    "item_3.03": "",
    "item_4.01": "",
    "item_4.02": "",
    "item_5.01": "",
    "item_5.02": "Item 5.02 Departure of Directors or Certain Officers; Election of Directors; Appointment ...",
    "item_5.03": "Item 5.03 Amendments to Articles of Incorporation or Bylaws; Change in Fiscal Year.\nOn August 17, 2022, Apple’s Board approved and adopted amended and restated bylaws ...",
    "item_5.04": "",
    "item_5.05": "",
    "item_5.06": "",
    "item_5.07": "",
    "item_5.08": "",
    "item_6.01": "",
    "item_6.02": "",
    "item_6.03": "",
    "item_6.04": "",
    "item_6.05": "",
    "item_7.01": "",
    "item_8.01": "",
    "item_9.01": "Item 9.01 Financial Statements and Exhibits.\n(d) Exhibits.\nExhibit\nNumber\nExhibit ...",
  }
  ```
  
</details> 

## Install

- Download `EDGAR-CRAWLER` locally via SSH or HTTPS:
```bash
# Method 1: SSH 
git clone https://github.com/nlpaueb/edgar-crawler.git 

# Method 2: HTTPS
git clone git@github.com:nlpaueb/edgar-crawler.git
```

- Then, it's recommended to create a new virtual environment using Python 3.8 by [installing and using Anaconda](https://docs.anaconda.com/anaconda/install/index.html).
```bash
conda create -n edgar-crawler-venv python=3.8 # After installing Anaconda, create a venv with python 3.8+
conda activate edgar-crawler-venv # Activate the environment
```

- Then, install the toolkit's dependencies via:
```bash
pip install -r requirements.txt # Install requirements for edgar-crawler
```

## Usage
- Before running any script, you should edit the `config.json` file, which configures the behavior of our 2 modules (one for downloading the filings of your choice, the other one for getting the structured output of them). 
  - Arguments for `download_filings.py`, the module to download financial reports:
      - `start_year XXXX`: the year range to start from (default is 2023).
      - `end_year YYYY`: the year range to end to (default is 2023).
      - `quarters`: the quarters that you want to download filings from (List).<br> Default value is: `[1, 2, 3, 4]`.
      - `filing_types`: list of filing types to download.<br> Default value is: `['10-K', '8-K', '10-Q']`.
      - `cik_tickers`: list or path of file containing CIKs or Tickers. e.g. `[789019, "1018724", "AAPL", "TWTR"]` <br>
        In case of file, provide each CIK or Ticker in a different line.  <br>
      If this argument is not provided, then the toolkit will download annual reports for all the U.S. publicly traded companies.
      - `user_agent`: the User-agent (name/email) that will be declared to SEC EDGAR.
      - `raw_filings_folder`: the name of the folder where downloaded filings will be stored.<br> Default value is `'RAW_FILINGS'`.
      - `indices_folder`: the name of the folder where EDGAR TSV files will be stored. These are used to locate the annual reports. Default value is `'INDICES'`.
      - `filings_metadata_file`: CSV filename to save metadata from the reports.
      - `skip_present_indices`: Whether to skip already downloaded EDGAR indices or download them nonetheless.<br> Default value is `True`.
  - Arguments for `extract_items.py`, the module to clean and extract textual data from already-downloaded reports:
    - `raw_filings_folder`: the name of the folder where the downloaded documents are stored.<br> Default value s `'RAW_FILINGS'`.
    - `extracted_filings_folder`: the name of the folder where extracted documents will be stored.<br> Default value is `'EXTRACTED_FILINGS'`.<br> For each downloaded report, a corresponding JSON file will be created containing the item sections as key-pair values.
    - `filings_metadata_file`: CSV filename to load reports metadata (Provide the same csv file as in `download_filings.py`).
    - `filing_types`: list of filing types to extract.
    - `include_signature`: Whether to include the signature section after the last item or not.
    - `items_to_extract`: a list with the certain item sections to extract. <br>
      e.g. `['7','8']` to extract 'Management’s Discussion and Analysis' and 'Financial Statements' section items for 10-K reports.<br>
      By default, this list is empty, in which case all items are extracted.
    - `remove_tables`: Whether to remove tables containing mostly numerical (financial) data. This work is mostly to facilitate NLP research where, often, numerical tables are not useful.
    - `skip_extracted_filings`: Whether to skip already extracted filings or extract them nonetheless.<br> Default value is `True`.
    - `special_items`: Configuration for extracting special/nonrecurring items from financial statements. This is a nested object with the following fields:
      - `enabled`: Whether to enable special items extraction.<br> Default value is `true`.
      - `scan_item_7_mda`: Whether to also scan Item 7 (MD&A) in addition to Item 8.<br> Default value is `false`.
      - `confidence_threshold`: Minimum confidence score (0.0-1.0) for including a special item.<br> Default value is `0.3`.
      - `debug_logging`: Whether to log detailed extraction information for debugging.<br> Default value is `false`.
      - `keywords`: A dictionary of keyword categories and their associated search terms. Categories include:
        - `restructuring`: Keywords for restructuring charges (e.g., "restructuring", "workforce reduction", "severance")
        - `impairment`: Keywords for asset impairments (e.g., "impairment", "write-down", "goodwill impairment")
        - `litigation`: Keywords for legal settlements (e.g., "litigation", "settlement", "legal proceeding")
        - `discontinued_ops`: Keywords for discontinued operations (e.g., "discontinued operation", "disposal of business")
        - `acquisition`: Keywords for M&A-related expenses (e.g., "acquisition", "merger", "transaction costs", "integration costs", "M&A")
        - `asset_sale`: Keywords for asset sales and divestitures (e.g., "gain on sale", "loss on sale", "asset disposal", "divestiture")
        - `unusual`: Keywords for unusual/nonrecurring items (e.g., "one-time", "special charge", "non-gaap adjustment")
        - `other`: Keywords for other special items (e.g., "debt extinguishment", "pension settlement", "foreign exchange")

- To download the raw financial reports from EDGAR, run `python download_filings.py`.
- To clean and extract specific item sections from already-downloaded documents, run `python extract_items.py`.
  - Reminder: **We currently support the structured JSON output for 10-K, 10-Q and 8-K documents.**
  - Note: For older 10-Q filings, it might not be possible to extract any items for specific parts (Part 1 or Part 2). Because of this, we also include each full `part` in the output file as a separate entry.


## Citation
Please cite our 2 relevant papers:

1. **EDGAR-CRAWLER** @ **The Web Conference (WWW)** 2025 — Sydney, Australia (URL: https://dl.acm.org/doi/10.1145/3701716.3715289)
```bibtex
@inproceedings{loukas-etal-2025-edgar-crawler-www25,
  author = {Loukas, Lefteris and Billert, Fabian and Fergadiotis, Manos and Malakasiotis, Prodromos and Androutsopoulos, Ion},
  title = {{EDGAR}-{CRAWLER}: {F}rom {R}aw {W}eb {D}ocuments to {S}tructured {F}inancial {NLP} {D}atasets},
  year = {2025},
  isbn = {9798400713316},
  publisher = {Association for Computing Machinery},
  address = {New York, NY, USA},
  url = {https://doi.org/10.1145/3701716.3715289},
  doi = {10.1145/3701716.3715289},
  booktitle = {Companion Proceedings of the ACM on Web Conference 2025},
  pages = {761–764},
  numpages = {4},
  keywords = {data resource, edgar, nlp, open-source software, web archive},
  location = {Sydney NSW, Australia},
  series = {WWW '25}
}
```
2. **EDGAR-CORPUS** @ **ECONLP Workshop, EMNLP** 2021 — Punta Cana, Dominican Republic (URL: https://aclanthology.org/2021.econlp-1.2/)
```bibtex
@inproceedings{loukas-etal-2021-edgar-corpus,
    title = "{EDGAR}-{CORPUS}: {B}illions of {T}okens {M}ake {T}he {W}orld {G}o {R}ound",
    author = "Loukas, Lefteris  and
      Fergadiotis, Manos  and
      Androutsopoulos, Ion  and
      Malakasiotis, Prodromos",
    booktitle = "Proceedings of the Third Workshop on Economics and Natural Language Processing (ECONLP)",
    month = nov,
    year = "2021",
    address = "Punta Cana, Dominican Republic",
    publisher = "Association for Computational Linguistics",
    url = "https://aclanthology.org/2021.econlp-1.2",
    pages = "13--18",
}
```

## Star History
[![Star History Chart](https://api.star-history.com/svg?repos=nlpaueb/edgar-crawler&type=Date)](https://www.star-history.com/#lefterisloukas/edgar-crawler&Date)

## Accompanying Resources
Here are some additional resources created by using **EDGAR-CRAWLER**:
 
- **EDGAR-CORPUS**: The largest financial NLP corpus, 6+ billion tokens from annual reports [(HuggingFace URL 🤗)](https://huggingface.co/datasets/eloukas/edgar-corpus/) | [(Zenodo URL)](https://zenodo.org/record/5528490).

- **EDGAR-W2V**: Financial Word2Vec embeddings, pre-trained on EDGAR-CORPUS [(Zenodo URL)](https://zenodo.org/record/5524358)

## Feedback
Do you have any feature request? [Tell us directly using this Google Form: (https://forms.gle/bpV8nxMqX8Sq2v5z8)!](https://forms.gle/bpV8nxMqX8Sq2v5z8)

## Contributing
PRs and contributions are accepted. We use the [Feature Branch Workflow](https://docs.github.com/en/get-started/exploring-projects-on-github/contributing-to-a-project).

## Issues
Please create an issue on GitHub instead of emailing us directly so all possible users can benefit from the troubleshooting.

## License
This software is licensed under the [GNU General Public License v3.0](https://github.com/nlpaueb/edgar-crawler/blob/main/LICENSE), a license approved by the Open-Source Initiative (OSI).
