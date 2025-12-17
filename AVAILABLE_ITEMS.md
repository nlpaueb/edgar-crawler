# Available Items for Extraction from SEC Filings

This document catalogs all extractable items from different SEC filing types. Use this reference when choosing which sections to extract for your research.

## Table of Contents

1. [10-K Annual Reports](#10-k-annual-reports)
2. [10-Q Quarterly Reports](#10-q-quarterly-reports)
3. [8-K Current Reports](#8-k-current-reports)
4. [Extraction Examples](#extraction-examples)
5. [Common Research Applications](#common-research-applications)

---

## 10-K Annual Reports

10-K filings are comprehensive annual reports required by the SEC for publicly traded companies. They provide detailed information about a company's business, financial condition, and operations.

### Available Items

| Item | Section Name | Description | Typical Length | Common Uses |
|------|--------------|-------------|----------------|-------------|
| **1** | Business | Description of company's business operations, products, services, markets | 5-20 pages | Industry analysis, business model research, competitive landscape |
| **1A** | Risk Factors | Comprehensive list of risks facing the company | 10-30 pages | Risk analysis, sentiment analysis, textual analysis, litigation risk |
| **1B** | Unresolved Staff Comments | SEC staff comments not yet resolved (if any) | 0-2 pages | SEC compliance research, audit quality |
| **1C** | Cybersecurity | Cybersecurity risk management and governance (NEW in 2023) | 1-5 pages | Cybersecurity research, IT governance |
| **2** | Properties | Description of physical properties (plants, offices, etc.) | 1-5 pages | Real estate analysis, asset location, manufacturing footprint |
| **3** | Legal Proceedings | Material legal proceedings involving the company | 1-10 pages | Litigation risk, legal exposure, regulatory compliance |
| **4** | Mine Safety Disclosures | Safety information for mining companies (if applicable) | 0-2 pages | Industry-specific (mining companies only) |
| **5** | Market for Registrant's Common Equity | Stock performance, dividends, shareholders | 2-5 pages | Share structure, dividend policy, stock liquidity |
| **6** | [Reserved] | Selected Financial Data (REMOVED as of 2021) | N/A | Historical: 5-year financial trends |
| **7** | **Management's Discussion and Analysis (MD&A)** | **Management's narrative explanation of financial results, trends, uncertainties** | **15-50 pages** | **Performance analysis, forward-looking statements, earnings quality, textual analysis** |
| **7A** | Quantitative and Qualitative Disclosures About Market Risk | Discussion of market risks (interest rate, FX, commodity) | 2-10 pages | Risk management, derivatives usage, market risk exposure |
| **8** | Financial Statements and Supplementary Data | Audited financial statements, footnotes | 20-100 pages | Accounting analysis, financial ratio calculation, earnings management |
| **9** | Changes in and Disagreements with Accountants | Auditor changes and disagreements (if any) | 0-2 pages | Audit quality, auditor switches, accounting disputes |
| **9A** | Controls and Procedures | Assessment of internal controls over financial reporting | 2-10 pages | Internal control quality, SOX compliance, audit risk |
| **9B** | Other Information | Additional information not reported elsewhere | 0-5 pages | Miscellaneous disclosures |
| **10** | Directors, Executive Officers and Corporate Governance | Information about board and management | 5-15 pages | Corporate governance, board composition, executive background |
| **11** | Executive Compensation | Detailed compensation for executives | 10-40 pages | Executive pay analysis, pay-performance sensitivity, compensation structure |
| **12** | Security Ownership of Certain Beneficial Owners and Management | Ownership structure and beneficial owners | 2-10 pages | Ownership concentration, insider ownership, institutional holdings |
| **13** | Certain Relationships and Related Transactions | Related party transactions | 1-5 pages | Conflict of interest, related party deals, governance quality |
| **14** | Principal Accounting Fees and Services | Audit and non-audit fees paid to auditors | 1-3 pages | Audit fees, auditor independence, non-audit services |
| **15** | Exhibits, Financial Statement Schedules | Referenced exhibits and schedules | Variable | Supporting documents, contracts, certifications |

### Extraction Commands

```bash
# Extract MD&A only (most common)
python flexible_extractor.py --items 7 --filing-type 10-K

# Extract Risk Factors only
python flexible_extractor.py --items 1A --filing-type 10-K

# Extract multiple items
python flexible_extractor.py --items 1,1A,7,7A --filing-type 10-K

# Extract all items
python flexible_extractor.py --all --filing-type 10-K
```

---

## 10-Q Quarterly Reports

10-Q filings are quarterly reports providing updates on financial performance and business developments. They are shorter and less detailed than 10-Ks.

### Part I - Financial Information

| Item | Section Name | Description | Typical Length | Common Uses |
|------|--------------|-------------|----------------|-------------|
| **part_1__1** | Financial Statements | Unaudited quarterly financial statements | 10-30 pages | Quarterly earnings, financial trends |
| **part_1__2** | **Management's Discussion and Analysis** | **Quarterly MD&A (similar to 10-K Item 7 but for quarter)** | **5-15 pages** | **Quarterly performance, management commentary** |
| **part_1__3** | Quantitative and Qualitative Disclosures About Market Risk | Quarterly market risk update | 1-5 pages | Quarterly risk exposure, derivatives |
| **part_1__4** | Controls and Procedures | Quarterly internal controls assessment | 1-3 pages | SOX compliance, control changes |

### Part II - Other Information

| Item | Section Name | Description | Typical Length | Common Uses |
|------|--------------|-------------|----------------|-------------|
| **part_2__1** | Legal Proceedings | Update on legal proceedings | 0-5 pages | Litigation updates |
| **part_2__1A** | Risk Factors | Updates to risk factors (if material changes) | 0-10 pages | New risks, risk evolution |
| **part_2__2** | Unregistered Sales of Equity Securities and Use of Proceeds | Stock issuances, buybacks | 1-3 pages | Share repurchases, equity issuance |
| **part_2__3** | Defaults Upon Senior Securities | Defaults on debt (if any) | 0-2 pages | Financial distress |
| **part_2__4** | Mine Safety Disclosures | Quarterly mine safety (if applicable) | 0-1 pages | Mining companies only |
| **part_2__5** | Other Information | Miscellaneous quarterly updates | 0-3 pages | Various disclosures |
| **part_2__6** | Exhibits | Referenced exhibits | Variable | Supporting documents |

### Extraction Commands

```bash
# Extract quarterly MD&A
python flexible_extractor.py --items part_1__2 --filing-type 10-Q

# Extract quarterly financials + MD&A
python flexible_extractor.py --items part_1__1,part_1__2 --filing-type 10-Q

# Extract all Part I items
python flexible_extractor.py --items part_1__1,part_1__2,part_1__3,part_1__4 --filing-type 10-Q
```

---

## 8-K Current Reports

8-K filings report material events that occur between quarterly reports. Unlike 10-K and 10-Q, 8-Ks are event-driven and vary greatly in content.

### Available Items (Event Types)

**Note:** 8-Ks do NOT contain MD&A sections. They report specific events only.

#### Section 1 - Registrant's Business and Operations
- **1.01** Entry into Material Agreement
- **1.02** Termination of Material Agreement
- **1.03** Bankruptcy or Receivership
- **1.04** Mine Safety Reporting
- **1.05** Material Cybersecurity Incidents (NEW in 2023)

#### Section 2 - Financial Information
- **2.01** Completion of Acquisition or Disposition
- **2.02** Results of Operations and Financial Condition
- **2.03** Creation of Direct Financial Obligation
- **2.04** Triggering Events That Accelerate Obligations
- **2.05** Costs Associated with Exit Activities
- **2.06** Material Impairments

#### Section 3 - Securities and Trading Markets
- **3.01** Notice of Delisting
- **3.02** Unregistered Sales of Equity Securities
- **3.03** Material Modifications to Rights of Security Holders

#### Section 4 - Accountants and Financial Statements
- **4.01** Changes in Registrant's Certifying Accountant
- **4.02** Non-Reliance on Previously Issued Financial Statements

#### Section 5 - Corporate Governance and Management
- **5.01** Changes in Control of Registrant
- **5.02** Departure/Election of Directors or Officers
- **5.03** Amendments to Articles or Bylaws
- **5.04** Temporary Suspension of Trading Under Employee Benefit Plans
- **5.05** Amendments to Registrant's Code of Ethics
- **5.06** Change in Shell Company Status
- **5.07** Submission of Matters to a Vote of Security Holders
- **5.08** Shareholder Nominations

#### Section 7 - Regulation FD
- **7.01** Regulation FD Disclosure

#### Section 8 - Other Events
- **8.01** Other Events (catch-all for material events)

#### Section 9 - Financial Statements and Exhibits
- **9.01** Financial Statements and Exhibits

### Extraction Notes

8-Ks are event-specific and don't follow the same structure as 10-K/10-Q. Extraction is more complex and depends on the specific event being reported.

---

## Extraction Examples

### Example 1: Basic MD&A Extraction (10-K)

```bash
# Extract MD&A for all firms
python flexible_extractor.py --items 7 --filing-type 10-K

# Consolidate into CSV
python consolidate_output.py --item 7 --output mda_analysis.csv
```

**Result:** CSV with columns: `cik`, `company_name`, `fiscal_year`, `item_7` (MD&A text)

### Example 2: Risk Factor Analysis

```bash
# Extract Risk Factors
python flexible_extractor.py --items 1A --filing-type 10-K

# Consolidate
python consolidate_output.py --item 1A --output risk_factors.csv
```

**Use cases:** Sentiment analysis, topic modeling, risk evolution over time

### Example 3: Multi-Item Text Analysis

```bash
# Extract Business + Risk + MD&A
python flexible_extractor.py --items 1,1A,7 --filing-type 10-K

# Consolidate all items
python consolidate_output.py --items 1,1A,7 --output text_corpus.csv
```

**Use cases:** Comprehensive textual analysis, tone comparison across sections

### Example 4: Quarterly MD&A Time Series

```bash
# Download 10-Q filings (if not already done)
python colab_batch_downloader.py --input wrds_data/wrds_identifiers.csv --filing-types 10-Q

# Extract quarterly MD&A
python flexible_extractor.py --items part_1__2 --filing-type 10-Q

# Consolidate
python consolidate_output.py --item part_1__2 --filing-type 10-Q --output quarterly_mda.csv
```

**Result:** 4x more data points per firm per year compared to annual 10-K

### Example 5: Executive Compensation Study

```bash
# Extract Item 11 (Executive Compensation)
python flexible_extractor.py --items 11 --filing-type 10-K

# Consolidate
python consolidate_output.py --item 11 --output exec_comp.csv
```

**Use cases:** Pay-performance analysis, compensation structure, pay ratio research

---

## Common Research Applications

### By Research Topic

| Research Topic | Filing Type | Items to Extract | Example Studies |
|----------------|-------------|------------------|-----------------|
| **Textual Analysis / Tone** | 10-K | 7 (MD&A), 1A (Risk) | Loughran & McDonald (2011), Li (2008) |
| **Forward-Looking Statements** | 10-K | 7 (MD&A) | Muslu et al. (2015) |
| **Risk Disclosure** | 10-K | 1A (Risk Factors) | Campbell et al. (2014) |
| **Business Description** | 10-K | 1 (Business) | Hoberg & Phillips (2016) |
| **Accounting Quality** | 10-K | 8 (Financials), 7 (MD&A) | Various accounting papers |
| **Corporate Governance** | 10-K | 10, 11, 12, 13 | Governance indices, board research |
| **Executive Compensation** | 10-K | 11 (Comp), 12 (Ownership) | Say-on-pay, incentive alignment |
| **Litigation Risk** | 10-K | 3 (Legal Proceedings), 1A (Risk) | Rogers & Van Buskirk (2009) |
| **Market Risk** | 10-K | 7A (Market Risk) | Linsmeier et al. (2002) |
| **Quarterly Reporting** | 10-Q | part_1__2 (MD&A) | Earnings announcements, guidance |
| **Material Events** | 8-K | Various event items | Event studies, information content |

### By Method

| Methodology | Best Items | Notes |
|-------------|-----------|-------|
| **Sentiment Analysis** | 7, 1A | Use dictionaries like Loughran-McDonald |
| **Topic Modeling (LDA, etc.)** | 1, 7, 1A | Long-form text ideal for topic extraction |
| **Named Entity Recognition** | 1, 7 | Extract companies, products, locations |
| **Readability Analysis** | 7, 1A | Fog Index, Flesch-Kincaid scores |
| **Similarity Measures** | 1, 7 | Compare firms, track changes over time |
| **Machine Learning** | All text items | Train classifiers, predict outcomes |
| **Word Embeddings** | 1, 7, 1A | Word2Vec, BERT, domain-specific models |

---

## Tips for Choosing Items

### For MD&A Research
- **10-K Item 7** is the gold standard
- Rich in management commentary, forward-looking statements
- Most consistent format across firms and years
- 15-50 pages of dense text

### For Risk Analysis
- **10-K Item 1A** provides comprehensive risk factors
- Often used for sentiment and tone analysis
- Can be very long (10-30 pages)
- New risks appear over time (longitudinal studies)

### For Quarterly Studies
- **10-Q Part I, Item 2** provides quarterly MD&A
- Shorter than annual MD&A
- More timely, event-driven
- 4x more observations per firm

### For Comprehensive Studies
- Extract **multiple items** (e.g., 1, 1A, 7, 7A)
- Compare tone/content across sections
- Richer feature set for machine learning
- Requires more storage and processing

---

## Extraction Best Practices

1. **Start with MD&A (Item 7)**
   - Most commonly used in research
   - Well-structured and consistent
   - Easy to extract and analyze

2. **Consider your research question**
   - Risk study? → Item 1A
   - Governance? → Items 10-14
   - Accounting? → Item 8 + 7

3. **Test on small sample first**
   - Extract 10-20 firms before scaling up
   - Check extraction quality
   - Verify text content

4. **Plan for storage**
   - Item 7 alone: ~20-40 GB for all firms
   - Multiple items: 50-100 GB+
   - Consolidated CSVs: 2-10 GB

5. **Document your extraction**
   - Save config files used
   - Note any filtering or cleaning applied
   - Track extraction date (filings can be amended)

---

## References

**SEC Filing Requirements:**
- 10-K: https://www.sec.gov/files/form10-k.pdf
- 10-Q: https://www.sec.gov/files/form10-q.pdf
- 8-K: https://www.sec.gov/files/form8-k.pdf

**Academic Papers (Examples):**
- Loughran & McDonald (2011) - Textual analysis and sentiment
- Li (2008) - MD&A and earnings forecasts
- Hoberg & Phillips (2016) - Product market competition from 10-K text
- Campbell et al. (2014) - Risk factor disclosures

---

**Questions?** Refer to [COLAB_GUIDE.md](COLAB_GUIDE.md) for usage instructions or open an issue on GitHub.
