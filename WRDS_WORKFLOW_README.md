# WRDS-to-EDGAR Workflow for Large-Scale MD&A Extraction

This extension to the edgar-crawler repository provides tools for extracting MD&A sections (and other items) from SEC filings at scale, using WRDS identifiers and optimized for Google Colab.

## Quick Start

### 1. Setup (One-Time)

Open `colab_setup.ipynb` in Google Colab and run all cells to:
- Mount Google Drive
- Install dependencies
- Clone repository
- Authenticate with WRDS

### 2. Download Firm Identifiers

```bash
python wrds_downloader.py --start-year 2010
```

**Output:** `wrds_data/wrds_identifiers.csv` with ~5,000-8,000 firms

### 3. Download SEC Filings (Takes ~1 Week)

```bash
python colab_batch_downloader.py --input wrds_data/wrds_identifiers.csv --batch-size 500
```

**Note:** This will take multiple 24-hour Colab sessions. The script automatically resumes.

### 4. Extract MD&A Sections

```bash
python flexible_extractor.py --config extraction_configs/mda_only.json
```

### 5. Consolidate to CSV

```bash
python consolidate_output.py --item 7 --output mda_analysis.csv
```

**Done!** You now have a CSV with MD&A text for all firms from 2010-present.

---

## New Files Added

### Scripts

| File | Purpose |
|------|---------|
| `wrds_downloader.py` | Download CIK/ticker mappings from WRDS COMPUSTAT |
| `colab_batch_downloader.py` | Batch download SEC filings with resume capability |
| `flexible_extractor.py` | Extract any items from downloaded filings |
| `download_manager.py` | Track inventory and progress |
| `consolidate_output.py` | Combine JSON files into analysis-ready CSV |

### Configuration Templates

| File | Purpose |
|------|---------|
| `extraction_configs/mda_only.json` | Extract MD&A (Item 7) from 10-K |
| `extraction_configs/risk_factors.json` | Extract Risk Factors (Item 1A) |
| `extraction_configs/multi_items.json` | Extract multiple items simultaneously |
| `extraction_configs/10q_mda.json` | Extract quarterly MD&A from 10-Q |

### Documentation

| File | Purpose |
|------|---------|
| `COLAB_GUIDE.md` | **START HERE** - Complete step-by-step guide |
| `AVAILABLE_ITEMS.md` | Catalog of all extractable items and research applications |
| `WRDS_WORKFLOW_README.md` | This file - quick reference |

### Notebooks

| File | Purpose |
|------|---------|
| `colab_setup.ipynb` | One-click setup for Google Colab environment |

---

## Key Features

### ✅ Optimized for Google Colab
- Handles 24-hour session timeouts
- Automatic resume after disconnection
- Progress saved to Google Drive
- Checkpoint after each batch

### ✅ Flexible Item Extraction
- Extract MD&A, Risk Factors, Business Description, etc.
- No need to re-download for different items
- Pre-built config templates for common use cases

### ✅ Large-Scale Processing
- Batch processing for thousands of firms
- Parallel extraction (configurable)
- Progress tracking and monitoring
- Error handling and retry logic

### ✅ Analysis-Ready Output
- Consolidated CSV format
- Derived columns (year, text length, etc.)
- Summary statistics
- Quality checks

---

## Workflow Architecture

```
┌─────────────────────────────────────────────────┐
│ 1. WRDS Download (5-10 min)                    │
│    Input: WRDS credentials + date range        │
│    Output: wrds_identifiers.csv                │
└─────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────┐
│ 2. SEC Download (4-7 days, multiple sessions)  │
│    Input: CIK list                              │
│    Output: RAW_FILINGS/*.html (100-200 GB)     │
│    Features: Batch processing, auto-resume     │
└─────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────┐
│ 3. Item Extraction (4-8 hours)                  │
│    Input: RAW filings + item config            │
│    Output: EXTRACTED_FILINGS/*.json (20-40 GB) │
│    Features: Flexible items, skip existing     │
└─────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────┐
│ 4. Consolidation (30-60 min)                    │
│    Input: JSON files                            │
│    Output: CSV file (2-5 GB)                    │
│    Features: Analysis-ready, summary stats     │
└─────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────┐
│ 5. Analysis                                     │
│    Tool: Python/R/Stata                         │
│    Data: Firm-year MD&A text                    │
└─────────────────────────────────────────────────┘
```

---

## Storage Requirements

| Stage | Storage | Deletable After Next Stage? |
|-------|---------|----------------------------|
| WRDS identifiers | <1 MB | No (needed for tracking) |
| RAW filings | 100-200 GB | No (needed for future extractions) |
| Extracted JSONs | 20-40 GB | Yes (after consolidation) |
| Consolidated CSV | 2-5 GB | No (final output) |

**Recommendation:** Keep RAW filings permanently (allows extracting different items later without re-downloading). Delete JSONs after consolidation to save space.

---

## Timeline Estimate

| Phase | Time | Sessions |
|-------|------|----------|
| Setup | 30 min | 1 |
| WRDS download | 10 min | 1 |
| SEC download | 4-7 days | 6-10 × 24h |
| Extraction | 4-8 hours | 1 |
| Consolidation | 1 hour | 1 |
| **Total** | **~1-2 weeks** | **~10 sessions** |

---

## Common Use Cases

### Research Project 1: MD&A Textual Analysis

```bash
# Download firms
python wrds_downloader.py --start-year 2010

# Download 10-K filings
python colab_batch_downloader.py --input wrds_data/wrds_identifiers.csv

# Extract MD&A
python flexible_extractor.py --items 7

# Consolidate
python consolidate_output.py --item 7 --output mda_analysis.csv
```

### Research Project 2: Risk Factor Evolution

```bash
# (Assuming filings already downloaded)

# Extract Risk Factors
python flexible_extractor.py --items 1A

# Consolidate
python consolidate_output.py --item 1A --output risk_analysis.csv
```

**Time saved:** Hours instead of days (no re-download needed!)

### Research Project 3: Industry-Specific Study

```python
# Filter to specific industry
import pandas as pd
df = pd.read_csv('wrds_data/wrds_identifiers.csv')
df_tech = df[(df['sic'] >= 7000) & (df['sic'] <= 7999)]  # Services
df_tech.to_csv('wrds_data/tech_firms.csv', index=False)

# Download only tech firms
!python colab_batch_downloader.py --input wrds_data/tech_firms.csv
```

### Research Project 4: Quarterly Analysis

```bash
# Download 10-Q filings
python colab_batch_downloader.py \
  --input wrds_data/wrds_identifiers.csv \
  --filing-types 10-Q

# Extract quarterly MD&A
python flexible_extractor.py --config extraction_configs/10q_mda.json

# Consolidate
python consolidate_output.py --item part_1__2 --filing-type 10-Q --output quarterly_mda.csv
```

---

## Monitoring and Management

### Check Download Progress

```bash
python download_manager.py --inventory
```

**Output:**
- Total filings downloaded
- Storage usage
- Batch completion status
- Filings by year and type

### Find Missing Filings

```bash
python download_manager.py --find-missing
```

**Output:**
- Firms with no downloads
- Firms with incomplete year coverage
- Saved to `logs/missing_firms.csv`

### Check Specific Firm

```bash
python download_manager.py --check-cik 320193
```

**Output:**
- Filing history for CIK 320193 (Apple)
- Date range
- Filing types

---

## Troubleshooting

### Problem: Colab Disconnects

**Solution:** This is normal! Just re-run the download command. Progress is automatically saved.

### Problem: Out of Storage

**Solutions:**
1. Delete extracted JSONs after consolidation: `rm -rf datasets/EXTRACTED_FILINGS/10-K/*.json`
2. Upgrade Google Drive storage
3. Process smaller batches (filter WRDS identifiers by industry/year)

### Problem: Some Firms Have No Filings

**Reasons:**
- Firm went public after 2010
- Foreign filer (uses 20-F, not 10-K)
- Delisted before filing period

**Check:** `python download_manager.py --find-missing`

### More Help

See [COLAB_GUIDE.md](COLAB_GUIDE.md) for comprehensive troubleshooting and FAQs.

---

## Citation

If you use this workflow in your research, please cite:

**EDGAR Crawler:**
```
Loukas, L., Fergadiotis, M., Androutsopoulos, I., & Malakasiotis, P. (2021).
EDGAR-CRAWLER: A Python package for downloading SEC filings.
GitHub: https://github.com/nlpaueb/edgar-crawler
```

**WRDS:**
```
Wharton Research Data Services (WRDS). (2024). COMPUSTAT.
University of Pennsylvania. https://wrds-www.wharton.upenn.edu/
```

---

## Support

- **EDGAR Crawler Issues:** https://github.com/nlpaueb/edgar-crawler/issues
- **WRDS Support:** wrds@wharton.upenn.edu
- **Documentation:** See [COLAB_GUIDE.md](COLAB_GUIDE.md) and [AVAILABLE_ITEMS.md](AVAILABLE_ITEMS.md)

---

## License

This extension follows the same license as the edgar-crawler repository. SEC filings are public domain. WRDS data is subject to licensing restrictions - check your institutional agreement.

---

**Ready to start?** Open `colab_setup.ipynb` in Google Colab and begin your large-scale extraction!
