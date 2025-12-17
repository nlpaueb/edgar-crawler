# Google Colab Guide for Large-Scale MD&A Extraction

This guide walks you through extracting Management's Discussion and Analysis (MD&A) sections from SEC 10-K filings for all publicly traded companies using Google Colab and WRDS.

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Setup](#setup)
4. [Workflow](#workflow)
5. [Troubleshooting](#troubleshooting)
6. [FAQ](#faq)

---

## Overview

### What This Workflow Does

1. **Downloads firm identifiers** from WRDS COMPUSTAT (CIK, ticker, company name)
2. **Downloads raw 10-K filings** from SEC EDGAR for all firms (2010-present)
3. **Extracts MD&A sections** (Item 7) from each filing
4. **Consolidates** extracted data into a single CSV file for analysis

### Why Use Google Colab?

✅ **Free compute** (or low-cost with Colab Pro)
✅ **No local storage needed** (uses Google Drive)
✅ **Accessible anywhere** (cloud-based)
✅ **Easy to resume** after disconnections
✅ **Pre-configured environment** (Python, Jupyter)

### Time and Storage Requirements

| Item | Estimate |
|------|----------|
| **Total firms** | ~5,000-8,000 publicly traded companies |
| **Total 10-K filings** | ~70,000-100,000 (2010-2024) |
| **Download time** | 4-7 days (across multiple 24-hour sessions) |
| **Extraction time** | 1-2 days (can be done in parallel) |
| **Storage needed** | ~200-300 GB (Google Drive) |
| **Colab sessions** | 6-10 sessions (24 hours each) |

---

## Prerequisites

### Required Accounts and Access

1. **WRDS Account**
   - Institutional subscription required
   - Must have access to COMPUSTAT database
   - Sign up: https://wrds-www.wharton.upenn.edu/

2. **Google Account**
   - Gmail account (free)
   - Sufficient Google Drive storage (~200-300 GB)
   - Consider Google One subscription if needed

3. **Colab Pro (Recommended)**
   - $9.99/month
   - 24-hour sessions (vs 12 hours for free)
   - Better GPUs (not needed here, but nice)
   - Sign up: https://colab.research.google.com/signup

### Technical Requirements

- Modern web browser (Chrome recommended)
- Stable internet connection
- Basic Python knowledge (helpful but not required)

---

## Setup

### Important: Repository Source

This workflow uses an **enhanced fork** of the original EDGAR crawler that includes WRDS integration scripts:
- **Original repository:** `nlpaueb/edgar-crawler` (base extraction tools)
- **Enhanced fork:** `haowenluo/edgar-crawler` (includes WRDS workflow and WRDS integration scripts)

### Step 1: Open Colab Setup Notebook

1. Go to Google Colab: https://colab.research.google.com/
2. Upload `colab_setup.ipynb` from this repository
3. Or create a new notebook and copy the cells

### Step 2: Run Setup Cells

Run each cell in `colab_setup.ipynb` sequentially:

```python
# Cell 1: Mount Google Drive
from google.colab import drive
drive.mount('/content/drive')
```

When prompted:
- Click the link
- Select your Google account
- Copy the authorization code
- Paste it back in Colab

```python
# Cell 2: Install dependencies
!pip install -q wrds beautifulsoup4 lxml requests pandas tqdm pathos
```

```python
# Cell 3: Clone repository
!git clone https://github.com/haowenluo/edgar-crawler.git
%cd edgar-crawler
```

```python
# Cell 4: Configure SEC User-Agent (REQUIRED!)
import json

YOUR_NAME = input("Enter your name: ")
YOUR_EMAIL = input("Enter your email: ")

user_agent = f"{YOUR_NAME} {YOUR_EMAIL}"

with open('config.json', 'r') as f:
    config = json.load(f)
config['download_filings']['user_agent'] = user_agent
with open('config.json', 'w') as f:
    json.dump(config, f, indent=2)

print(f"✅ User-Agent configured: {user_agent}")
```

⚠️ **CRITICAL:** The SEC requires real identification. You'll get 403 errors without this!

```python
# Cell 5: Authenticate with WRDS
import wrds
db = wrds.Connection(wrds_username='your_username')
```

When prompted, enter your WRDS password.

### Step 3: Verify Setup

Run the verification cell to check everything is working:

```python
!python download_manager.py --inventory
```

You should see a setup summary with ✅ checkmarks.

---

## Workflow

### Phase 1: Download Firm Identifiers from WRDS

**Estimated time:** 5-10 minutes
**Storage:** < 1 MB

```python
# Download all publicly traded firms with 10-K filings from 2010-present
!python wrds_downloader.py --start-year 2010
```

**Output:**
- `wrds_data/wrds_identifiers.csv` - Full dataset with CIK, ticker, company name, etc.
- `wrds_data/cik_list.txt` - Simple CIK list

**Expected result:**
```
✅ Retrieved ~5,000-8,000 unique firms from COMPUSTAT
✅ Saved to wrds_data/wrds_identifiers.csv
```

**Review the data:**
```python
import pandas as pd
df = pd.read_csv('wrds_data/wrds_identifiers.csv')
print(df.head())
print(f"Total firms: {len(df)}")
```

### Phase 2: Download Raw SEC Filings

**Estimated time:** 4-7 days (across 6-10 sessions)
**Storage:** 100-200 GB

⚠️ **BEFORE YOU START:** Make sure you've configured your User-Agent in the setup notebook! You'll get 403 errors without it.

This is the most time-consuming phase. The script processes firms in batches and saves progress after each batch, allowing you to resume after Colab's 24-hour timeout.

#### Session 1: Start First Batch

```python
!python colab_batch_downloader.py \
  --input wrds_data/wrds_identifiers.csv \
  --batch-size 500 \
  --start-year 2010
```

**What happens:**
- Firms are divided into batches of 500
- Each batch takes ~6-12 hours
- Progress is saved to `logs/download_progress.csv`
- RAW filings saved to `datasets/RAW_FILINGS/10-K/`

**Monitor progress:**

The script shows progress bars like:
```
Processing batch 1 / 13
Firms in this batch: 500
Downloading filings: 45% ████████░░░░░░ [3,500/7,800]
```

**After ~20-24 hours**, Colab will disconnect. This is normal!

#### Session 2+: Resume Download

When Colab disconnects, simply:

1. Re-run the setup cells (Mount Drive, Change Directory)
2. Re-run the same command:

```python
!python colab_batch_downloader.py \
  --input wrds_data/wrds_identifiers.csv \
  --batch-size 500 \
  --start-year 2010
```

The script will **automatically skip completed batches** and resume from where it left off.

**Check progress between sessions:**
```python
!python download_manager.py --inventory
```

Output:
```
📊 Batch Status:
   Total batches: 13
   ✅ Completed: 3 (23.1%)
   ⏳ Pending: 10
```

#### Repeat Until Complete

Continue starting new Colab sessions and re-running the command until:

```
✅ All batches completed!
```

**Expected timeline:**
- Session 1: Batches 1-3
- Session 2: Batches 4-6
- Session 3: Batches 7-9
- Session 4: Batches 10-13

### Phase 3: Extract MD&A Sections

**Estimated time:** 4-8 hours
**Storage:** 20-40 GB

Once all RAW filings are downloaded, extract the MD&A sections:

```python
!python flexible_extractor.py --config extraction_configs/mda_only.json
```

**What happens:**
- Parses RAW HTML files
- Extracts Item 7 (MD&A) from each filing
- Cleans text (removes HTML, tables, special characters)
- Saves to `datasets/EXTRACTED_FILINGS/10-K/*.json`

**Monitor progress:**
```
🚀 Starting extraction process...
Processing filings: 78% ███████████░░ [56,234/72,000]
Estimated time remaining: 1.2 hours
```

**This runs in a single session** (4-8 hours, well within 24-hour limit).

### Phase 4: Consolidate into CSV

**Estimated time:** 30-60 minutes
**Storage:** 2-5 GB

Convert individual JSON files into a single CSV for analysis:

```python
!python consolidate_output.py \
  --item 7 \
  --filing-type 10-K \
  --output mda_analysis.csv
```

**Output:**

`mda_analysis.csv` with columns:
- `cik` - Firm identifier
- `company_name` - Company name
- `ticker` - Stock ticker
- `filing_date` - When filed
- `fiscal_year` - Fiscal year
- `item_7` - MD&A text content
- `item_7_length` - Character count
- ... and more

**Verify output:**
```python
import pandas as pd
df = pd.read_csv('mda_analysis.csv')
print(f"Total records: {len(df):,}")
print(f"Unique firms: {df['cik'].nunique():,}")
print(f"Date range: {df['fiscal_year'].min()} - {df['fiscal_year'].max()}")
```

### Phase 5: Analysis

Your data is now ready for analysis!

**Download to your computer:**
```python
from google.colab import files
files.download('mda_analysis.csv')
```

**Or analyze in Colab:**
```python
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('mda_analysis.csv')

# Example: MD&A length over time
avg_length = df.groupby('fiscal_year')['item_7_length'].mean()
avg_length.plot(kind='line', title='Average MD&A Length Over Time')
plt.ylabel('Characters')
plt.show()
```

---

## Troubleshooting

### Problem: Colab Disconnects Frequently

**Solution:**
- Use Colab Pro for 24-hour sessions (vs 12 hours free)
- Run the keep-alive script (in setup notebook)
- Don't minimize the browser tab
- Ensure stable internet connection

### Problem: "No module named 'wrds'"

**Solution:**
```python
!pip install wrds
```

### Problem: 403 Errors When Downloading Filings

**Cause:** Invalid or missing User-Agent string.

**Symptoms:**
```
Failed downloading - Max retries exceeded
ResponseError('too many 403 error responses')
```

**Solution:**

The SEC requires you to identify yourself with your real name and email.

**Option 1: Run the User-Agent configuration cell in the setup notebook**

**Option 2: Manually update config.json:**
```python
import json
with open('config.json', 'r') as f:
    config = json.load(f)
config['download_filings']['user_agent'] = "Your Name your.email@university.edu"
with open('config.json', 'w') as f:
    json.dump(config, f, indent=2)
```

**Option 3: Use command-line flag:**
```python
!python colab_batch_downloader.py \
  --input wrds_data/wrds_identifiers.csv \
  --user-agent "Your Name your.email@university.edu"
```

**Why this is required:**
- Per SEC's [fair access policy](https://www.sec.gov/os/accessing-edgar-data)
- Allows SEC to contact you if there are issues
- Prevents abuse of their free service

### Problem: WRDS Authentication Fails

**Solution:**
- Check username/password
- Verify COMPUSTAT access in your WRDS subscription
- Try resetting WRDS password
- Contact WRDS support: wrds@wharton.upenn.edu

### Problem: "Directory not found" Errors

**Solution:**
```python
# Re-mount Google Drive
from google.colab import drive
drive.mount('/content/drive', force_remount=True)

# Change to correct directory
%cd /content/drive/MyDrive/EDGAR_Project/edgar-crawler
```

### Problem: Missing Scripts (wrds_downloader.py, colab_batch_downloader.py, etc.)

**Cause:** You cloned the wrong repository.

**Solution:**
```python
# Remove the old repository
%cd /content/drive/MyDrive/EDGAR_Project
!rm -rf edgar-crawler

# Clone the correct repository with WRDS workflow scripts
!git clone https://github.com/haowenluo/edgar-crawler.git
%cd edgar-crawler

# Verify files exist
!ls -la *.py | grep -E "(wrds|batch|flexible)"
```

You should see output showing the three WRDS scripts.

### Problem: Download is Very Slow

**Reasons:**
- SEC rate limiting (intentional, respectful crawling)
- High SEC server load (try off-peak hours)
- Your internet connection

**This is normal!** Large-scale downloads take days, not hours.

### Problem: Some Firms Have No Filings

**Reasons:**
- Firm went public after 2010
- Firm was delisted before 2010
- Foreign filers (may use 20-F instead of 10-K)
- Missing CIK mapping in COMPUSTAT

**Solution:**
```python
!python download_manager.py --find-missing
```

Review `logs/missing_firms.csv` to investigate.

### Problem: Extraction Fails for Some Filings

**Reasons:**
- Unusual HTML formatting
- Non-standard item numbering
- Scanned PDFs (not parseable)

**This is normal!** Expect ~5-10% failure rate. Check logs:
```python
!tail -n 50 logs/ExtractItems.log
```

### Problem: Out of Storage Space

**Solutions:**

1. **Delete RAW filings after extraction:**
```python
# WARNING: Only do this after extraction is complete!
# You won't be able to extract other items without re-downloading
!rm -rf datasets/RAW_FILINGS/
```

2. **Keep only consolidated CSV:**
```python
# After consolidation, delete individual JSONs
!rm -rf datasets/EXTRACTED_FILINGS/10-K/*.json
```

3. **Upgrade Google Drive storage:**
- Google One: 100 GB ($1.99/mo), 200 GB ($2.99/mo)

---

## FAQ

### Q: Can I extract other sections besides MD&A?

**A: Yes!** See [AVAILABLE_ITEMS.md](AVAILABLE_ITEMS.md) for full list.

Example - Extract Risk Factors (Item 1A):
```python
!python flexible_extractor.py --config extraction_configs/risk_factors.json
```

Since you already have RAW filings, this takes only hours, not days!

### Q: Can I extract from 10-Q (quarterly) filings?

**A: Yes!** Use `--filing-types 10-Q` when downloading.

```python
!python colab_batch_downloader.py \
  --input wrds_data/wrds_identifiers.csv \
  --filing-types 10-Q
```

Then extract quarterly MD&A:
```python
!python flexible_extractor.py --config extraction_configs/10q_mda.json
```

### Q: How do I filter by industry?

**A: Filter the WRDS identifiers before downloading.**

```python
import pandas as pd

# Load all firms
df = pd.read_csv('wrds_data/wrds_identifiers.csv')

# Filter to specific SIC codes (e.g., 6000-6999 = Financial)
df_finance = df[(df['sic'] >= 6000) & (df['sic'] <= 6999)]

# Save filtered list
df_finance.to_csv('wrds_data/finance_firms.csv', index=False)

# Use filtered list for download
!python colab_batch_downloader.py --input wrds_data/finance_firms.csv
```

### Q: Can I use this on my local machine instead of Colab?

**A: Yes!** The scripts work on any Linux/Mac/Windows machine with Python.

Just skip the Colab-specific steps (Drive mounting, keep-alive, etc.)

### Q: How much does this cost?

**Typical costs:**
- WRDS access: $0 (institutional subscription)
- Colab Pro: $10/month × 2 months = $20
- Google Drive storage (200 GB): $3/month × 2 months = $6
- **Total: ~$26**

**Free alternative:**
- Use free Colab (slower, 12-hour sessions)
- Use existing Drive storage
- **Total: $0**

### Q: Can I share the downloaded data?

**⚠️ Important:**
- SEC filings are public domain (can share)
- WRDS data has licensing restrictions (check your agreement)
- Extracted MD&A text is derivative of public filings (generally OK)
- Consult your institution's data policies

### Q: How do I cite this in my research?

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

### Q: Where can I get help?

**Resources:**
- EDGAR Crawler Issues: https://github.com/nlpaueb/edgar-crawler/issues
- WRDS Support: wrds@wharton.upenn.edu
- SEC EDGAR Help: https://www.sec.gov/edgar/searchedgar/accessing-edgar-data.htm

---

## Next Steps

1. **Start with the setup notebook:** `colab_setup.ipynb`
2. **Download firm identifiers:** `wrds_downloader.py`
3. **Begin batch download:** `colab_batch_downloader.py`
4. **Monitor progress:** Check back every 24 hours to resume
5. **Extract MD&A:** Once downloads complete
6. **Consolidate to CSV:** For analysis
7. **Analyze!** Your research awaits

---

## Tips for Success

✅ **Be patient** - Large-scale data collection takes time
✅ **Check progress daily** - Resume after each 24-hour timeout
✅ **Save intermediate results** - Always consolidate before deleting RAW files
✅ **Keep logs** - Helpful for debugging and verification
✅ **Respect SEC servers** - Built-in rate limiting is intentional
✅ **Plan ahead** - Start downloads weeks before analysis deadline

---

## Example Timeline

**Week 1:**
- Day 1: Setup, WRDS download, start batch 1-3
- Day 2: Resume, batches 4-6
- Day 3: Resume, batches 7-9

**Week 2:**
- Day 1: Finish batches 10-13
- Day 2: Extract MD&A sections
- Day 3: Consolidate CSV, begin analysis

**Total: ~10 days from start to analysis-ready data**

---

**Good luck with your research!** 🎓📊

For questions or issues, please open an issue on GitHub or consult the troubleshooting section above.
