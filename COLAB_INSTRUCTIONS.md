# How to Rebuild Metadata in Google Colab

Your extraction is only processing 5,618 files because your metadata only tracks 5,618 files, even though you have 85,019 files on disk. This will rebuild the metadata to include all files.

---

## Step 1: Mount Google Drive

```python
from google.colab import drive
drive.mount('/content/drive')
```

---

## Step 2: Change to Your Project Directory

```python
import os
os.chdir('/content/drive/MyDrive/EDGAR_Project/edgar-crawler')

# Verify you're in the right place
!pwd
!ls -la datasets/RAW_FILINGS/10-K/ | head
```

---

## Step 3: Install Dependencies (if needed)

```python
# These should already be installed in Colab, but just in case
!pip install -q beautifulsoup4 lxml pandas tqdm
```

---

## Step 4: Upload the Script

Two options:

### Option A: Copy from your GitHub repo
```python
# If you've pushed it to your repo
!wget https://raw.githubusercontent.com/haowenluo/edgar-crawler/claude/fix-edgar-inventory-01VHYDnmaNQtFpWqjL8B6Bwq/rebuild_metadata_colab.py
```

### Option B: Upload manually
1. Download `rebuild_metadata_colab.py` from your repo
2. Use Colab's Files panel (📁 icon on left) → Upload

---

## Step 5: Run the Rebuilder (Preview First)

```python
# Import the script
from rebuild_metadata_colab import rebuild_for_colab

# PREVIEW ONLY - doesn't modify anything
rebuild_for_colab(
    filing_types=['10-K'],
    fast_mode=True,      # Fast mode: only reads filenames, not HTML content
    dry_run=True         # Preview mode: shows what would be done
)
```

You should see output like:
```
📂 Scanning 10-K directory...
   Found 85,019 files

📊 Summary:
   Files scanned: 85,019
   Unique CIKs: [number of unique companies]

🔍 Comparison with existing metadata:
   Existing tracked: 5,618 filings
   Found on disk: 85,019 filings
   New discoveries: 79,401 filings
```

---

## Step 6: Actually Rebuild the Metadata

```python
# FAST MODE - Recommended (takes ~5-10 minutes)
# Only extracts from filenames: CIK, Type, Year, Accession Number
rebuild_for_colab(
    filing_types=['10-K'],
    fast_mode=True,      # Only read filenames
    dry_run=False        # Actually write the files
)
```

Or if you want ALL metadata (takes longer, ~30-60 minutes):

```python
# FULL MODE - Extracts everything including filing dates, company names
rebuild_for_colab(
    filing_types=['10-K'],
    fast_mode=False,     # Parse HTML files for extra metadata
    dry_run=False        # Actually write the files
)
```

---

## Step 7: Verify the Results

```python
import pandas as pd

# Check the new metadata
metadata = pd.read_csv('datasets/FILINGS_METADATA.csv')

print(f"Total filings in metadata: {len(metadata):,}")
print(f"Unique CIKs: {metadata['CIK'].nunique():,}")
print(f"\nBy filing type:")
print(metadata['Type'].value_counts())
print(f"\nBy year:")
print(metadata['year'].value_counts().sort_index())
```

You should now see:
```
Total filings in metadata: 85,019
Unique CIKs: [many more than 456]
```

---

## Step 8: Update Your Extraction Config

```python
# Now update your extraction to use the complete metadata
import json

config_path = 'config.json'

with open(config_path, 'r') as f:
    config = json.load(f)

# Use the rebuilt metadata (not the filtered one)
config['extract_items']['filings_metadata_file'] = 'FILINGS_METADATA.csv'
config['extract_items']['filing_types'] = ['10-K']

with open(config_path, 'w') as f:
    json.dump(config, f, indent=2)

print("✅ Config updated to use complete metadata")
print(f"   Now tracks: 85,019 filings")
```

---

## Step 9: Re-run Your Extraction

Now when you run your extraction process (Cell 3 from your original code), it will extract all 85,019 filings instead of just 5,618!

```python
# Your existing extraction code will now work with all files
!python extract_items.py
```

---

## Troubleshooting

### If the path is wrong:

```python
# Find your actual path
!find /content/drive -name "edgar-crawler" -type d 2>/dev/null
```

Then update in the script:

```python
# If your path is different, create rebuilder manually
from rebuild_metadata_colab import MetadataRebuilder

base_path = '/content/drive/My Drive/EDGAR_Project/edgar-crawler'  # Your actual path
rebuilder = MetadataRebuilder(base_path)
rebuilder.rebuild_metadata(['10-K'], dry_run=True, extract_from_files=False)
```

---

## Quick Reference

| Mode | Speed | What it extracts |
|------|-------|-----------------|
| `fast_mode=True` | ⚡ Fast (~5-10 min) | CIK, Type, Year, Accession Number (from filename) |
| `fast_mode=False` | 🐌 Slow (~30-60 min) | Everything above + Filing Date, Company Name, SIC (from HTML) |

**Recommendation:** Use `fast_mode=True` first. You can always re-run with `fast_mode=False` later if you need the extra metadata.

---

## What Gets Created

After running the rebuild:

1. **FILINGS_METADATA.csv** - Complete metadata with all 85,019 files ✅
2. **FILINGS_METADATA_BACKUP.csv** - Backup of your old metadata
3. **FILINGS_METADATA_DISCOVERED.csv** - Just the newly discovered files

You can now use the complete metadata for extraction!
