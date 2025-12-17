# Guide: Reorganizing EDGAR Filings to Fix Google Drive Issues

## Problem

You're experiencing Google Drive errors when you have too many files in a single directory:
- `OSError: [Errno 5] Input/output error` when listing directories
- Google Drive mount timeouts
- `download_manager.py --inventory` fails

This happens because Google Drive has performance issues with directories containing many files (your 79,401 10-K filings exceed this limit).

## Solution

Reorganize files from a flat structure into year-based subdirectories:

**Before:**
```
datasets/RAW_FILINGS/10-K/
├── 0000001_10K_2020_xxx.txt
├── 0000001_10K_2021_xxx.txt
└── ... (79,401 files in one folder)
```

**After:**
```
datasets/RAW_FILINGS/10-K/
├── 2020/
│   └── 0000001_10K_2020_xxx.txt
├── 2021/
│   └── 0000001_10K_2021_xxx.txt
└── ...
```

---

## Step-by-Step Instructions

### Step 1: First, Unmount and Remount Google Drive

Since your Drive is having issues, it's best to restart the connection:

```python
# In Colab, unmount if mounted
try:
    from google.colab import drive
    drive.flush_and_unmount()
except:
    pass

# Wait a few seconds, then remount
import time
time.sleep(5)

# Remount
from google.colab import drive
drive.mount('/content/drive', force_remount=True)
```

### Step 2: Navigate to Your Project Directory

```bash
cd /content/drive/MyDrive/EDGAR_Project/edgar-crawler
```

### Step 3: Test with Dry Run (Recommended)

First, see what would happen WITHOUT actually moving files:

```bash
python reorganize_filings.py --dry-run --filing-type 10-K
```

This will show you:
- How many files will be moved
- Which year directories will be created
- No files are actually moved

### Step 4: Reorganize Your Files

Once you're comfortable with the dry run output, reorganize your files:

```bash
python reorganize_filings.py --filing-type 10-K
```

**Expected output:**
```
======================================================================
Reorganizing 10-K using metadata
======================================================================

📂 Loading metadata...
   Found 79,401 10-K filings in metadata

📊 Years: 10

📦 Moving files...
   2015: 100%|████████████████████| 8000/8000
   2016: 100%|████████████████████| 8200/8200
   ...

✅ Reorganization complete!
   Files processed: 79,401
```

**This will take some time** (10-30 minutes for 79k files) because:
- Google Drive operations are slow
- Each file needs to be moved individually
- The script uses metadata to avoid the directory listing issue

**Important Notes:**
- If the script is interrupted, you can safely run it again - it will skip already-moved files
- If you see "Files not found" warnings, those files may have already been moved
- The script creates subdirectories automatically

### Step 5: Verify Reorganization

After reorganization completes, verify it worked:

```bash
python reorganize_filings.py --verify --filing-type 10-K
```

**Expected output:**
```
======================================================================
Verifying 10-K reorganization
======================================================================

📊 Structure:
   Year directories: 10
   Remaining files in root: 0

   Year directories found: ['2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023', '2024']
      2015/: 7,523 files
      2016/: 7,892 files
      2017/: 8,156 files
      ...

   Total organized files: 79,401

✅ All files successfully organized into year directories!
```

### Step 6: Test Inventory Command

Now your inventory command should work:

```bash
python download_manager.py --inventory
```

**Expected output:**
```
======================================================================
EDGAR DOWNLOAD INVENTORY
======================================================================

💾 Storage Usage:
   RAW filings: 12.34 GB
   EXTRACTED filings: 5.67 GB
   Total: 18.01 GB

📊 Downloaded Filings:
   Total filings: 79,401
   ...

📁 RAW Filings:
   10-K: 79,401 files  ← Should work now!
```

---

## Troubleshooting

### Problem: "Still cannot list directory" after reorganization

**Solution:** Unmount and remount Google Drive:
```python
from google.colab import drive
drive.flush_and_unmount()
import time; time.sleep(5)
drive.mount('/content/drive', force_remount=True)
```

### Problem: Script interrupted mid-reorganization

**Solution:** Just run it again - it will skip already-moved files:
```bash
python reorganize_filings.py --filing-type 10-K
```

### Problem: "Files not found" warnings

This is usually normal - those files may have already been moved to year subdirectories. Check the final count to verify all files were processed.

### Problem: Want to reorganize all filing types at once

```bash
# Reorganize 10-K, 10-Q, and 8-K
python reorganize_filings.py --filing-type all
```

---

## What Changed in the Code

The following files were updated to support year-based organization:

1. **download_filings.py** - Downloads now save to `RAW_FILINGS/{filing-type}/{year}/` instead of `RAW_FILINGS/{filing-type}/`

2. **download_manager.py** - Inventory command now counts files recursively in year subdirectories

3. **reorganize_filings.py** - New utility to reorganize existing files

**Future downloads will automatically use the new structure**, so you won't have this problem again.

---

## Next Steps After Reorganization

1. ✅ Verify all files were moved correctly
2. ✅ Test inventory command works
3. ✅ Continue with your normal download workflow
4. Future downloads will automatically organize into year folders

---

## Questions?

- The reorganization script is safe - it checks if files exist before moving
- Files are moved, not copied, so no extra storage is used
- The script uses metadata to avoid directory listing issues
- Year subdirectories are created automatically

If you encounter any issues, check:
1. Google Drive is properly mounted
2. You're in the correct directory
3. Metadata file exists: `datasets/FILINGS_METADATA.csv`
