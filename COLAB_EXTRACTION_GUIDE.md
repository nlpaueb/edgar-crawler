# Google Colab Extraction Guide 🚀

## Problem: Extraction Showing "Complete" When It's Not

### What's Happening
When running the notebook in Google Colab, the extraction may show as "completed" even though only a fraction of files have been processed. This is because:

1. **Colab Session Timeout**: Google Colab sessions disconnect after 12-24 hours
2. **Very Slow Processing**: Single-threaded extraction takes 110+ hours for ~80,000 files
3. **No Interruption Detection**: The script doesn't detect when Colab disconnects
4. **Misleading Success Message**: Says "✅ Extraction completed!" even if interrupted

### How to Verify Actual Status

Run this command in a Colab cell:
```python
!python check_extraction_status.py
```

This will show:
- ✅ How many files were actually extracted
- 📊 Progress percentage
- ⏱️ Estimated time remaining
- 📁 Breakdown by year

---

## Solutions

### ✅ Solution 1: Use Fast Multi-Process Extraction (RECOMMENDED)

The original extraction uses only **1 CPU core**. The fast version uses **2-4 cores**, making it **2-4x faster**.

#### Quick Start

Replace your **Cell 9** with this:

```python
# Cell 9: Run FAST Multi-Process Extraction
print("="*70)
print(" STARTING FAST EXTRACTION (Multi-Process)")
print("="*70)

import multiprocessing
cpu_count = multiprocessing.cpu_count()
num_processes = min(3, cpu_count - 1) if cpu_count > 1 else 1

print(f"💻 Available CPU cores: {cpu_count}")
print(f"⚡ Using {num_processes} parallel processes")
print(f"🚀 This will be {num_processes}x faster!\n")

# Run fast extraction
!python flexible_extractor_fast.py \
    --config extraction_configs/items_1_1a.json \
    --processes {num_processes}
```

#### Expected Speed Improvements

| Processes | Speed | Time for 80K files |
|-----------|-------|-------------------|
| 1 (current) | 0.25 files/sec | ~110 hours |
| 2 processes | 0.50 files/sec | ~55 hours |
| 3 processes | 0.75 files/sec | ~37 hours |
| 4 processes | 1.0 files/sec | ~28 hours |

---

### ✅ Solution 2: Process by Year (Batch Mode)

Since Colab sessions are time-limited, process one year at a time:

```python
# Cell X: Extract Single Year
import pandas as pd
import os

# Choose year to process
YEAR_TO_PROCESS = 2010  # Change this for each run

print(f"Processing year: {YEAR_TO_PROCESS}")

# Load and filter metadata
metadata = pd.read_csv('datasets/FILINGS_METADATA.csv')
metadata_year = metadata[
    (metadata['Type'] == '10-K') &
    (metadata['year'] == YEAR_TO_PROCESS)
]

# Save year-specific metadata
year_metadata_path = f'datasets/FILINGS_METADATA_{YEAR_TO_PROCESS}.csv'
metadata_year.to_csv(year_metadata_path, index=False)

print(f"Found {len(metadata_year):,} filings for {YEAR_TO_PROCESS}")

# Update config to use year-specific metadata
import json
with open('config.json', 'r') as f:
    config = json.load(f)

config['extract_items']['filings_metadata_file'] = f'FILINGS_METADATA_{YEAR_TO_PROCESS}.csv'

with open('config.json', 'w') as f:
    json.dump(config, f, indent=2)

# Run extraction
!python flexible_extractor_fast.py \
    --config extraction_configs/items_1_1a.json \
    --processes 3

print(f"✅ Completed {YEAR_TO_PROCESS}")
```

Then repeat for each year (2010-2025).

---

### ✅ Solution 3: Real-Time Progress Monitoring

Add this cell to monitor extraction progress **while it's running**:

```python
# Cell: Real-Time Progress Monitor (run in separate cell)
import os
import time
from IPython.display import clear_output

print("📊 Real-Time Progress Monitor")
print("Press STOP to exit (extraction continues)")
print("="*70)

extracted_dir = 'datasets/EXTRACTED_FILINGS/10-K'
initial_count = sum(1 for root, dirs, files in os.walk(extracted_dir)
                   for f in files if f.endswith('.json'))
start_time = time.time()

try:
    while True:
        clear_output(wait=True)

        current_count = sum(1 for root, dirs, files in os.walk(extracted_dir)
                          for f in files if f.endswith('.json'))

        elapsed = time.time() - start_time
        files_this_session = current_count - initial_count

        if elapsed > 0 and files_this_session > 0:
            rate = files_this_session / elapsed
            remaining = (79532 - current_count) / rate if rate > 0 else 0

            print(f"📊 Progress: {current_count:,} / 79,532 ({current_count/79532*100:.1f}%)")
            print(f"This session: {files_this_session:,} files in {elapsed/60:.1f} min")
            print(f"Rate: {rate*60:.1f} files/min ({rate*3600:.0f} files/hour)")
            print(f"Est. remaining: {remaining/3600:.1f} hours")

        time.sleep(30)  # Update every 30 seconds

except KeyboardInterrupt:
    print("\n✅ Monitoring stopped")
```

---

## How to Resume After Colab Disconnection

### Step 1: Reconnect and Check Status
```python
# Run Quick Resume (Cell 13 in notebook)
!python check_extraction_status.py
```

### Step 2: Resume Extraction
The extraction automatically skips already-extracted files (if `skip_existing: true`), so just run:

```python
!python flexible_extractor_fast.py \
    --config extraction_configs/items_1_1a.json \
    --processes 3
```

It will continue where it left off!

---

## Troubleshooting

### Q: Why is extraction so slow?
**A:** The original code uses only 1 CPU core. Use the fast multi-process version.

### Q: How do I know if extraction actually completed?
**A:** Run `!python check_extraction_status.py` to see real progress.

### Q: Can I pause and resume?
**A:** Yes! Just stop the cell and run it again. It will skip already-extracted files.

### Q: What if Colab keeps disconnecting?
**A:** Use batch mode (process by year) or consider:
- Colab Pro (longer sessions)
- Running locally on your machine
- Using a cloud VM

### Q: How can I make it even faster?
**A:**
- Use more processes: `--processes 4`
- Disable table removal: `"remove_tables": false` in config
- Disable special items extraction: `"special_items": {"enabled": false}`

---

## Files Created

| File | Purpose |
|------|---------|
| `extract_items_fast.py` | Multi-process extraction engine |
| `flexible_extractor_fast.py` | Fast version of flexible extractor |
| `check_extraction_status.py` | Verify extraction progress |
| `colab_extraction_cells.md` | Updated notebook cells |
| `COLAB_EXTRACTION_GUIDE.md` | This guide |

---

## Complete Updated Cell Sequence

Replace cells 9-14 in your notebook with:

```python
# Cell 9A: Check Status
!python check_extraction_status.py
```

```python
# Cell 9B: Run Fast Extraction
import multiprocessing
num_processes = min(3, multiprocessing.cpu_count() - 1)

!python flexible_extractor_fast.py \
    --config extraction_configs/items_1_1a.json \
    --processes {num_processes}
```

```python
# Cell 9C: Verify Completion
!python check_extraction_status.py
```

```python
# Cell 9D: Move to item_1_1a Directory
# (Keep existing Cell 9.5 from notebook)
```

```python
# Cell 9E: Restore MD&A Files
# (Keep existing Cell 9.6 from notebook)
```

---

## Summary

✅ **Use multi-process extraction** for 2-4x speedup
✅ **Monitor progress in real-time** to detect issues early
✅ **Verify completion** with status checker before moving on
✅ **Process by year** if hitting Colab session limits
✅ **Resume easily** - extraction skips existing files automatically

---

**Questions?** Check the repository issues or create a new one at:
https://github.com/haowenluo/edgar-crawler/issues
