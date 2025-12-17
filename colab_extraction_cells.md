# IMPROVED COLAB EXTRACTION CELLS

## Replace Cell 9 (Run Extraction) with these cells:

### Cell 9A: Check Current Progress
```python
import os
import json

print("="*70)
print(" CHECKING EXTRACTION PROGRESS")
print("="*70)

# Count raw files to process
raw_dir = 'datasets/RAW_FILINGS/10-K'
raw_count = 0
for root, dirs, files in os.walk(raw_dir):
    raw_count += len([f for f in files if f.endswith(('.htm', '.txt'))])

# Count extracted JSON files
extracted_dir = 'datasets/EXTRACTED_FILINGS/10-K'
extracted_count = 0
if os.path.exists(extracted_dir):
    for root, dirs, files in os.walk(extracted_dir):
        extracted_count += len([f for f in files if f.endswith('.json')])

print(f"\nTotal raw files to extract: {raw_count:,}")
print(f"Already extracted: {extracted_count:,}")
print(f"Remaining: {raw_count - extracted_count:,}")
print(f"Progress: {extracted_count/raw_count*100:.1f}%")

if extracted_count > 0:
    print(f"\n⚠️ Found {extracted_count:,} existing extracted files")
    print(f"   These will be SKIPPED (skip_extracted_filings = True)")
print("\n" + "="*70)
```

### Cell 9B: Run FAST Multi-Process Extraction (NEW!)
```python
import os
import sys

# Check CPU count
import multiprocessing
cpu_count = multiprocessing.cpu_count()
print(f"💻 Available CPU cores: {cpu_count}")

# Use 2-3 processes for Colab (leave 1 core for system)
num_processes = min(3, cpu_count - 1) if cpu_count > 1 else 1
print(f"🚀 Using {num_processes} parallel processes\n")

print("="*70)
print(" STARTING FAST EXTRACTION")
print("="*70)

# Import and run fast extraction
from extract_items_fast import main_fast

try:
    main_fast(num_processes=num_processes)
    print("\n✅ Extraction completed!")
except KeyboardInterrupt:
    print("\n⚠️ Extraction interrupted by user")
except Exception as e:
    print(f"\n❌ Extraction failed: {e}")
    raise
```

### Cell 9C: Real-Time Progress Monitor (Run in Separate Cell While Extracting)
```python
import os
import time
from IPython.display import clear_output

print("📊 Real-Time Progress Monitor")
print("Press STOP button to exit monitoring (extraction continues)")
print("="*70)

extracted_dir = 'datasets/EXTRACTED_FILINGS/10-K'

# Get initial count
initial_count = 0
if os.path.exists(extracted_dir):
    for root, dirs, files in os.walk(extracted_dir):
        initial_count += len([f for f in files if f.endswith('.json')])

start_time = time.time()

try:
    while True:
        clear_output(wait=True)

        # Count current extracted files
        current_count = 0
        if os.path.exists(extracted_dir):
            for root, dirs, files in os.walk(extracted_dir):
                current_count += len([f for f in files if f.endswith('.json')])

        elapsed = time.time() - start_time
        files_this_session = current_count - initial_count

        if elapsed > 0 and files_this_session > 0:
            rate = files_this_session / elapsed
            estimated_remaining = (79532 - current_count) / rate if rate > 0 else 0

            print(f"📊 Real-Time Progress Monitor")
            print(f"="*70)
            print(f"Extracted so far: {current_count:,} / 79,532")
            print(f"Progress: {current_count/79532*100:.1f}%")
            print(f"This session: {files_this_session:,} files in {elapsed/60:.1f} minutes")
            print(f"Rate: {rate*60:.1f} files/minute ({rate*3600:.0f} files/hour)")
            print(f"Est. time remaining: {estimated_remaining/3600:.1f} hours")
            print(f"="*70)
        else:
            print(f"Waiting for extraction to start...")
            print(f"Current count: {current_count:,}")

        time.sleep(30)  # Update every 30 seconds

except KeyboardInterrupt:
    print("\n✅ Monitoring stopped (extraction continues in background)")
```

---

## Cell 9D: Check What Was Actually Extracted (Run After "Completion")
```python
import os
import json
from collections import defaultdict

print("="*70)
print(" EXTRACTION VERIFICATION")
print("="*70)

extracted_dir = 'datasets/EXTRACTED_FILINGS/10-K'

if not os.path.exists(extracted_dir):
    print("❌ No extracted files found!")
else:
    # Count by year
    year_counts = defaultdict(int)
    total_count = 0

    for root, dirs, files in os.walk(extracted_dir):
        json_files = [f for f in files if f.endswith('.json')]
        if json_files:
            year = os.path.basename(root)
            if year.isdigit():
                year_counts[year] = len(json_files)
            total_count += len(json_files)

    print(f"\n✅ Found {total_count:,} extracted JSON files")
    print(f"\nBreakdown by year:")
    for year in sorted(year_counts.keys()):
        print(f"   {year}: {year_counts[year]:,} files")

    # Check metadata expectations
    import pandas as pd
    metadata_path = 'datasets/FILINGS_METADATA_2010_onwards.csv'
    if os.path.exists(metadata_path):
        metadata = pd.read_csv(metadata_path)
        expected = len(metadata)
        print(f"\n📊 Comparison:")
        print(f"   Expected: {expected:,} files")
        print(f"   Extracted: {total_count:,} files")
        print(f"   Remaining: {expected - total_count:,} files")
        print(f"   Progress: {total_count/expected*100:.1f}%")

        if total_count >= expected:
            print(f"\n🎉 EXTRACTION IS COMPLETE!")
        else:
            print(f"\n⚠️  EXTRACTION IS INCOMPLETE")
            print(f"   Run Cell 9B again to resume (will skip existing files)")
```

---

## Usage Instructions:

1. **First time**: Run Cell 9A to check progress, then Cell 9B to start extraction
2. **Monitor progress**: Run Cell 9C in a separate cell while Cell 9B is running
3. **After completion/interruption**: Run Cell 9D to verify what was actually extracted
4. **To resume**: Just run Cell 9B again - it will skip already-extracted files

---

## Expected Speed Improvements:

- **Single process (current)**: ~0.2-0.5 files/second = **44-110 hours**
- **2 processes**: ~0.4-1.0 files/second = **22-55 hours**
- **3 processes**: ~0.6-1.5 files/second = **15-37 hours**

Even with 3 processes, you'll need **multiple Colab sessions** to complete 79,532 files.

---

## Strategy for Colab Limitations:

### Approach 1: Batch Processing by Year
Process one year at a time to complete within Colab session limits:
- Filter metadata to single year: `metadata_filtered = metadata[metadata['year'] == 2010]`
- Extract that year
- Repeat for next year

### Approach 2: Use Colab Pro
- Longer sessions (24 hours)
- Better CPU/RAM
- Background execution

### Approach 3: Local Execution
- Download to local machine
- Run overnight for 2-3 days
- Upload results back to Drive
