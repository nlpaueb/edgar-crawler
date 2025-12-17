#!/usr/bin/env python3
"""
Extraction Status Checker

Checks the current status of EDGAR filing extraction and provides detailed statistics.
Use this to verify if extraction actually completed or was interrupted.
"""

import os
import json
from collections import defaultdict
from pathlib import Path
import pandas as pd


def count_files_recursive(directory, extensions):
    """Count files with specific extensions recursively"""
    count = 0
    year_counts = defaultdict(int)

    if not os.path.exists(directory):
        return count, year_counts

    for root, dirs, files in os.walk(directory):
        matching_files = [f for f in files if any(f.endswith(ext) for ext in extensions)]
        count += len(matching_files)

        # Try to extract year from path
        parts = Path(root).parts
        for part in parts:
            if part.isdigit() and len(part) == 4:
                year_counts[part] += len(matching_files)
                break

    return count, year_counts


def main():
    print("=" * 80)
    print(" EDGAR EXTRACTION STATUS CHECKER")
    print("=" * 80)

    # Check raw files
    print("\n📁 RAW FILES (to be extracted):")
    raw_dir = "datasets/RAW_FILINGS/10-K"
    raw_count, raw_year_counts = count_files_recursive(raw_dir, ['.htm', '.txt'])

    if raw_count > 0:
        print(f"   Total: {raw_count:,} files")
        if raw_year_counts:
            print(f"   By year:")
            for year in sorted(raw_year_counts.keys()):
                print(f"      {year}: {raw_year_counts[year]:,} files")
    else:
        print(f"   ⚠️  No raw files found in {raw_dir}")
        print(f"   Check if path is correct or files need to be downloaded")

    # Check extracted files
    print(f"\n📊 EXTRACTED FILES (JSON):")
    extracted_dir = "datasets/EXTRACTED_FILINGS/10-K"
    extracted_count, extracted_year_counts = count_files_recursive(extracted_dir, ['.json'])

    if extracted_count > 0:
        print(f"   Total: {extracted_count:,} files")
        if extracted_year_counts:
            print(f"   By year:")
            for year in sorted(extracted_year_counts.keys()):
                print(f"      {year}: {extracted_year_counts[year]:,} files")
    else:
        print(f"   ⚠️  No extracted files found in {extracted_dir}")
        print(f"   Extraction has not started or no output directory exists")

    # Calculate progress
    if raw_count > 0:
        print(f"\n📈 PROGRESS:")
        progress_pct = (extracted_count / raw_count) * 100
        remaining = raw_count - extracted_count

        print(f"   Extracted: {extracted_count:,} / {raw_count:,} ({progress_pct:.1f}%)")
        print(f"   Remaining: {remaining:,} files")

        if progress_pct >= 99.9:
            print(f"\n   ✅ EXTRACTION IS COMPLETE!")
        elif progress_pct >= 50:
            print(f"\n   ⏳ EXTRACTION IS PARTIALLY COMPLETE")
            print(f"   Run extraction again to process remaining {remaining:,} files")
        elif progress_pct > 0:
            print(f"\n   ⚠️  EXTRACTION IS INCOMPLETE")
            print(f"   Only {progress_pct:.1f}% complete")
        else:
            print(f"\n   ❌ EXTRACTION HAS NOT STARTED")
            print(f"   No files have been extracted yet")

    # Check metadata
    print(f"\n📋 METADATA CHECK:")
    metadata_paths = [
        "datasets/FILINGS_METADATA_2010_onwards.csv",
        "datasets/FILINGS_METADATA.csv"
    ]

    metadata_found = False
    for metadata_path in metadata_paths:
        if os.path.exists(metadata_path):
            metadata_found = True
            try:
                df = pd.read_csv(metadata_path)
                df_10k = df[df['Type'] == '10-K']
                print(f"   ✅ Found {metadata_path}")
                print(f"      Total 10-K records: {len(df_10k):,}")

                if 'year' in df.columns:
                    year_dist = df_10k['year'].value_counts().sort_index()
                    print(f"      Year range: {year_dist.index.min()} - {year_dist.index.max()}")
            except Exception as e:
                print(f"   ⚠️  Could not read {metadata_path}: {e}")

    if not metadata_found:
        print(f"   ⚠️  No metadata files found")
        print(f"   Expected locations: {', '.join(metadata_paths)}")

    # Check for items_1_1a directory
    print(f"\n📂 SPECIAL OUTPUT DIRECTORIES:")
    special_dirs = [
        "datasets/EXTRACTED_FILINGS/item_1_1a",
        "datasets/EXTRACTED_FILINGS/Item_7_MDA"
    ]

    for special_dir in special_dirs:
        if os.path.exists(special_dir):
            count, year_counts = count_files_recursive(special_dir, ['.json'])
            print(f"   ✅ {special_dir}/")
            print(f"      Files: {count:,}")
        else:
            print(f"   ⚠️  {special_dir}/ (not found)")

    # Check backup directory
    backup_dir = "datasets/EXTRACTED_FILINGS/10-K_MDA_BACKUP"
    if os.path.exists(backup_dir):
        print(f"\n⚠️  BACKUP DIRECTORY STILL EXISTS:")
        print(f"   {backup_dir}/")
        print(f"   This means MD&A files have not been restored yet")
        print(f"   Run the restore cell (Cell 9.6) to restore MD&A files")

    # Speed estimation
    if extracted_count > 0 and raw_count > 0:
        print(f"\n⏱️  TIME ESTIMATION:")
        remaining = raw_count - extracted_count
        if remaining > 0:
            # Estimate based on different process counts
            print(f"   Remaining files: {remaining:,}")
            print(f"\n   Estimated time to completion:")
            print(f"      1 process:  {remaining * 5 / 3600:.1f} hours (current single-thread)")
            print(f"      2 processes: {remaining * 2.5 / 3600:.1f} hours (2x faster)")
            print(f"      3 processes: {remaining * 1.7 / 3600:.1f} hours (3x faster)")
            print(f"      4 processes: {remaining * 1.3 / 3600:.1f} hours (4x faster)")

    print("\n" + "=" * 80)
    print(" END OF STATUS CHECK")
    print("=" * 80)


if __name__ == "__main__":
    main()
