#!/usr/bin/env python3
"""
Reorganize EDGAR Filings into Year Subdirectories

This script reorganizes flat filing directories into year-based subdirectories
to avoid Google Drive's limit on files per directory.

Current structure:
  datasets/RAW_FILINGS/10-K/
    ├── 0000001_10K_2020_xxx.txt
    ├── 0000001_10K_2021_xxx.txt
    └── ... (79,401 files)

New structure:
  datasets/RAW_FILINGS/10-K/
    ├── 2020/
    │   └── 0000001_10K_2020_xxx.txt
    ├── 2021/
    │   └── 0000001_10K_2021_xxx.txt
    └── ...

Usage:
    # Dry run (just show what would be done, don't move files)
    python reorganize_filings.py --dry-run

    # Actually reorganize files
    python reorganize_filings.py

    # Reorganize only specific filing type
    python reorganize_filings.py --filing-type 10-K
"""

import argparse
import os
import re
import shutil
from pathlib import Path
from tqdm import tqdm
import pandas as pd


class FilingReorganizer:
    """Reorganizes filings into year-based subdirectories"""

    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        self.raw_filings_dir = "datasets/RAW_FILINGS"
        self.metadata_file = "datasets/FILINGS_METADATA.csv"

    def extract_year_from_filename(self, filename):
        """
        Extract year from filename pattern: CIK_FILINGTYPE_YEAR_ACCESSION.ext
        Example: 0000001_10K_2020_0001193125-20-123456.txt -> 2020
        """
        match = re.search(r'_(\d{4})_', filename)
        if match:
            return match.group(1)
        return None

    def reorganize_filing_type(self, filing_type):
        """Reorganize all files for a specific filing type"""

        filing_dir = os.path.join(self.raw_filings_dir, filing_type)

        if not os.path.exists(filing_dir):
            print(f"⚠️  Directory not found: {filing_dir}")
            return

        print(f"\n{'='*70}")
        print(f"Reorganizing {filing_type} filings")
        print(f"{'='*70}")

        # Get list of files in the directory
        print("📂 Scanning directory...")
        try:
            all_items = os.listdir(filing_dir)
        except OSError as e:
            print(f"❌ Error reading directory (too many files): {e}")
            print("\n💡 Using metadata file instead...")
            return self.reorganize_from_metadata(filing_type)

        # Filter only files (not directories)
        files = [f for f in all_items if os.path.isfile(os.path.join(filing_dir, f))]

        if len(files) == 0:
            print("✅ No files to reorganize (already organized or empty)")
            return

        print(f"   Found {len(files):,} files to reorganize")

        # Group files by year
        year_groups = {}
        files_without_year = []

        for filename in files:
            year = self.extract_year_from_filename(filename)
            if year:
                if year not in year_groups:
                    year_groups[year] = []
                year_groups[year].append(filename)
            else:
                files_without_year.append(filename)

        print(f"\n📊 Summary:")
        print(f"   Years found: {len(year_groups)}")
        print(f"   Files with year: {sum(len(files) for files in year_groups.values()):,}")
        print(f"   Files without year: {len(files_without_year):,}")

        if files_without_year:
            print(f"\n⚠️  Warning: {len(files_without_year)} files don't match expected naming pattern")
            print(f"   First 5: {files_without_year[:5]}")

        # Reorganize files
        if self.dry_run:
            print("\n🔍 DRY RUN - No files will be moved")
            for year in sorted(year_groups.keys()):
                print(f"   Would create directory: {filing_type}/{year}/")
                print(f"   Would move {len(year_groups[year]):,} files")
        else:
            print("\n📦 Moving files...")
            total_moved = 0
            total_failed = 0

            for year in sorted(year_groups.keys()):
                year_dir = os.path.join(filing_dir, year)

                # Create year directory
                os.makedirs(year_dir, exist_ok=True)

                # Move files with progress bar
                year_files = year_groups[year]
                for filename in tqdm(year_files, desc=f"   {year}", ncols=70):
                    src = os.path.join(filing_dir, filename)
                    dst = os.path.join(year_dir, filename)

                    try:
                        # Check if destination already exists
                        if os.path.exists(dst):
                            # If file already exists in destination, remove source
                            os.remove(src)
                        else:
                            # Move file
                            shutil.move(src, dst)
                        total_moved += 1
                    except Exception as e:
                        print(f"\n   ⚠️  Failed to move {filename}: {e}")
                        total_failed += 1

            print(f"\n✅ Reorganization complete!")
            print(f"   Files moved: {total_moved:,}")
            if total_failed > 0:
                print(f"   Files failed: {total_failed:,}")

    def reorganize_from_metadata(self, filing_type):
        """
        Reorganize files using metadata CSV instead of directory listing.
        This is useful when directory has too many files to list.
        """
        print(f"\n{'='*70}")
        print(f"Reorganizing {filing_type} using metadata")
        print(f"{'='*70}")

        if not os.path.exists(self.metadata_file):
            print(f"❌ Metadata file not found: {self.metadata_file}")
            return

        # Load metadata
        print("📂 Loading metadata...")
        try:
            metadata_df = pd.read_csv(self.metadata_file)
        except Exception as e:
            print(f"❌ Error reading metadata: {e}")
            return

        # Filter for this filing type
        type_df = metadata_df[metadata_df['Type'] == filing_type].copy()

        if len(type_df) == 0:
            print(f"⚠️  No {filing_type} filings found in metadata")
            return

        print(f"   Found {len(type_df):,} {filing_type} filings in metadata")

        # Extract year from Filing Date
        # Try both 'Filing Date' and 'filing_date' for compatibility
        date_col = 'Filing Date' if 'Filing Date' in type_df.columns else 'filing_date'
        type_df['year'] = pd.to_datetime(type_df[date_col]).dt.year.astype(str)

        # Group by year
        year_groups = type_df.groupby('year')

        print(f"\n📊 Years: {type_df['year'].nunique()}")

        filing_dir = os.path.join(self.raw_filings_dir, filing_type)

        if self.dry_run:
            print("\n🔍 DRY RUN - No files will be moved")
            for year, group in year_groups:
                print(f"   Would create directory: {filing_type}/{year}/")
                print(f"   Would move {len(group):,} files")
        else:
            print("\n📦 Moving files...")
            total_moved = 0
            total_failed = 0
            total_not_found = 0

            for year, group in year_groups:
                year_dir = os.path.join(filing_dir, year)

                # Create year directory
                os.makedirs(year_dir, exist_ok=True)

                # Move files with progress bar
                for _, row in tqdm(group.iterrows(), total=len(group), desc=f"   {year}", ncols=70):
                    filename = row.get('Filename') or row.get('filename')

                    if not filename or pd.isna(filename):
                        continue

                    src = os.path.join(filing_dir, filename)
                    dst = os.path.join(year_dir, filename)

                    try:
                        # Check if source file exists
                        if not os.path.exists(src):
                            # Might already be moved
                            if os.path.exists(dst):
                                total_moved += 1
                            else:
                                total_not_found += 1
                            continue

                        # Check if destination already exists
                        if os.path.exists(dst):
                            # If file already exists in destination, remove source
                            os.remove(src)
                        else:
                            # Move file
                            shutil.move(src, dst)
                        total_moved += 1
                    except Exception as e:
                        total_failed += 1
                        # Don't print every error to avoid flooding output

            print(f"\n✅ Reorganization complete!")
            print(f"   Files processed: {total_moved:,}")
            if total_not_found > 0:
                print(f"   Files not found: {total_not_found:,} (may already be organized)")
            if total_failed > 0:
                print(f"   Files failed: {total_failed:,}")

    def verify_reorganization(self, filing_type):
        """Verify that reorganization was successful"""
        print(f"\n{'='*70}")
        print(f"Verifying {filing_type} reorganization")
        print(f"{'='*70}")

        filing_dir = os.path.join(self.raw_filings_dir, filing_type)

        if not os.path.exists(filing_dir):
            print(f"❌ Directory not found: {filing_dir}")
            return

        try:
            items = os.listdir(filing_dir)
        except OSError as e:
            print(f"❌ Still cannot list directory: {e}")
            print("   You may need to remount Google Drive")
            return

        # Count directories (years) vs files
        dirs = [d for d in items if os.path.isdir(os.path.join(filing_dir, d))]
        files = [f for f in items if os.path.isfile(os.path.join(filing_dir, f))]

        print(f"\n📊 Structure:")
        print(f"   Year directories: {len(dirs)}")
        print(f"   Remaining files in root: {len(files)}")

        if len(dirs) > 0:
            print(f"\n   Year directories found: {sorted(dirs)}")

            # Count files in each year
            total_organized = 0
            for year_dir in sorted(dirs):
                year_path = os.path.join(filing_dir, year_dir)
                try:
                    year_files = [f for f in os.listdir(year_path) if os.path.isfile(os.path.join(year_path, f))]
                    total_organized += len(year_files)
                    print(f"      {year_dir}/: {len(year_files):,} files")
                except Exception as e:
                    print(f"      {year_dir}/: Error reading - {e}")

            print(f"\n   Total organized files: {total_organized:,}")

        if len(files) > 0:
            print(f"\n⚠️  Warning: {len(files)} files still in root directory")
            print(f"   First 10: {files[:10]}")
        else:
            print(f"\n✅ All files successfully organized into year directories!")


def main():
    parser = argparse.ArgumentParser(
        description='Reorganize EDGAR filings into year subdirectories',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Dry run to see what would happen
    python reorganize_filings.py --dry-run

    # Reorganize all filing types
    python reorganize_filings.py

    # Reorganize only 10-K filings
    python reorganize_filings.py --filing-type 10-K

    # Verify reorganization
    python reorganize_filings.py --verify --filing-type 10-K
        """
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without actually moving files'
    )

    parser.add_argument(
        '--filing-type',
        type=str,
        choices=['10-K', '10-Q', '8-K', 'all'],
        default='all',
        help='Filing type to reorganize (default: all)'
    )

    parser.add_argument(
        '--verify',
        action='store_true',
        help='Verify reorganization instead of reorganizing'
    )

    args = parser.parse_args()

    # Create reorganizer
    reorganizer = FilingReorganizer(dry_run=args.dry_run)

    # Determine which filing types to process
    if args.filing_type == 'all':
        filing_types = ['10-K', '10-Q', '8-K']
    else:
        filing_types = [args.filing_type]

    # Process each filing type
    for filing_type in filing_types:
        if args.verify:
            reorganizer.verify_reorganization(filing_type)
        else:
            # Try direct reorganization first, will fall back to metadata if needed
            reorganizer.reorganize_filing_type(filing_type)

    print("\n" + "="*70)
    print("DONE!")
    print("="*70)

    if not args.dry_run and not args.verify:
        print("\n💡 Next steps:")
        print("   1. Verify reorganization: python reorganize_filings.py --verify")
        print("   2. Check inventory: python download_manager.py --inventory")


if __name__ == '__main__':
    main()
