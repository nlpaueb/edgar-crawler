#!/usr/bin/env python3
"""
Rebuild metadata from actual files on disk - COLAB VERSION

This script scans all downloaded filing files and creates/updates the metadata CSV
to include ALL files, not just the ones from the last download session.

FOR GOOGLE COLAB: This version uses absolute paths for Google Drive
"""

import os
import re
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm


class MetadataRebuilder:
    """Rebuilds metadata from files on disk"""

    def __init__(self, base_path):
        """
        Initialize with base path to edgar-crawler directory

        Args:
            base_path: Path to edgar-crawler directory
                      (e.g., '/content/drive/MyDrive/EDGAR_Project/edgar-crawler')
        """
        self.base_path = base_path
        self.raw_filings_dir = os.path.join(base_path, "datasets", "RAW_FILINGS")
        self.metadata_file = os.path.join(base_path, "datasets", "FILINGS_METADATA.csv")
        self.metadata_backup = os.path.join(base_path, "datasets", "FILINGS_METADATA_BACKUP.csv")
        self.discovered_file = os.path.join(base_path, "datasets", "FILINGS_METADATA_DISCOVERED.csv")

    def extract_metadata_from_filename(self, filename):
        """
        Extract metadata from filename.

        Filename format: {CIK}_{filing_type}_{year}_{accession_num}.{ext}
        Example: 1000229_10K_2018_0001193125-18-123456.htm

        Returns:
            dict with CIK, Type, year, accession_number, filename
        """
        # Remove extension
        name_without_ext = os.path.splitext(filename)[0]

        # Split by underscore
        parts = name_without_ext.split('_')

        if len(parts) < 4:
            return None

        return {
            'CIK': parts[0],
            'Type': self._normalize_filing_type(parts[1]),
            'year': parts[2],
            'accession_number': parts[3],
            'filename': filename
        }

    def _normalize_filing_type(self, filing_type):
        """Normalize filing type from filename to standard format"""
        # Remove dashes/slashes from filename
        # 10K -> 10-K, 10Q -> 10-Q, 8K -> 8-K
        normalized = filing_type.upper()

        if normalized in ['10K', '10-K']:
            return '10-K'
        elif normalized in ['10Q', '10-Q']:
            return '10-Q'
        elif normalized in ['8K', '8-K']:
            return '8-K'
        else:
            return filing_type

    def extract_metadata_from_file(self, filepath):
        """
        Extract additional metadata from the HTML file itself.
        This includes filing date, period of report, company name, etc.
        """
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()

            soup = BeautifulSoup(content, 'lxml')

            metadata = {}

            # Try to extract filing date and period of report from HTML
            info_divs = soup.find_all('div', class_=['info', 'infoHead'])

            for i, div in enumerate(info_divs):
                text = div.get_text()
                if 'Filing Date' in text:
                    if i + 1 < len(info_divs):
                        metadata['filing_date'] = info_divs[i + 1].get_text().strip()

                if 'Period of Report' in text:
                    if i + 1 < len(info_divs):
                        metadata['period_of_report'] = info_divs[i + 1].get_text().strip()

            # Try to extract company name
            company_info = soup.find('span', class_='companyName')
            if company_info:
                company_text = company_info.get_text()
                # Remove CIK from company name
                metadata['Company'] = re.sub(r'\s+CIK#.*$', '', company_text).strip()

            # Try to extract SIC
            sic_link = soup.find('a', href=re.compile(r'SIC='))
            if sic_link:
                metadata['SIC'] = sic_link.get_text().strip()

            return metadata

        except Exception as e:
            # If we can't parse the file, return empty dict
            return {}

    def scan_directory(self, filing_type, extract_from_files=True):
        """
        Scan directory for a specific filing type and collect metadata.

        Args:
            filing_type: Filing type to scan (e.g., '10-K')
            extract_from_files: Whether to open and parse HTML files for additional metadata
        """
        dir_path = os.path.join(self.raw_filings_dir, filing_type)

        if not os.path.exists(dir_path):
            print(f"❌ Directory not found: {dir_path}")
            return []

        print(f"\n📂 Scanning {filing_type} directory...")

        all_files = []

        # Walk through all subdirectories (including year folders)
        for root, dirs, files in os.walk(dir_path):
            for filename in files:
                if filename.endswith(('.htm', '.html', '.txt')):
                    all_files.append(os.path.join(root, filename))

        print(f"   Found {len(all_files):,} files")

        if len(all_files) == 0:
            return []

        # Extract metadata from each file
        metadata_list = []

        print(f"   Extracting metadata...")
        for filepath in tqdm(all_files, desc=f"   Processing {filing_type}"):
            # Get basic metadata from filename
            filename = os.path.basename(filepath)
            file_metadata = self.extract_metadata_from_filename(filename)

            if file_metadata is None:
                print(f"   ⚠️  Could not parse filename: {filename}")
                continue

            # Optionally extract additional metadata from file content
            if extract_from_files:
                file_content_metadata = self.extract_metadata_from_file(filepath)
                file_metadata.update(file_content_metadata)

            # Add file path info
            file_metadata['filepath'] = filepath

            metadata_list.append(file_metadata)

        return metadata_list

    def rebuild_metadata(self, filing_types, dry_run=False, extract_from_files=True):
        """
        Rebuild metadata for specified filing types.

        Args:
            filing_types: List of filing types to rebuild (e.g., ['10-K', '10-Q'])
            dry_run: If True, don't write to file, just show what would be done
            extract_from_files: Whether to parse HTML files for additional metadata
        """
        print("\n" + "=" * 70)
        print("REBUILDING METADATA FROM DISK")
        print("=" * 70)
        print(f"Base path: {self.base_path}")
        print(f"RAW filings: {self.raw_filings_dir}")

        # Backup existing metadata if it exists
        if os.path.exists(self.metadata_file) and not dry_run:
            print(f"\n💾 Backing up existing metadata to: {self.metadata_backup}")
            import shutil
            shutil.copy2(self.metadata_file, self.metadata_backup)

            # Load existing metadata
            existing_df = pd.read_csv(self.metadata_file)
            print(f"   Existing entries: {len(existing_df):,}")
        else:
            existing_df = pd.DataFrame()
            if not os.path.exists(self.metadata_file):
                print(f"\n⚠️  No existing metadata file found")

        # Scan all filing types
        all_metadata = []

        for filing_type in filing_types:
            metadata_list = self.scan_directory(filing_type, extract_from_files)
            all_metadata.extend(metadata_list)

        if len(all_metadata) == 0:
            print("\n❌ No files found to process")
            return

        # Create DataFrame
        new_df = pd.DataFrame(all_metadata)

        print(f"\n📊 Summary:")
        print(f"   Files scanned: {len(new_df):,}")
        print(f"   Unique CIKs: {new_df['CIK'].nunique():,}")

        if 'Type' in new_df.columns:
            print(f"\n   By Filing Type:")
            for filing_type, count in new_df['Type'].value_counts().items():
                print(f"      {filing_type}: {count:,}")

        if 'year' in new_df.columns:
            print(f"\n   By Year:")
            year_counts = new_df['year'].value_counts().sort_index()
            for year, count in year_counts.items():
                print(f"      {year}: {count:,}")

        # Compare with existing metadata
        if len(existing_df) > 0:
            # Identify new entries
            if 'accession_number' in existing_df.columns and 'accession_number' in new_df.columns:
                existing_accessions = set(existing_df['accession_number'].unique())
                new_accessions = set(new_df['accession_number'].unique())
                truly_new = new_accessions - existing_accessions

                print(f"\n🔍 Comparison with existing metadata:")
                print(f"   Existing tracked: {len(existing_accessions):,} filings")
                print(f"   Found on disk: {len(new_accessions):,} filings")
                print(f"   New discoveries: {len(truly_new):,} filings")
                print(f"   Missing from disk: {len(existing_accessions - new_accessions):,} filings")

        if dry_run:
            print(f"\n🔍 DRY RUN - No files were modified")
            print(f"\nFirst 10 entries that would be added:")
            if 'filepath' in new_df.columns:
                display_cols = ['CIK', 'Type', 'year', 'filename']
            else:
                display_cols = new_df.columns.tolist()[:4]
            print(new_df.head(10)[display_cols].to_string())
        else:
            # Merge with existing metadata (keep all from both, remove duplicates by accession_number)
            if len(existing_df) > 0 and 'accession_number' in existing_df.columns:
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                # Remove duplicates, keeping the newer entry (from new_df)
                combined_df = combined_df.drop_duplicates(subset=['accession_number'], keep='last')
            else:
                combined_df = new_df

            # Remove filepath column before saving (too long, not needed in metadata)
            if 'filepath' in combined_df.columns:
                combined_df = combined_df.drop('filepath', axis=1)

            # Save to file
            print(f"\n💾 Writing metadata to: {self.metadata_file}")
            combined_df.to_csv(self.metadata_file, index=False)
            print(f"   ✅ Saved {len(combined_df):,} total entries")

            # Also create a "DISCOVERED" file showing what was found
            new_df_save = new_df.drop('filepath', axis=1) if 'filepath' in new_df.columns else new_df
            new_df_save.to_csv(self.discovered_file, index=False)
            print(f"   ✅ Saved newly discovered entries to: {self.discovered_file}")

        print("\n" + "=" * 70)


# ============================================================================
# GOOGLE COLAB USAGE
# ============================================================================

def rebuild_for_colab(filing_types=['10-K'], fast_mode=False, dry_run=False):
    """
    Convenience function for Google Colab

    Args:
        filing_types: List of filing types to rebuild (default: ['10-K'])
        fast_mode: If True, skip parsing HTML files (faster)
        dry_run: If True, preview without modifying files
    """
    # Typical Colab path - adjust if yours is different
    base_path = '/content/drive/MyDrive/EDGAR_Project/edgar-crawler'

    # Check if path exists
    if not os.path.exists(base_path):
        print(f"❌ Path not found: {base_path}")
        print("\nPlease update the base_path to match your Google Drive structure.")
        print("Common paths:")
        print("  /content/drive/MyDrive/EDGAR_Project/edgar-crawler")
        print("  /content/drive/My Drive/EDGAR_Project/edgar-crawler")
        return None

    print(f"✅ Found directory: {base_path}")

    # Create rebuilder
    rebuilder = MetadataRebuilder(base_path)

    # Rebuild metadata
    extract_from_files = not fast_mode
    rebuilder.rebuild_metadata(
        filing_types=filing_types,
        dry_run=dry_run,
        extract_from_files=extract_from_files
    )

    return rebuilder


# Example usage in Colab:
# rebuild_for_colab(filing_types=['10-K'], fast_mode=False, dry_run=True)  # Preview
# rebuild_for_colab(filing_types=['10-K'], fast_mode=True, dry_run=False)  # Fast rebuild
