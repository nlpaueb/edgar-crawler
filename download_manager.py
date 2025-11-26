#!/usr/bin/env python3
"""
Download Manager for EDGAR Crawler

This script helps you manage and track your EDGAR downloads:
- View inventory of downloaded filings
- Check what's been extracted
- Identify missing filings
- Generate summary reports
- Clean up storage

Usage:
    # View inventory
    python download_manager.py --inventory

    # Check specific firm's filings
    python download_manager.py --check-cik 320193

    # Find missing filings for all firms
    python download_manager.py --find-missing

    # Generate detailed report
    python download_manager.py --report --output report.txt
"""

import argparse
import json
import os
import sys
from pathlib import Path

import pandas as pd
from tqdm import tqdm


class DownloadManager:
    """Manages and tracks EDGAR downloads"""

    def __init__(self):
        self.raw_filings_dir = "datasets/RAW_FILINGS"
        self.extracted_filings_dir = "datasets/EXTRACTED_FILINGS"
        self.metadata_file = "datasets/FILINGS_METADATA.csv"
        self.wrds_file = "wrds_data/wrds_identifiers.csv"
        self.progress_file = "logs/download_progress.csv"

    def _count_files_recursive(self, dir_path, extension=None):
        """
        Count files recursively in a directory.
        Handles year-based subdirectory structure.

        Args:
            dir_path: Directory to count files in
            extension: Optional file extension filter (e.g., '.json')

        Returns:
            int: Total number of files
        """
        count = 0
        try:
            for entry in os.scandir(dir_path):
                if entry.is_file():
                    if extension is None or entry.name.endswith(extension):
                        count += 1
                elif entry.is_dir():
                    # Recursively count files in subdirectories
                    count += self._count_files_recursive(entry.path, extension)
        except OSError as e:
            # If we can't list the directory, use metadata as fallback
            pass
        return count

    def _count_unique_filings(self, dir_path):
        """
        Count unique filings (not all files) in a directory.
        Each filing is identified by its unique accession number.

        Filename pattern: {CIK}_{filing_type}_{year}_{accession_num}.{ext}

        Args:
            dir_path: Directory to count filings in

        Returns:
            int: Number of unique filings
        """
        unique_accessions = set()
        try:
            for entry in os.scandir(dir_path):
                if entry.is_file():
                    # Extract accession number from filename
                    # Format: {CIK}_{type}_{year}_{accession}.{ext}
                    parts = entry.name.rsplit('.', 1)[0].split('_')
                    if len(parts) >= 4:
                        accession = parts[-1]  # Last part is accession number
                        unique_accessions.add(accession)
                elif entry.is_dir():
                    # Recursively process subdirectories (year folders)
                    unique_accessions.update(self._get_accessions_recursive(entry.path))
        except OSError:
            pass
        return len(unique_accessions)

    def _get_accessions_recursive(self, dir_path):
        """Helper to recursively get all unique accession numbers"""
        accessions = set()
        try:
            for entry in os.scandir(dir_path):
                if entry.is_file():
                    parts = entry.name.rsplit('.', 1)[0].split('_')
                    if len(parts) >= 4:
                        accessions.add(parts[-1])
                elif entry.is_dir():
                    accessions.update(self._get_accessions_recursive(entry.path))
        except OSError:
            pass
        return accessions

    def check_setup(self):
        """Check if necessary files and directories exist"""
        print("Checking setup...\n")

        checks = {
            "RAW filings directory": os.path.exists(self.raw_filings_dir),
            "EXTRACTED filings directory": os.path.exists(self.extracted_filings_dir),
            "Metadata file": os.path.exists(self.metadata_file),
            "WRDS identifiers": os.path.exists(self.wrds_file),
            "Progress tracker": os.path.exists(self.progress_file),
        }

        all_good = True
        for name, exists in checks.items():
            status = "✅" if exists else "❌"
            print(f"{status} {name}")
            if not exists:
                all_good = False

        return all_good

    def get_storage_usage(self):
        """Calculate storage usage"""

        def get_dir_size(path):
            """Get total size of directory"""
            total = 0
            try:
                for entry in os.scandir(path):
                    if entry.is_file():
                        total += entry.stat().st_size
                    elif entry.is_dir():
                        total += get_dir_size(entry.path)
            except Exception as e:
                pass
            return total

        storage = {}

        if os.path.exists(self.raw_filings_dir):
            storage["RAW filings"] = get_dir_size(self.raw_filings_dir)

        if os.path.exists(self.extracted_filings_dir):
            storage["EXTRACTED filings"] = get_dir_size(self.extracted_filings_dir)

        return storage

    def show_inventory(self):
        """Display inventory of downloads"""
        print("\n" + "=" * 70)
        print("EDGAR DOWNLOAD INVENTORY")
        print("=" * 70)

        # Storage usage
        print("\n💾 Storage Usage:")
        storage = self.get_storage_usage()
        total_gb = 0
        for name, size_bytes in storage.items():
            size_gb = size_bytes / (1024**3)
            total_gb += size_gb
            print(f"   {name}: {size_gb:.2f} GB")
        print(f"   Total: {total_gb:.2f} GB")

        # Metadata summary
        if os.path.exists(self.metadata_file):
            print("\n📊 Downloaded Filings (from metadata):")
            print("   [Successfully downloaded and recorded in FILINGS_METADATA.csv]")
            try:
                metadata_df = pd.read_csv(self.metadata_file)
                print(f"   Total filings: {len(metadata_df):,}")

                # By filing type
                if "Type" in metadata_df.columns:
                    print(f"\n   By Filing Type:")
                    for filing_type, count in (
                        metadata_df["Type"].value_counts().items()
                    ):
                        print(f"      {filing_type}: {count:,}")

                # By year
                if "filing_date" in metadata_df.columns:
                    metadata_df["year"] = pd.to_datetime(
                        metadata_df["filing_date"]
                    ).dt.year
                    print(f"\n   By Year:")
                    year_counts = metadata_df["year"].value_counts().sort_index()
                    for year, count in year_counts.items():
                        print(f"      {year}: {count:,}")

                # Unique firms
                if "CIK" in metadata_df.columns:
                    unique_firms = metadata_df["CIK"].nunique()
                    print(f"\n   Unique Firms: {unique_firms:,}")

            except Exception as e:
                print(f"   ⚠️  Could not read metadata: {e}")

        # RAW filings count
        print("\n📁 RAW Filings (on disk):")
        print("   [Shows files in RAW_FILINGS directory]")
        if os.path.exists(self.raw_filings_dir):
            for filing_type in ["10-K", "10-Q", "8-K"]:
                dir_path = os.path.join(self.raw_filings_dir, filing_type)
                if os.path.exists(dir_path):
                    total_files = self._count_files_recursive(dir_path)
                    unique_filings = self._count_unique_filings(dir_path)

                    if unique_filings > 0:
                        files_per_filing = total_files / unique_filings
                        print(f"   {filing_type}: {unique_filings:,} unique filings ({total_files:,} total files, ~{files_per_filing:.1f} files/filing)")
                    else:
                        print(f"   {filing_type}: {total_files:,} total files")

        # EXTRACTED filings count
        print("\n📄 EXTRACTED Filings:")
        if os.path.exists(self.extracted_filings_dir):
            for filing_type in ["10-K", "10-Q", "8-K"]:
                dir_path = os.path.join(self.extracted_filings_dir, filing_type)
                if os.path.exists(dir_path):
                    count = self._count_files_recursive(dir_path, extension=".json")
                    print(f"   {filing_type}: {count:,} JSON files")

        # Batch progress
        if os.path.exists(self.progress_file):
            print("\n📈 Batch Download Progress:")
            try:
                progress_df = pd.read_csv(self.progress_file)
                total = len(progress_df)
                completed = len(progress_df[progress_df["status"] == "completed"])
                failed = len(progress_df[progress_df["status"] == "failed"])
                pending = total - completed - failed

                print(f"   Total batches: {total}")
                print(f"   ✅ Completed: {completed} ({completed/total*100:.1f}%)")
                if failed > 0:
                    print(f"   ❌ Failed: {failed}")
                if pending > 0:
                    print(f"   ⏳ Pending: {pending}")

            except Exception as e:
                print(f"   ⚠️  Could not read progress: {e}")

        # Compare metadata vs disk
        if os.path.exists(self.metadata_file) and os.path.exists(self.raw_filings_dir):
            print("\n🔍 Metadata vs Disk Comparison:")
            try:
                metadata_df = pd.read_csv(self.metadata_file)
                for filing_type in ["10-K", "10-Q", "8-K"]:
                    metadata_count = len(metadata_df[metadata_df["Type"] == filing_type]) if "Type" in metadata_df.columns else 0
                    dir_path = os.path.join(self.raw_filings_dir, filing_type)
                    if os.path.exists(dir_path):
                        disk_count = self._count_unique_filings(dir_path)
                        diff = disk_count - metadata_count
                        status = "✅" if diff == 0 else ("⚠️" if abs(diff) < 100 else "❌")

                        if metadata_count > 0 or disk_count > 0:
                            print(f"   {status} {filing_type}: Metadata={metadata_count:,}, Disk={disk_count:,} (diff: {diff:+,})")
            except Exception as e:
                print(f"   ⚠️  Could not compare: {e}")

        # Add explanatory notes
        print("\n" + "ℹ️  Notes:")
        print("   • 'Downloaded Filings' = Successfully downloaded filings in metadata")
        print("   • 'RAW Filings' = Files on disk (unique filings + total files)")
        print("   • Metadata vs Disk shows if counts match (ideally should be equal)")
        print("   • If Disk > Metadata: Extra files on disk (duplicates/failed downloads)")
        print("   • If Metadata > Disk: Missing files (deleted or moved)")

        print("\n" + "=" * 70)

    def check_firm(self, cik):
        """Check filings for a specific firm"""
        print(f"\n{'='*70}")
        print(f"FIRM DETAILS: CIK {cik}")
        print(f"{'='*70}")

        if not os.path.exists(self.metadata_file):
            print("❌ Metadata file not found")
            return

        try:
            metadata_df = pd.read_csv(self.metadata_file)
            metadata_df["CIK"] = metadata_df["CIK"].astype(str)

            firm_filings = metadata_df[metadata_df["CIK"] == str(cik)]

            if len(firm_filings) == 0:
                print(f"⚠️  No filings found for CIK {cik}")
                return

            print(f"\n📊 Total Filings: {len(firm_filings)}")

            # By type
            print(f"\nBy Filing Type:")
            for filing_type, count in firm_filings["Type"].value_counts().items():
                print(f"   {filing_type}: {count}")

            # Date range
            if "filing_date" in firm_filings.columns:
                dates = pd.to_datetime(firm_filings["filing_date"])
                print(f"\nDate Range:")
                print(f"   Earliest: {dates.min().strftime('%Y-%m-%d')}")
                print(f"   Latest: {dates.max().strftime('%Y-%m-%d')}")

            # Recent filings
            print(f"\n📋 Recent Filings (last 10):")
            recent = firm_filings.sort_values("filing_date", ascending=False).head(10)
            for _, row in recent.iterrows():
                print(
                    f"   {row['filing_date']} - {row['Type']} - {row.get('Filename', 'N/A')}"
                )

        except Exception as e:
            print(f"❌ Error: {e}")

    def find_missing_filings(self):
        """Identify firms with missing filings"""
        print(f"\n{'='*70}")
        print("FINDING MISSING FILINGS")
        print(f"{'='*70}")

        if not os.path.exists(self.wrds_file):
            print("❌ WRDS identifiers file not found")
            return

        if not os.path.exists(self.metadata_file):
            print("❌ Metadata file not found")
            return

        print("\n⏳ Analyzing firms...")

        try:
            # Load WRDS firms
            wrds_df = pd.read_csv(self.wrds_file)
            wrds_df["cik"] = wrds_df["cik"].astype(str)

            # Load metadata
            metadata_df = pd.read_csv(self.metadata_file)
            metadata_df["CIK"] = metadata_df["CIK"].astype(str)

            # Find firms with no downloads
            downloaded_ciks = set(metadata_df["CIK"].unique())
            all_ciks = set(wrds_df["cik"].unique())
            missing_ciks = all_ciks - downloaded_ciks

            print(f"\n📊 Summary:")
            print(f"   Total firms in WRDS list: {len(all_ciks):,}")
            print(f"   Firms with downloads: {len(downloaded_ciks):,}")
            print(f"   Firms with NO downloads: {len(missing_ciks):,}")

            if len(missing_ciks) > 0:
                print(f"\n⚠️  Firms with no downloads (showing first 20):")
                missing_firms = wrds_df[wrds_df["cik"].isin(missing_ciks)].head(20)
                for _, row in missing_firms.iterrows():
                    ticker = row.get("ticker", "N/A")
                    company = row.get("company_name", "N/A")
                    print(f"   CIK {row['cik']}: {ticker} - {company}")

                # Save full list
                missing_file = "logs/missing_firms.csv"
                wrds_df[wrds_df["cik"].isin(missing_ciks)].to_csv(
                    missing_file, index=False
                )
                print(f"\n💾 Full list saved to: {missing_file}")

            # Find firms with incomplete date ranges
            print(f"\n📅 Checking date completeness...")

            # Load config to get expected year range
            try:
                with open("config.json", "r") as f:
                    config = json.load(f)
                start_year = config["download_filings"]["start_year"]
                end_year = config["download_filings"]["end_year"]
                filing_types = config["download_filings"]["filing_types"]

                print(
                    f"   Expected range: {start_year}-{end_year} for {', '.join(filing_types)}"
                )

                # Check each firm
                metadata_df["year"] = pd.to_datetime(metadata_df["filing_date"]).dt.year
                incomplete_firms = []

                for cik in tqdm(downloaded_ciks, desc="Checking firms"):
                    firm_filings = metadata_df[metadata_df["CIK"] == cik]

                    for filing_type in filing_types:
                        type_filings = firm_filings[firm_filings["Type"] == filing_type]

                        if len(type_filings) > 0:
                            years_present = set(type_filings["year"].unique())
                            expected_years = set(range(start_year, end_year + 1))
                            missing_years = expected_years - years_present

                            if len(missing_years) > 0:
                                incomplete_firms.append(
                                    {
                                        "cik": cik,
                                        "filing_type": filing_type,
                                        "missing_years": sorted(missing_years),
                                        "years_present": len(years_present),
                                        "years_expected": len(expected_years),
                                    }
                                )

                if len(incomplete_firms) > 0:
                    print(
                        f"\n⚠️  Found {len(incomplete_firms)} firm-filing type combinations with missing years"
                    )
                    print(f"   (Showing first 10):")
                    for firm in incomplete_firms[:10]:
                        print(
                            f"   CIK {firm['cik']} - {firm['filing_type']}: Missing {len(firm['missing_years'])} years"
                        )

                    # Save to file
                    incomplete_file = "logs/incomplete_firms.csv"
                    pd.DataFrame(incomplete_firms).to_csv(incomplete_file, index=False)
                    print(f"\n💾 Full list saved to: {incomplete_file}")

            except Exception as e:
                print(f"   ⚠️  Could not check date completeness: {e}")

        except Exception as e:
            print(f"❌ Error: {e}")

        print(f"\n{'='*70}")

    def generate_report(self, output_file=None):
        """Generate comprehensive report"""

        if output_file:
            # Redirect stdout to file
            original_stdout = sys.stdout
            sys.stdout = open(output_file, "w")

        print("EDGAR CRAWLER - COMPREHENSIVE REPORT")
        print(f"Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)

        # Run all checks
        print("\n## SETUP CHECK")
        self.check_setup()

        print("\n## INVENTORY")
        self.show_inventory()

        print("\n## MISSING FILINGS ANALYSIS")
        self.find_missing_filings()

        if output_file:
            sys.stdout.close()
            sys.stdout = original_stdout
            print(f"\n✅ Report saved to: {output_file}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Download manager for EDGAR crawler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # View inventory of downloads
    python download_manager.py --inventory

    # Check specific firm
    python download_manager.py --check-cik 320193

    # Find missing filings
    python download_manager.py --find-missing

    # Generate full report
    python download_manager.py --report --output report.txt
        """,
    )

    parser.add_argument(
        "--inventory", action="store_true", help="Show inventory of downloads"
    )

    parser.add_argument("--check-cik", type=str, help="Check filings for specific CIK")

    parser.add_argument(
        "--find-missing", action="store_true", help="Find firms with missing filings"
    )

    parser.add_argument(
        "--report", action="store_true", help="Generate comprehensive report"
    )

    parser.add_argument("--output", type=str, help="Output file for report")

    args = parser.parse_args()

    # Create manager
    manager = DownloadManager()

    # Execute requested action
    if args.inventory:
        manager.show_inventory()

    elif args.check_cik:
        manager.check_firm(args.check_cik)

    elif args.find_missing:
        manager.find_missing_filings()

    elif args.report:
        manager.generate_report(args.output)

    else:
        # Default: show inventory
        manager.show_inventory()


if __name__ == "__main__":
    main()
