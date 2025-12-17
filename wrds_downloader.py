#!/usr/bin/env python3
"""
WRDS Identifier Downloader for EDGAR Crawler

This script downloads CIK and ticker information from WRDS COMPUSTAT
for all publicly traded companies in a specified date range.

Usage:
    python wrds_downloader.py [--start-year YYYY] [--end-year YYYY] [--output FILE]

Requirements:
    - WRDS account with COMPUSTAT access
    - wrds Python package installed
    - Proper authentication configured
"""

import wrds
import pandas as pd
import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

class WRDSDownloader:
    """Downloads firm identifiers from WRDS COMPUSTAT"""

    def __init__(self, wrds_username=None):
        """
        Initialize WRDS connection

        Args:
            wrds_username: WRDS username (optional, will use environment or prompt)
        """
        self.wrds_username = wrds_username or os.environ.get('WRDS_USERNAME')
        self.db = None

    def connect(self):
        """Establish connection to WRDS"""
        print("Connecting to WRDS...")
        try:
            self.db = wrds.Connection(wrds_username=self.wrds_username)
            print("✅ Successfully connected to WRDS")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to WRDS: {e}")
            print("\nTroubleshooting:")
            print("1. Check your WRDS credentials")
            print("2. Ensure you have COMPUSTAT access")
            print("3. Set WRDS_USERNAME environment variable or use --username flag")
            return False

    def get_compustat_identifiers(self, start_year=2010, end_year=None):
        """
        Download firm identifiers from COMPUSTAT

        Args:
            start_year: Starting year for data
            end_year: Ending year for data (defaults to current year)

        Returns:
            DataFrame with CIK, ticker, company name, and date ranges
        """
        if end_year is None:
            end_year = datetime.now().year

        print(f"\n📊 Querying COMPUSTAT for firms from {start_year} to {end_year}...")

        # Query COMPUSTAT for company identifiers
        # We use comp.company and comp.security tables
        query = f"""
        SELECT DISTINCT
            c.gvkey,
            c.cik,
            s.tic as ticker,
            c.conm as company_name,
            c.sic,
            c.state,
            c.fic as country,
            MIN(f.datadate) as first_date,
            MAX(f.datadate) as last_date
        FROM
            comp.company c
        INNER JOIN
            comp.security s ON c.gvkey = s.gvkey
        INNER JOIN
            comp.funda f ON c.gvkey = f.gvkey
        WHERE
            c.cik IS NOT NULL
            AND s.tic IS NOT NULL
            AND f.indfmt = 'INDL'
            AND f.datafmt = 'STD'
            AND f.popsrc = 'D'
            AND f.consol = 'C'
            AND EXTRACT(YEAR FROM f.datadate) BETWEEN {start_year} AND {end_year}
        GROUP BY
            c.gvkey, c.cik, s.tic, c.conm, c.sic, c.state, c.fic
        ORDER BY
            c.cik
        """

        try:
            df = self.db.raw_sql(query)
            print(f"✅ Retrieved {len(df)} unique firms from COMPUSTAT")
            return df
        except Exception as e:
            print(f"❌ Query failed: {e}")
            print("\nQuery used:")
            print(query)
            return None

    def clean_and_format(self, df):
        """
        Clean and format the downloaded data

        Args:
            df: Raw DataFrame from COMPUSTAT

        Returns:
            Cleaned DataFrame ready for EDGAR crawler
        """
        print("\n🧹 Cleaning and formatting data...")

        # Remove rows with missing CIK or ticker
        initial_count = len(df)
        df = df.dropna(subset=['cik', 'ticker'])
        print(f"   Removed {initial_count - len(df)} rows with missing CIK or ticker")

        # Format CIK (remove leading zeros for consistency, SEC uses various formats)
        df['cik'] = df['cik'].astype(int).astype(str)

        # Clean ticker (remove spaces, convert to uppercase)
        df['ticker'] = df['ticker'].str.strip().str.upper()

        # Remove duplicates (keep first occurrence)
        initial_count = len(df)
        df = df.drop_duplicates(subset=['cik'], keep='first')
        print(f"   Removed {initial_count - len(df)} duplicate CIKs")

        # Convert dates to string format
        df['first_date'] = pd.to_datetime(df['first_date']).dt.strftime('%Y-%m-%d')
        df['last_date'] = pd.to_datetime(df['last_date']).dt.strftime('%Y-%m-%d')

        # Reorder columns
        columns_order = [
            'cik', 'ticker', 'company_name', 'gvkey', 'sic',
            'state', 'country', 'first_date', 'last_date'
        ]
        df = df[columns_order]

        print(f"✅ Data cleaned: {len(df)} firms ready")

        return df

    def generate_summary_stats(self, df):
        """Generate and print summary statistics"""
        print("\n" + "="*60)
        print("SUMMARY STATISTICS")
        print("="*60)

        print(f"\n📊 Total Firms: {len(df):,}")

        # Date range
        print(f"\n📅 Date Range:")
        print(f"   Earliest filing: {df['first_date'].min()}")
        print(f"   Latest filing: {df['last_date'].max()}")

        # Industry distribution (top 10 SIC codes)
        print(f"\n🏭 Top 10 Industries (by SIC):")
        sic_counts = df['sic'].value_counts().head(10)
        for sic, count in sic_counts.items():
            print(f"   SIC {sic}: {count:,} firms")

        # Country distribution
        print(f"\n🌍 Country Distribution:")
        country_counts = df['country'].value_counts().head(10)
        for country, count in country_counts.items():
            country_name = country if pd.notna(country) else "Unknown"
            print(f"   {country_name}: {count:,} firms")

        # Sample records
        print(f"\n📋 Sample Records (first 5):")
        print(df[['cik', 'ticker', 'company_name', 'first_date', 'last_date']].head())

        print("\n" + "="*60)

    def save_to_file(self, df, output_file):
        """
        Save DataFrame to CSV file

        Args:
            df: DataFrame to save
            output_file: Path to output file
        """
        print(f"\n💾 Saving to {output_file}...")

        # Create directory if it doesn't exist
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Save to CSV
        df.to_csv(output_file, index=False)

        # Get file size
        file_size = output_path.stat().st_size
        file_size_mb = file_size / (1024 * 1024)

        print(f"✅ Saved {len(df):,} firms to {output_file}")
        print(f"   File size: {file_size_mb:.2f} MB")

        # Also save a simple CIK list for direct use with config.json
        cik_list_file = output_path.parent / "cik_list.txt"
        df['cik'].to_csv(cik_list_file, index=False, header=False)
        print(f"✅ Also saved CIK-only list to {cik_list_file}")

    def close(self):
        """Close WRDS connection"""
        if self.db:
            self.db.close()
            print("\n✅ WRDS connection closed")

    def run(self, start_year=2010, end_year=None, output_file='wrds_data/wrds_identifiers.csv'):
        """
        Main execution flow

        Args:
            start_year: Starting year for data
            end_year: Ending year for data
            output_file: Path to output CSV file
        """
        print("="*60)
        print("WRDS IDENTIFIER DOWNLOADER")
        print("="*60)

        # Connect to WRDS
        if not self.connect():
            return False

        # Download data
        df = self.get_compustat_identifiers(start_year, end_year)
        if df is None or len(df) == 0:
            print("❌ No data retrieved")
            return False

        # Clean and format
        df = self.clean_and_format(df)

        # Generate summary
        self.generate_summary_stats(df)

        # Save to file
        self.save_to_file(df, output_file)

        # Close connection
        self.close()

        print("\n" + "="*60)
        print("✅ DOWNLOAD COMPLETE!")
        print("="*60)
        print(f"\nNext steps:")
        print(f"1. Review the downloaded data: {output_file}")
        print(f"2. Optionally filter the data based on your criteria")
        print(f"3. Run colab_batch_downloader.py to download SEC filings")
        print(f"\nExample:")
        print(f"   python colab_batch_downloader.py --input {output_file}")

        return True


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Download firm identifiers from WRDS COMPUSTAT',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Download all firms from 2010 to present
    python wrds_downloader.py

    # Download firms from 2015 to 2023
    python wrds_downloader.py --start-year 2015 --end-year 2023

    # Specify custom output file
    python wrds_downloader.py --output my_firms.csv

    # Use specific WRDS username
    python wrds_downloader.py --username myusername
        """
    )

    parser.add_argument(
        '--start-year',
        type=int,
        default=2010,
        help='Starting year for data (default: 2010)'
    )

    parser.add_argument(
        '--end-year',
        type=int,
        default=None,
        help='Ending year for data (default: current year)'
    )

    parser.add_argument(
        '--output',
        type=str,
        default='wrds_data/wrds_identifiers.csv',
        help='Output CSV file path (default: wrds_data/wrds_identifiers.csv)'
    )

    parser.add_argument(
        '--username',
        type=str,
        default=None,
        help='WRDS username (default: use environment variable or prompt)'
    )

    args = parser.parse_args()

    # Create downloader instance
    downloader = WRDSDownloader(wrds_username=args.username)

    # Run the download
    success = downloader.run(
        start_year=args.start_year,
        end_year=args.end_year,
        output_file=args.output
    )

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
