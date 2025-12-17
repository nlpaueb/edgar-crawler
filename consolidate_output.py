#!/usr/bin/env python3
"""
Consolidate EDGAR Extraction Output

This script consolidates extracted JSON files into a single CSV file for easy analysis.

Usage:
    # Consolidate MD&A (Item 7) from 10-K filings
    python consolidate_output.py --item 7 --filing-type 10-K --output mda_analysis.csv

    # Consolidate Risk Factors (Item 1A)
    python consolidate_output.py --item 1A --filing-type 10-K --output risk_factors.csv

    # Consolidate multiple items into separate columns
    python consolidate_output.py --items 1,1A,7,7A --filing-type 10-K --output multi_items.csv

    # Consolidate all items
    python consolidate_output.py --all --filing-type 10-K --output all_items.csv
"""

import argparse
import json
import os
import sys
from pathlib import Path

import pandas as pd
from tqdm import tqdm


class OutputConsolidator:
    """Consolidates extracted JSON files into CSV"""

    def __init__(self, filing_type="10-K", output_file="consolidated_output.csv"):
        """
        Initialize consolidator

        Args:
            filing_type: Type of filing ('10-K', '10-Q', etc.)
            output_file: Path to output CSV file
        """
        self.filing_type = filing_type
        self.output_file = output_file
        self.extracted_dir = f"datasets/EXTRACTED_FILINGS/{filing_type}"

    def get_item_column_name(self, item):
        """
        Convert item number to JSON key name

        Args:
            item: Item number (e.g., '7', '1A')

        Returns:
            Column name in JSON (e.g., 'item_7', 'item_1a')
        """
        # Handle 10-Q items (e.g., 'part_1__2')
        if "part_" in str(item).lower():
            return item.lower()

        # Handle 10-K items
        item_normalized = str(item).lower().replace(" ", "")
        return f"item_{item_normalized}"

    def load_json_files(self, item=None, items=None, load_all=False):
        """
        Load JSON files from extracted directory

        Args:
            item: Single item to extract (e.g., '7')
            items: List of items to extract (e.g., ['1', '1A', '7'])
            load_all: If True, load all items from all files

        Returns:
            DataFrame with consolidated data
        """

        if not os.path.exists(self.extracted_dir):
            print(f"❌ Directory not found: {self.extracted_dir}")
            return None

        # Get list of JSON files
        json_files = list(Path(self.extracted_dir).glob("*.json"))

        if len(json_files) == 0:
            print(f"❌ No JSON files found in {self.extracted_dir}")
            return None

        print(f"\n📂 Found {len(json_files)} JSON files")

        # Determine which items to extract
        if item:
            items_to_extract = [item]
        elif items:
            items_to_extract = items
        else:
            items_to_extract = None  # Extract all

        print(f"📋 Loading data...")
        if items_to_extract:
            print(f"   Items to extract: {', '.join(items_to_extract)}")
        else:
            print(f"   Extracting ALL items")

        records = []

        for json_file in tqdm(json_files, desc="Processing files"):
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)

                # Base record with metadata
                record = {
                    "cik": data.get("cik"),
                    "company_name": data.get("company"),
                    "ticker": data.get("ticker", ""),
                    "filing_type": data.get("filing_type"),
                    "filing_date": data.get("filing_date"),
                    "period_of_report": data.get("period_of_report"),
                    "fiscal_year_end": data.get("fiscal_year_end"),
                    "sic": data.get("sic"),
                    "state_of_inc": data.get("state_of_inc"),
                    "filename": data.get("filename"),
                    "filing_url": data.get("filing_html_index"),
                }

                # Extract specified items
                if items_to_extract:
                    for item_num in items_to_extract:
                        col_name = self.get_item_column_name(item_num)
                        item_text = data.get(col_name, "")
                        record[f"item_{item_num}"] = item_text
                else:
                    # Extract all items
                    for key, value in data.items():
                        if key.startswith("item_") or key.startswith("part_"):
                            record[key] = value

                records.append(record)

            except json.JSONDecodeError as e:
                print(f"\n⚠️  Error decoding {json_file.name}: {e}")
                continue
            except Exception as e:
                print(f"\n⚠️  Error processing {json_file.name}: {e}")
                continue

        if len(records) == 0:
            print("❌ No records extracted")
            return None

        # Create DataFrame
        df = pd.DataFrame(records)

        print(f"\n✅ Loaded {len(df)} records")

        return df

    def add_derived_columns(self, df):
        """
        Add derived columns for analysis convenience

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with additional columns
        """
        print("\n🔧 Adding derived columns...")

        # Extract year from filing_date
        if "filing_date" in df.columns:
            df["filing_year"] = pd.to_datetime(df["filing_date"]).dt.year

        # Extract year from period_of_report
        if "period_of_report" in df.columns:
            df["fiscal_year"] = pd.to_datetime(df["period_of_report"]).dt.year

        # Text length for each item (useful for quality checks)
        item_cols = [col for col in df.columns if col.startswith("item_") or col.startswith("part_")]

        for col in item_cols:
            df[f"{col}_length"] = df[col].str.len()

        print(f"   Added {len(item_cols) + 2} derived columns")

        return df

    def save_to_csv(self, df):
        """
        Save DataFrame to CSV file

        Args:
            df: DataFrame to save
        """
        print(f"\n💾 Saving to {self.output_file}...")

        # Create output directory if needed
        output_path = Path(self.output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Save to CSV
        df.to_csv(self.output_file, index=False, encoding="utf-8")

        # Get file size
        file_size = output_path.stat().st_size
        file_size_mb = file_size / (1024 * 1024)

        print(f"✅ Saved {len(df)} records to {self.output_file}")
        print(f"   File size: {file_size_mb:.2f} MB")
        print(f"   Columns: {len(df.columns)}")

    def generate_summary_stats(self, df):
        """Generate summary statistics"""
        print("\n" + "=" * 70)
        print("SUMMARY STATISTICS")
        print("=" * 70)

        print(f"\n📊 Dataset Overview:")
        print(f"   Total records: {len(df):,}")
        print(f"   Total columns: {len(df.columns)}")

        if "cik" in df.columns:
            print(f"   Unique firms: {df['cik'].nunique():,}")

        if "filing_year" in df.columns:
            print(f"\n📅 Date Range:")
            print(f"   Earliest filing: {df['filing_year'].min()}")
            print(f"   Latest filing: {df['filing_year'].max()}")

            print(f"\n📈 Filings by Year:")
            year_counts = df["filing_year"].value_counts().sort_index()
            for year, count in year_counts.items():
                print(f"   {year}: {count:,}")

        # Item statistics
        item_cols = [col for col in df.columns if (col.startswith("item_") or col.startswith("part_")) and not col.endswith("_length")]

        if len(item_cols) > 0:
            print(f"\n📋 Extracted Items:")
            for col in item_cols:
                non_empty = df[col].notna() & (df[col] != "")
                count = non_empty.sum()
                pct = count / len(df) * 100

                # Get average length
                if f"{col}_length" in df.columns:
                    avg_length = df.loc[non_empty, f"{col}_length"].mean()
                    print(
                        f"   {col}: {count:,} ({pct:.1f}%) | Avg length: {avg_length:,.0f} chars"
                    )
                else:
                    print(f"   {col}: {count:,} ({pct:.1f}%)")

        # Data quality checks
        print(f"\n🔍 Data Quality:")

        if "cik" in df.columns:
            missing_cik = df["cik"].isna().sum()
            print(f"   Missing CIK: {missing_cik}")

        if "filing_date" in df.columns:
            missing_date = df["filing_date"].isna().sum()
            print(f"   Missing filing date: {missing_date}")

        # Check for empty item content
        for col in item_cols:
            empty_count = ((df[col].isna()) | (df[col] == "")).sum()
            if empty_count > 0:
                print(f"   Empty {col}: {empty_count} ({empty_count/len(df)*100:.1f}%)")

        print("\n" + "=" * 70)

    def run(self, item=None, items=None, load_all=False):
        """
        Main execution flow

        Args:
            item: Single item to consolidate
            items: List of items to consolidate
            load_all: Load all available items
        """
        print("=" * 70)
        print("OUTPUT CONSOLIDATOR")
        print("=" * 70)
        print(f"\nConfiguration:")
        print(f"  Filing type: {self.filing_type}")
        print(f"  Output file: {self.output_file}")

        # Load JSON files
        df = self.load_json_files(item=item, items=items, load_all=load_all)

        if df is None or len(df) == 0:
            print("\n❌ No data to consolidate")
            return False

        # Add derived columns
        df = self.add_derived_columns(df)

        # Generate summary
        self.generate_summary_stats(df)

        # Save to CSV
        self.save_to_csv(df)

        print("\n" + "=" * 70)
        print("✅ CONSOLIDATION COMPLETE")
        print("=" * 70)

        print(f"\nNext steps:")
        print(f"1. Load the CSV file: pd.read_csv('{self.output_file}')")
        print(f"2. Begin your analysis!")

        return True


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Consolidate extracted EDGAR items into CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Consolidate MD&A (Item 7) from 10-K
    python consolidate_output.py --item 7 --output mda_analysis.csv

    # Consolidate Risk Factors (Item 1A)
    python consolidate_output.py --item 1A --output risk_factors.csv

    # Consolidate multiple items
    python consolidate_output.py --items 1,1A,7,7A --output multi_items.csv

    # Consolidate all items
    python consolidate_output.py --all --output all_items.csv

    # Consolidate 10-Q MD&A
    python consolidate_output.py --item part_1__2 --filing-type 10-Q --output 10q_mda.csv
        """,
    )

    parser.add_argument("--item", type=str, help="Single item to extract (e.g., '7')")

    parser.add_argument(
        "--items", type=str, help="Comma-separated items (e.g., '1,1A,7,7A')"
    )

    parser.add_argument("--all", action="store_true", help="Extract all available items")

    parser.add_argument(
        "--filing-type", type=str, default="10-K", help="Filing type (default: 10-K)"
    )

    parser.add_argument(
        "--output",
        type=str,
        default="consolidated_output.csv",
        help="Output CSV file path",
    )

    args = parser.parse_args()

    # Validate arguments
    if not args.item and not args.items and not args.all:
        print(
            "❌ Error: Must specify --item, --items, or --all"
        )
        parser.print_help()
        sys.exit(1)

    # Parse items if provided
    items_list = None
    if args.items:
        items_list = [item.strip() for item in args.items.split(",")]

    # Create consolidator
    consolidator = OutputConsolidator(
        filing_type=args.filing_type, output_file=args.output
    )

    # Run consolidation
    success = consolidator.run(item=args.item, items=items_list, load_all=args.all)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
