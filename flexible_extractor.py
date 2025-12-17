#!/usr/bin/env python3
"""
Flexible Item Extractor for EDGAR Filings

This script extracts specific items from downloaded SEC filings with full flexibility.
You can extract any combination of items (MD&A, Risk Factors, Business, etc.) without
re-downloading the raw filings.

Usage:
    # Extract MD&A (Item 7) from 10-K filings
    python flexible_extractor.py --items 7 --output-dir EXTRACTED_FILINGS/Item_7_MDA

    # Extract Risk Factors (Item 1A)
    python flexible_extractor.py --items 1A --output-dir EXTRACTED_FILINGS/Item_1A_Risk

    # Extract multiple items at once
    python flexible_extractor.py --items 1,1A,7,7A --output-dir EXTRACTED_FILINGS/Multi_Items

    # Use a configuration file
    python flexible_extractor.py --config extraction_configs/mda_only.json

Requirements:
    - RAW filings must be already downloaded (use colab_batch_downloader.py first)
"""

import argparse
import json
import os
import sys
from pathlib import Path

# Import the original extraction module
import extract_items


class FlexibleExtractor:
    """
    Flexible extractor that can extract any items from downloaded filings
    """

    def __init__(
        self,
        items_to_extract=None,
        output_dir=None,
        filing_types=None,
        remove_tables=True,
        skip_existing=True,
    ):
        """
        Initialize the flexible extractor

        Args:
            items_to_extract: List of item numbers/names to extract (e.g., ['7', '1A'])
            output_dir: Custom output directory for extracted files
            filing_types: List of filing types to process (default: ['10-K'])
            remove_tables: Whether to remove HTML tables from extracted text
            skip_existing: Skip already-extracted filings
        """
        self.items_to_extract = items_to_extract or []
        self.output_dir = output_dir
        self.filing_types = filing_types or ["10-K"]
        self.remove_tables = remove_tables
        self.skip_existing = skip_existing

        self.config_file = "config.json"

    def validate_items(self):
        """Validate that requested items are valid"""

        # Valid 10-K items
        valid_10k_items = [
            "1",
            "1A",
            "1B",
            "1C",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "7A",
            "8",
            "9",
            "9A",
            "9B",
            "10",
            "11",
            "12",
            "13",
            "14",
            "15",
        ]

        # Valid 10-Q items (Part I and Part II items)
        valid_10q_items = [
            "part_1__1",
            "part_1__2",
            "part_1__3",
            "part_1__4",
            "part_2__1",
            "part_2__1A",
            "part_2__2",
            "part_2__3",
            "part_2__4",
            "part_2__5",
            "part_2__6",
        ]

        print(f"\n🔍 Validating items to extract: {self.items_to_extract}")

        for item in self.items_to_extract:
            # Normalize item (uppercase, remove spaces)
            item_normalized = str(item).upper().replace(" ", "")

            # Check if it's a valid 10-K item
            if "10-K" in self.filing_types and item_normalized not in valid_10k_items:
                # Check if it might be a 10-Q item
                if item not in valid_10q_items:
                    print(f"   ⚠️  Warning: '{item}' may not be a valid item number")
                    print(f"   Valid 10-K items: {', '.join(valid_10k_items)}")
                    print(f"   Valid 10-Q items: {', '.join(valid_10q_items)}")

        print(f"✅ Items validated")

    def update_config(self):
        """Update config.json with extraction settings"""
        print(f"\n⚙️  Updating config.json...")

        try:
            # Load existing config
            with open(self.config_file, "r") as f:
                config = json.load(f)

            # Update extract_items section
            config["extract_items"]["items_to_extract"] = self.items_to_extract
            config["extract_items"]["remove_tables"] = self.remove_tables
            config["extract_items"]["skip_extracted_filings"] = self.skip_existing

            # If custom output directory is specified, we'll handle it differently
            # (the original script uses hardcoded paths, so we'll need to work around this)

            # Save updated config
            with open(self.config_file, "w") as f:
                json.dump(config, f, indent=2)

            print(f"✅ Config updated:")
            print(f"   Items to extract: {self.items_to_extract}")
            print(f"   Remove tables: {self.remove_tables}")
            print(f"   Skip existing: {self.skip_existing}")

        except Exception as e:
            print(f"❌ Error updating config: {e}")
            raise


    def organize_output(self):
        """
        Organize extracted files into custom output directory if specified
        """
        if self.output_dir is None:
            return

        print(f"\n📁 Organizing output files...")

        import shutil

        # Original output directory
        original_dir = "datasets/EXTRACTED_FILINGS"

        # Create custom output directory
        custom_dir = f"datasets/{self.output_dir}"
        os.makedirs(custom_dir, exist_ok=True)

        # Copy files from each filing type folder to custom directory
        total_copied = 0
        for filing_type in self.filing_types:
            source_dir = os.path.join(original_dir, filing_type)

            if not os.path.exists(source_dir):
                print(f"   ⚠️  Source directory not found: {source_dir}")
                continue

            # Get all JSON files in source directory
            json_files = [f for f in os.listdir(source_dir) if f.endswith('.json')]

            # Copy each JSON file to custom directory
            for filename in json_files:
                source_path = os.path.join(source_dir, filename)
                dest_path = os.path.join(custom_dir, filename)

                # Copy file (skip if already exists)
                if not os.path.exists(dest_path):
                    shutil.copy2(source_path, dest_path)
                    total_copied += 1

            print(f"   ✅ Copied {len(json_files)} {filing_type} files")

        print(f"\n✅ Output organized in {custom_dir}")
        print(f"   Total files copied: {total_copied}")

    def generate_item_summary(self):
        """Display what items will be extracted and their descriptions"""
        print("\n" + "=" * 70)
        print("EXTRACTION SUMMARY")
        print("=" * 70)

        # Item descriptions
        item_descriptions = {
            "1": "Business",
            "1A": "Risk Factors",
            "1B": "Unresolved Staff Comments",
            "1C": "Cybersecurity",
            "2": "Properties",
            "3": "Legal Proceedings",
            "4": "Mine Safety Disclosures",
            "5": "Market for Registrant's Common Equity",
            "6": "Selected Financial Data (Reserved)",
            "7": "Management's Discussion and Analysis (MD&A)",
            "7A": "Quantitative and Qualitative Disclosures About Market Risk",
            "8": "Financial Statements and Supplementary Data",
            "9": "Changes in and Disagreements with Accountants",
            "9A": "Controls and Procedures",
            "9B": "Other Information",
            "10": "Directors, Executive Officers and Corporate Governance",
            "11": "Executive Compensation",
            "12": "Security Ownership of Certain Beneficial Owners and Management",
            "13": "Certain Relationships and Related Transactions",
            "14": "Principal Accounting Fees and Services",
            "15": "Exhibits, Financial Statement Schedules",
            "part_1__1": "10-Q Part I - Item 1: Financial Statements",
            "part_1__2": "10-Q Part I - Item 2: MD&A",
            "part_1__3": "10-Q Part I - Item 3: Quantitative and Qualitative Disclosures About Market Risk",
            "part_1__4": "10-Q Part I - Item 4: Controls and Procedures",
            "part_2__1": "10-Q Part II - Item 1: Legal Proceedings",
            "part_2__1A": "10-Q Part II - Item 1A: Risk Factors",
            "part_2__2": "10-Q Part II - Item 2: Unregistered Sales of Equity Securities",
            "part_2__6": "10-Q Part II - Item 6: Exhibits",
        }

        print(f"\n📋 Items to Extract:")
        for item in self.items_to_extract:
            description = item_descriptions.get(str(item).upper(), "Unknown Item")
            print(f"   Item {item}: {description}")

        print(f"\n📂 Filing Types: {', '.join(self.filing_types)}")
        print(f"🗑️  Remove Tables: {self.remove_tables}")
        print(f"⏭️  Skip Existing: {self.skip_existing}")

        print("\n" + "=" * 70)

    def run(self):
        """
        Main execution flow
        """
        print("=" * 70)
        print("FLEXIBLE ITEM EXTRACTOR")
        print("=" * 70)

        # Validate items
        if self.items_to_extract:
            self.validate_items()
        else:
            print(
                "\n⚠️  No items specified - will extract ALL items (this may take longer)"
            )

        # Generate summary
        self.generate_item_summary()

        # Update config
        self.update_config()

        # Run the original extraction
        print(f"\n🚀 Starting extraction process...")
        print(f"   (This may take several hours depending on the number of filings)\n")

        try:
            # Call the original main function
            extract_items.main()

            print(f"\n✅ Extraction completed successfully!")

            # Organize output if custom directory specified
            if self.output_dir:
                self.organize_output()

        except Exception as e:
            print(f"\n❌ Extraction failed: {e}")
            raise

        print("\n" + "=" * 70)
        print("✅ EXTRACTION COMPLETE")
        print("=" * 70)

        print(f"\nNext steps:")
        print(f"1. Check datasets/EXTRACTED_FILINGS/ for JSON files")
        print(f"2. Run consolidate_output.py to create CSV file")
        print(f"3. Begin your analysis!")

        return True


def load_config_file(config_path):
    """Load extraction configuration from JSON file"""
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
        return config
    except Exception as e:
        print(f"❌ Error loading config file: {e}")
        sys.exit(1)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Flexible extractor for SEC filing items",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Extract MD&A from 10-K (Item 7)
    python flexible_extractor.py --items 7

    # Extract Risk Factors (Item 1A)
    python flexible_extractor.py --items 1A

    # Extract multiple items
    python flexible_extractor.py --items 1,1A,7,7A

    # Extract MD&A from 10-Q
    python flexible_extractor.py --items part_1__2 --filing-types 10-Q

    # Use configuration file
    python flexible_extractor.py --config extraction_configs/mda_only.json

    # Extract all items (no filter)
    python flexible_extractor.py

Common Items:
    10-K:
        1    = Business
        1A   = Risk Factors
        7    = MD&A
        7A   = Market Risk Disclosures
        8    = Financial Statements

    10-Q:
        part_1__2  = MD&A
        part_1__1  = Financial Statements
        part_2__1A = Risk Factors
        """,
    )

    parser.add_argument(
        "--items",
        type=str,
        default=None,
        help="Comma-separated list of items to extract (e.g., '7' or '1,1A,7')",
    )

    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to extraction configuration JSON file",
    )

    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Custom output directory name (within datasets/)",
    )

    parser.add_argument(
        "--filing-types",
        type=str,
        default="10-K",
        help="Comma-separated filing types (default: 10-K)",
    )

    parser.add_argument(
        "--remove-tables",
        action="store_true",
        default=True,
        help="Remove HTML tables from extracted text (default: True)",
    )

    parser.add_argument(
        "--no-remove-tables",
        action="store_false",
        dest="remove_tables",
        help="Keep HTML tables in extracted text",
    )

    parser.add_argument(
        "--skip-existing",
        action="store_true",
        default=True,
        help="Skip already-extracted filings (default: True)",
    )

    parser.add_argument(
        "--no-skip-existing",
        action="store_false",
        dest="skip_existing",
        help="Re-extract even if files already exist",
    )

    args = parser.parse_args()

    # Load from config file if specified
    if args.config:
        config = load_config_file(args.config)

        items_to_extract = config.get("items_to_extract", [])
        output_dir = config.get("output_dir", None)
        filing_types = config.get("filing_types", ["10-K"])
        remove_tables = config.get("remove_tables", True)
        skip_existing = config.get("skip_existing", True)

    else:
        # Use command-line arguments
        items_to_extract = (
            [item.strip() for item in args.items.split(",")] if args.items else []
        )
        output_dir = args.output_dir
        filing_types = [ft.strip() for ft in args.filing_types.split(",")]
        remove_tables = args.remove_tables
        skip_existing = args.skip_existing

    # Create extractor instance
    extractor = FlexibleExtractor(
        items_to_extract=items_to_extract,
        output_dir=output_dir,
        filing_types=filing_types,
        remove_tables=remove_tables,
        skip_existing=skip_existing,
    )

    # Run extraction
    success = extractor.run()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
