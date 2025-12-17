#!/usr/bin/env python3
"""
Fast Flexible Item Extractor for EDGAR Filings (Multi-Process)

This is an optimized version that uses multiple CPU cores for faster extraction.
Especially useful for Google Colab with time-limited sessions.

Usage:
    python flexible_extractor_fast.py --config extraction_configs/items_1_1a.json --processes 3
"""

import argparse
import json
import os
import sys
from pathlib import Path

# Import the original extraction module and our fast version
import extract_items
from extract_items_fast import main_fast


class FlexibleExtractorFast:
    """
    Fast flexible extractor using multiple processes
    """

    def __init__(
        self,
        items_to_extract=None,
        output_dir=None,
        filing_types=None,
        remove_tables=True,
        skip_existing=True,
        num_processes=2,
    ):
        """
        Initialize the fast flexible extractor

        Args:
            items_to_extract: List of item numbers/names to extract (e.g., ['7', '1A'])
            output_dir: Custom output directory for extracted files
            filing_types: List of filing types to process (default: ['10-K'])
            remove_tables: Whether to remove HTML tables from extracted text
            skip_existing: Skip already-extracted filings
            num_processes: Number of parallel processes (default: 2)
        """
        self.items_to_extract = items_to_extract or []
        self.output_dir = output_dir
        self.filing_types = filing_types or ["10-K"]
        self.remove_tables = remove_tables
        self.skip_existing = skip_existing
        self.num_processes = num_processes

        self.config_file = "config.json"

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

            # Save updated config
            with open(self.config_file, "w") as f:
                json.dump(config, f, indent=2)

            print(f"✅ Config updated:")
            print(f"   Items to extract: {self.items_to_extract}")
            print(f"   Remove tables: {self.remove_tables}")
            print(f"   Skip existing: {self.skip_existing}")
            print(f"   Parallel processes: {self.num_processes}")

        except Exception as e:
            print(f"❌ Error updating config: {e}")
            raise

    def generate_summary(self):
        """Display extraction summary"""
        print("\n" + "=" * 70)
        print("FAST EXTRACTION SUMMARY")
        print("=" * 70)

        item_descriptions = {
            "1": "Business",
            "1A": "Risk Factors",
            "7": "Management's Discussion and Analysis (MD&A)",
            "7A": "Quantitative and Qualitative Disclosures About Market Risk",
            "8": "Financial Statements and Supplementary Data",
        }

        print(f"\n📋 Items to Extract:")
        for item in self.items_to_extract:
            description = item_descriptions.get(str(item).upper(), "Unknown Item")
            print(f"   Item {item}: {description}")

        print(f"\n📂 Filing Types: {', '.join(self.filing_types)}")
        print(f"🗑️  Remove Tables: {self.remove_tables}")
        print(f"⏭️  Skip Existing: {self.skip_existing}")
        print(f"⚡ Parallel Processes: {self.num_processes}")

        print("\n" + "=" * 70)

    def run(self):
        """
        Main execution flow
        """
        print("=" * 70)
        print("FAST FLEXIBLE ITEM EXTRACTOR (Multi-Process)")
        print("=" * 70)

        # Generate summary
        self.generate_summary()

        # Update config
        self.update_config()

        # Run the fast extraction
        print(f"\n🚀 Starting FAST extraction process...")
        print(f"   Using {self.num_processes} parallel processes")
        print(f"   (This will be {self.num_processes}x faster than single-process)\n")

        try:
            # Call the fast main function
            main_fast(num_processes=self.num_processes)

            print(f"\n✅ Extraction completed successfully!")

        except Exception as e:
            print(f"\n❌ Extraction failed: {e}")
            raise

        print("\n" + "=" * 70)
        print("✅ EXTRACTION COMPLETE")
        print("=" * 70)

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
        description="Fast flexible extractor for SEC filing items (Multi-Process)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Extract Items 1 & 1A with 3 processes
    python flexible_extractor_fast.py --config extraction_configs/items_1_1a.json --processes 3

    # Extract MD&A with 2 processes
    python flexible_extractor_fast.py --items 7 --processes 2

    # Extract multiple items with auto-detected CPU count
    python flexible_extractor_fast.py --items 1,1A,7,7A --auto-processes
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
        "--processes",
        type=int,
        default=2,
        help="Number of parallel processes (default: 2)",
    )

    parser.add_argument(
        "--auto-processes",
        action="store_true",
        help="Auto-detect CPU count and use optimal number of processes",
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
        "--skip-existing",
        action="store_true",
        default=True,
        help="Skip already-extracted filings (default: True)",
    )

    args = parser.parse_args()

    # Auto-detect CPU count if requested
    if args.auto_processes:
        import multiprocessing
        cpu_count = multiprocessing.cpu_count()
        # Use n-1 cores, leaving 1 for system
        num_processes = max(1, cpu_count - 1)
        print(f"💻 Auto-detected {cpu_count} CPU cores")
        print(f"⚡ Using {num_processes} processes (leaving 1 core for system)")
    else:
        num_processes = args.processes

    # Load from config file if specified
    if args.config:
        config = load_config_file(args.config)

        items_to_extract = config.get("items_to_extract", [])
        filing_types = config.get("filing_types", ["10-K"])
        remove_tables = config.get("remove_tables", True)
        skip_existing = config.get("skip_existing", True)

    else:
        # Use command-line arguments
        items_to_extract = (
            [item.strip() for item in args.items.split(",")] if args.items else []
        )
        filing_types = [ft.strip() for ft in args.filing_types.split(",")]
        remove_tables = args.remove_tables
        skip_existing = args.skip_existing

    # Create extractor instance
    extractor = FlexibleExtractorFast(
        items_to_extract=items_to_extract,
        filing_types=filing_types,
        remove_tables=remove_tables,
        skip_existing=skip_existing,
        num_processes=num_processes,
    )

    # Run extraction
    success = extractor.run()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
