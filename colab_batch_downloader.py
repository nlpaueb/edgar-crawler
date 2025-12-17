#!/usr/bin/env python3
"""
Colab Batch Downloader for EDGAR Filings

This script is optimized for Google Colab environments with the following features:
- Batch processing (process firms in chunks to handle 24-hour timeouts)
- Resume capability (automatically skip already-downloaded filings)
- Progress tracking (detailed logs and status updates)
- Direct Google Drive output (all files written to Drive for persistence)

Usage:
    python colab_batch_downloader.py --input wrds_data/wrds_identifiers.csv
    python colab_batch_downloader.py --input wrds_data/wrds_identifiers.csv --batch-size 500
    python colab_batch_downloader.py --input wrds_data/wrds_identifiers.csv --batch-id 2

Requirements:
    - WRDS identifiers CSV file with 'cik' column
    - Proper User-Agent configured
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from tqdm import tqdm

# Import the original download_filings module
import download_filings


class ColabBatchDownloader:
    """
    Batch downloader optimized for Google Colab

    Features:
    - Splits firms into manageable batches
    - Tracks progress across sessions
    - Resumes from last completed batch
    - Saves checkpoints to Google Drive
    """

    def __init__(
        self,
        input_file,
        batch_size=500,
        start_year=2010,
        end_year=None,
        filing_types=None,
        user_agent=None,
    ):
        """
        Initialize the batch downloader

        Args:
            input_file: Path to WRDS identifiers CSV file
            batch_size: Number of firms to process in each batch
            start_year: Starting year for filings
            end_year: Ending year for filings (defaults to current year)
            filing_types: List of filing types to download (default: ['10-K'])
            user_agent: User agent string for SEC requests
        """
        self.input_file = input_file
        self.batch_size = batch_size
        self.start_year = start_year
        self.end_year = end_year or datetime.now().year
        self.filing_types = filing_types or ["10-K"]
        self.user_agent = user_agent or "YourName YourEmail@example.com"

        # Paths
        self.progress_file = "logs/download_progress.csv"
        self.config_file = "config.json"

        # Load firm data
        self.firms_df = None
        self.batches = []
        self.current_batch_id = 0

    def load_firms(self):
        """Load firm identifiers from WRDS CSV file"""
        print(f"\n📂 Loading firms from {self.input_file}...")

        try:
            self.firms_df = pd.read_csv(self.input_file)

            # Validate required columns
            if "cik" not in self.firms_df.columns:
                print("❌ Error: 'cik' column not found in input file")
                print(f"   Available columns: {list(self.firms_df.columns)}")
                return False

            # Convert CIK to string and remove any NaN values
            self.firms_df["cik"] = self.firms_df["cik"].astype(str)
            self.firms_df = self.firms_df[self.firms_df["cik"].notna()]

            print(f"✅ Loaded {len(self.firms_df)} firms")

            # Show sample
            print(f"\n📋 Sample firms:")
            display_cols = [
                col
                for col in ["cik", "ticker", "company_name"]
                if col in self.firms_df.columns
            ]
            print(self.firms_df[display_cols].head())

            return True

        except FileNotFoundError:
            print(f"❌ Error: Input file not found: {self.input_file}")
            return False
        except Exception as e:
            print(f"❌ Error loading firms: {e}")
            return False

    def create_batches(self):
        """Divide firms into batches"""
        print(f"\n📦 Creating batches (size: {self.batch_size} firms per batch)...")

        total_firms = len(self.firms_df)
        num_batches = (total_firms + self.batch_size - 1) // self.batch_size

        for i in range(num_batches):
            start_idx = i * self.batch_size
            end_idx = min((i + 1) * self.batch_size, total_firms)

            batch = {
                "batch_id": i + 1,
                "start_idx": start_idx,
                "end_idx": end_idx,
                "firms": self.firms_df.iloc[start_idx:end_idx],
                "status": "pending",
            }

            self.batches.append(batch)

        print(f"✅ Created {len(self.batches)} batches")
        print(f"   Total firms: {total_firms}")
        print(f"   Firms per batch: {self.batch_size}")
        print(f"   Last batch size: {len(self.batches[-1]['firms'])}")

    def load_progress(self):
        """Load progress from previous sessions"""
        if not os.path.exists(self.progress_file):
            print("\n📊 No previous progress found (starting fresh)")
            return

        print(f"\n📊 Loading progress from {self.progress_file}...")

        try:
            progress_df = pd.read_csv(self.progress_file)

            # Update batch statuses
            for batch in self.batches:
                batch_progress = progress_df[
                    progress_df["batch_id"] == batch["batch_id"]
                ]

                if len(batch_progress) > 0:
                    status = batch_progress.iloc[0]["status"]
                    batch["status"] = status

            completed = sum(1 for b in self.batches if b["status"] == "completed")
            in_progress = sum(1 for b in self.batches if b["status"] == "in_progress")
            pending = sum(1 for b in self.batches if b["status"] == "pending")

            print(f"✅ Progress loaded:")
            print(f"   Completed: {completed} batches")
            print(f"   In Progress: {in_progress} batches")
            print(f"   Pending: {pending} batches")

        except Exception as e:
            print(f"⚠️  Warning: Could not load progress: {e}")

    def save_progress(self, batch_id, status):
        """Save progress after each batch"""

        # Create logs directory if it doesn't exist
        os.makedirs("logs", exist_ok=True)

        # Load existing progress or create new
        if os.path.exists(self.progress_file):
            progress_df = pd.read_csv(self.progress_file)
        else:
            progress_df = pd.DataFrame(
                columns=["batch_id", "status", "timestamp", "firms_count"]
            )

        # Update or add this batch's status
        batch_mask = progress_df["batch_id"] == batch_id
        batch_info = self.batches[batch_id - 1]

        if batch_mask.any():
            progress_df.loc[batch_mask, "status"] = status
            progress_df.loc[batch_mask, "timestamp"] = datetime.now().isoformat()
        else:
            new_row = pd.DataFrame(
                [
                    {
                        "batch_id": batch_id,
                        "status": status,
                        "timestamp": datetime.now().isoformat(),
                        "firms_count": len(batch_info["firms"]),
                    }
                ]
            )
            progress_df = pd.concat([progress_df, new_row], ignore_index=True)

        # Save to file
        progress_df.to_csv(self.progress_file, index=False)

    def update_config(self, cik_list):
        """Update config.json with current batch's CIK list"""
        print(f"\n⚙️  Updating config.json with {len(cik_list)} firms...")

        try:
            # Load existing config
            with open(self.config_file, "r") as f:
                config = json.load(f)

            # Update download_filings section
            config["download_filings"]["cik_tickers"] = cik_list
            config["download_filings"]["start_year"] = self.start_year
            config["download_filings"]["end_year"] = self.end_year
            config["download_filings"]["filing_types"] = self.filing_types
            config["download_filings"]["user_agent"] = self.user_agent

            # Save updated config
            with open(self.config_file, "w") as f:
                json.dump(config, f, indent=2)

            print(f"✅ Config updated")

        except Exception as e:
            print(f"❌ Error updating config: {e}")
            raise

    def process_batch(self, batch):
        """Process a single batch of firms"""
        batch_id = batch["batch_id"]
        firms = batch["firms"]

        print("\n" + "=" * 70)
        print(f"PROCESSING BATCH {batch_id} / {len(self.batches)}")
        print("=" * 70)
        print(f"Firms in this batch: {len(firms)}")
        print(f"Date range: {self.start_year} - {self.end_year}")
        print(f"Filing types: {', '.join(self.filing_types)}")
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Extract CIK list for this batch
        cik_list = firms["cik"].tolist()

        # Update config.json with this batch's CIKs
        self.update_config(cik_list)

        # Mark batch as in progress
        self.save_progress(batch_id, "in_progress")

        # Run the original download_filings script
        try:
            print(f"\n🚀 Starting download process...")
            download_filings.main()

            # Mark batch as completed
            self.save_progress(batch_id, "completed")

            print(f"\n✅ Batch {batch_id} completed successfully!")

        except KeyboardInterrupt:
            print(f"\n⚠️  Batch {batch_id} interrupted by user")
            self.save_progress(batch_id, "interrupted")
            raise

        except Exception as e:
            print(f"\n❌ Error in batch {batch_id}: {e}")
            self.save_progress(batch_id, "failed")
            raise

    def generate_summary(self):
        """Generate and display summary statistics"""
        if not os.path.exists(self.progress_file):
            return

        print("\n" + "=" * 70)
        print("DOWNLOAD SUMMARY")
        print("=" * 70)

        progress_df = pd.read_csv(self.progress_file)

        total_batches = len(self.batches)
        completed = len(progress_df[progress_df["status"] == "completed"])
        failed = len(progress_df[progress_df["status"] == "failed"])
        in_progress = len(progress_df[progress_df["status"] == "in_progress"])
        pending = total_batches - completed - failed - in_progress

        print(f"\n📊 Batch Status:")
        print(f"   Total batches: {total_batches}")
        print(f"   ✅ Completed: {completed} ({completed/total_batches*100:.1f}%)")
        print(f"   ❌ Failed: {failed}")
        print(f"   🔄 In Progress: {in_progress}")
        print(f"   ⏳ Pending: {pending}")

        total_firms = len(self.firms_df)
        completed_firms = progress_df[progress_df["status"] == "completed"][
            "firms_count"
        ].sum()

        print(f"\n📈 Firm Progress:")
        print(f"   Total firms: {total_firms}")
        print(
            f"   Processed: {completed_firms} ({completed_firms/total_firms*100:.1f}%)"
        )

        # Check if FILINGS_METADATA.csv exists for more detailed stats
        metadata_file = "datasets/FILINGS_METADATA.csv"
        if os.path.exists(metadata_file):
            try:
                metadata_df = pd.read_csv(metadata_file)
                print(f"\n📄 Downloaded Filings:")
                print(f"   Total filings: {len(metadata_df)}")

                for filing_type in self.filing_types:
                    count = len(metadata_df[metadata_df["Type"] == filing_type])
                    print(f"   {filing_type}: {count}")

            except Exception as e:
                print(f"\n⚠️  Could not read metadata: {e}")

        print("\n" + "=" * 70)

    def run(self, specific_batch_id=None):
        """
        Main execution flow

        Args:
            specific_batch_id: If provided, only process this specific batch
        """
        print("=" * 70)
        print("COLAB BATCH DOWNLOADER FOR EDGAR FILINGS")
        print("=" * 70)
        print(f"\nConfiguration:")
        print(f"  Input file: {self.input_file}")
        print(f"  Batch size: {self.batch_size} firms")
        print(f"  Year range: {self.start_year} - {self.end_year}")
        print(f"  Filing types: {', '.join(self.filing_types)}")

        # Load firms
        if not self.load_firms():
            return False

        # Create batches
        self.create_batches()

        # Load previous progress
        self.load_progress()

        # Determine which batches to process
        if specific_batch_id is not None:
            batches_to_process = [
                b for b in self.batches if b["batch_id"] == specific_batch_id
            ]
            if not batches_to_process:
                print(f"\n❌ Batch {specific_batch_id} not found")
                return False
        else:
            # Process only pending or failed batches
            batches_to_process = [
                b for b in self.batches if b["status"] in ["pending", "failed"]
            ]

        if not batches_to_process:
            print("\n✅ All batches already completed!")
            self.generate_summary()
            return True

        print(f"\n🎯 Will process {len(batches_to_process)} batch(es)")

        # Process batches
        start_time = time.time()

        try:
            for i, batch in enumerate(batches_to_process):
                print(f"\n{'='*70}")
                print(f"Batch {i+1} of {len(batches_to_process)} to process")
                print(f"{'='*70}")

                self.process_batch(batch)

                # Show progress after each batch
                elapsed = time.time() - start_time
                print(f"\n⏱️  Elapsed time: {elapsed/3600:.2f} hours")

                # Estimate remaining time
                batches_done = i + 1
                batches_remaining = len(batches_to_process) - batches_done
                if batches_done > 0:
                    avg_time_per_batch = elapsed / batches_done
                    est_remaining = avg_time_per_batch * batches_remaining
                    print(
                        f"⏳ Estimated time remaining: {est_remaining/3600:.2f} hours"
                    )

        except KeyboardInterrupt:
            print("\n\n⚠️  Process interrupted by user")
            print("Progress has been saved. You can resume by running this script again.")

        except Exception as e:
            print(f"\n\n❌ Process failed: {e}")
            print("Progress has been saved. You can resume by running this script again.")

        # Generate final summary
        self.generate_summary()

        print("\n" + "=" * 70)
        print("✅ BATCH PROCESSING COMPLETE")
        print("=" * 70)

        print(f"\nNext steps:")
        print(f"1. Check logs/download_progress.csv for detailed status")
        print(f"2. Review datasets/FILINGS_METADATA.csv for downloaded filings")
        print(f"3. Run flexible_extractor.py to extract MD&A sections")

        return True


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Batch downloader for EDGAR filings (Colab-optimized)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Process all firms in batches of 500
    python colab_batch_downloader.py --input wrds_data/wrds_identifiers.csv

    # Use smaller batches (faster checkpoints)
    python colab_batch_downloader.py --input wrds_data/wrds_identifiers.csv --batch-size 200

    # Process a specific batch (useful for retry)
    python colab_batch_downloader.py --input wrds_data/wrds_identifiers.csv --batch-id 3

    # Specify custom date range
    python colab_batch_downloader.py --input wrds_data/wrds_identifiers.csv --start-year 2015 --end-year 2023

    # Download multiple filing types
    python colab_batch_downloader.py --input wrds_data/wrds_identifiers.csv --filing-types 10-K,10-Q
        """,
    )

    parser.add_argument(
        "--input", type=str, required=True, help="Path to WRDS identifiers CSV file"
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of firms per batch (default: 500)",
    )

    parser.add_argument(
        "--batch-id",
        type=int,
        default=None,
        help="Process only this specific batch (default: process all pending)",
    )

    parser.add_argument(
        "--start-year",
        type=int,
        default=2010,
        help="Starting year for filings (default: 2010)",
    )

    parser.add_argument(
        "--end-year", type=int, default=None, help="Ending year (default: current year)"
    )

    parser.add_argument(
        "--filing-types",
        type=str,
        default="10-K",
        help="Comma-separated filing types (default: 10-K)",
    )

    parser.add_argument(
        "--user-agent",
        type=str,
        default=None,
        help="User agent for SEC requests (default: from config.json)",
    )

    args = parser.parse_args()

    # Parse filing types
    filing_types = [ft.strip() for ft in args.filing_types.split(",")]

    # Load user agent from config if not provided
    user_agent = args.user_agent
    if user_agent is None:
        try:
            with open("config.json", "r") as f:
                config = json.load(f)
                user_agent = config["download_filings"]["user_agent"]
        except Exception as e:
            print(f"⚠️  Warning: Could not load user agent from config.json: {e}")
            print("Please provide --user-agent or update config.json")
            sys.exit(1)

    # Validate user agent is not a placeholder
    placeholder_values = [
        "YourName YourEmail@example.com",
        "Your name (your email)",
        "REPLACE_WITH_YOUR_NAME_AND_EMAIL",
    ]
    if user_agent in placeholder_values or not user_agent or "@" not in user_agent:
        print("\n" + "=" * 70)
        print("❌ ERROR: Invalid User-Agent")
        print("=" * 70)
        print("\nThe SEC requires a valid User-Agent with your name and email.")
        print("This is per SEC's fair access policy:")
        print("https://www.sec.gov/os/accessing-edgar-data")
        print("\nYour current User-Agent:", user_agent)
        print("\nPlease provide a valid User-Agent using one of these methods:")
        print("\n1. Command-line argument:")
        print('   --user-agent "Your Name your.email@university.edu"')
        print("\n2. Update config.json:")
        print('   "user_agent": "Your Name your.email@university.edu"')
        print("\nExample:")
        print('   --user-agent "Jane Smith jsmith@stanford.edu"')
        print("\n" + "=" * 70)
        sys.exit(1)

    # Create downloader instance
    downloader = ColabBatchDownloader(
        input_file=args.input,
        batch_size=args.batch_size,
        start_year=args.start_year,
        end_year=args.end_year,
        filing_types=filing_types,
        user_agent=user_agent,
    )

    # Run the batch processing
    success = downloader.run(specific_batch_id=args.batch_id)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
