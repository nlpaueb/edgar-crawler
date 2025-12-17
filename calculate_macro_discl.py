#!/usr/bin/env python3
"""
MacroDiscl Measure Calculator for Google Colab
================================================
Replicates the "MacroDiscl" measure from Holstead et al. (2024).

Formula: MacroDiscl = (MacroWords / TotalWords) × 1,000

This script:
1. Mounts Google Drive
2. Processes 10-K text files from a specified folder
3. Cleans HTML tags and attempts to remove financial tables
4. Counts macro-related terms using the paper's dictionary
5. Calculates the MacroDiscl score for each filing
6. Saves results to a CSV file in Google Drive

Usage in Google Colab:
    - Upload this script or copy-paste into a Colab cell
    - Adjust INPUT_FOLDER and OUTPUT_CSV paths below
    - Run the script
"""

import os
import re
import csv
from pathlib import Path
from typing import Dict, List, Tuple

# BeautifulSoup for HTML parsing
try:
    from bs4 import BeautifulSoup
except ImportError:
    print("Installing beautifulsoup4...")
    import subprocess
    subprocess.check_call(['pip', 'install', 'beautifulsoup4'])
    from bs4 import BeautifulSoup

# Mount Google Drive (for Colab)
try:
    from google.colab import drive
    drive.mount('/content/drive')
    print("✓ Google Drive mounted successfully")
except ImportError:
    print("⚠ Not running in Google Colab - skipping Drive mount")
except Exception as e:
    print(f"⚠ Drive mount failed: {e}")

# =============================================================================
# CONFIGURATION: Adjust these paths for your Google Drive setup
# =============================================================================

# Input folder containing 10-K text files
INPUT_FOLDER = "/content/drive/MyDrive/My_10K_Files/"

# Output CSV file path
OUTPUT_CSV = "/content/drive/MyDrive/MacroDiscl_Results.csv"

# =============================================================================
# MACRO DICTIONARY (Holstead et al., 2024)
# =============================================================================
# Based on the paper's methodology, all matching is case-insensitive

# Exact match unigrams (require word boundaries)
EXACT_UNIGRAMS = [
    'macro', 'macroeconomic', 'macroeconomics', 'macroeconomy',
    'import', 'importing', 'imported',
    'export', 'exporting', 'exported',
    'gdp', 'gnp', 'fed'
]

# Substring match unigrams (match even within other words, e.g., "hyperinflation")
SUBSTRING_UNIGRAMS = [
    'inflation', 'deflation', 'recession', 'currency'
]

# Bigrams (two-word consecutive phrases)
BIGRAMS = [
    'economic condition', 'economic environment', 'economic downturn',
    'economic factor', 'economic trend', 'economic instability',
    'economic growth', 'economic activity', 'economic development',
    'economic slowdown', 'economic uncertainty', 'economic recovery',
    'economic climate', 'economic data', 'economic cycle',
    'economic crisis', 'economic indicator', 'economic output',
    'economic expansion',
    'capital market', 'credit market', 'global market',
    'international market', 'exchange market', 'emerging market',
    'bear market', 'bull market', 'market risk', 'credit risk',
    'global risk', 'international risk', 'exchange risk', 'economic risk',
    'global economy', 'international economy', 'emerging economy',
    'general economy', 'foreign exchange', 'foreign investor',
    'foreign investment', 'real estate', 'real property', 'real growth',
    'real rate', 'federal reserve', 'central bank', 'gross domestic',
    'gross national', 'monetary policy', 'fiscal policy', 'interest rate',
    'discount rate', 'business cycle', 'global trade'
]

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def clean_html_and_tables(text: str) -> str:
    """
    Clean HTML tags and attempt to remove financial tables from 10-K text.

    Per the paper: uses "entire 10-K report, excluding financial statement tables"

    Args:
        text: Raw text (may contain HTML)

    Returns:
        Cleaned text with HTML removed and tables excluded
    """
    # Parse HTML
    soup = BeautifulSoup(text, 'html.parser')

    # Remove script and style elements
    for element in soup(['script', 'style', 'head', 'title', 'meta']):
        element.decompose()

    # Remove table elements (attempt to exclude financial tables)
    for table in soup.find_all('table'):
        table.decompose()

    # Extract visible text
    text = soup.get_text(separator=' ')

    # Clean up whitespace
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()

    return text


def clean_text_for_matching(text: str) -> str:
    """
    Prepare text for matching by:
    1. Converting to lowercase (paper requirement)
    2. Replacing punctuation with spaces for clean tokenization
    3. Normalizing whitespace

    Args:
        text: Cleaned text (HTML already removed)

    Returns:
        Text ready for pattern matching
    """
    # Convert to lowercase (paper explicitly requires this)
    text = text.lower()

    # Replace punctuation with spaces to enable clean bigram matching
    # This handles cases like "economic, condition" -> "economic condition"
    text = re.sub(r'[^\w\s]', ' ', text)

    # Normalize whitespace (multiple spaces -> single space)
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()

    return text


def count_words(text: str) -> int:
    """
    Count total words in the text.

    Args:
        text: Cleaned and normalized text

    Returns:
        Total word count
    """
    words = text.split()
    return len(words)


def count_macro_terms(text: str) -> Tuple[int, Dict[str, int]]:
    """
    Count occurrences of all macro-related terms from the dictionary.

    Per the paper: "count the number of occurrences" (every match counts).

    Args:
        text: Cleaned and normalized text (lowercase, punctuation removed)

    Returns:
        Tuple of (total_macro_count, breakdown_dict)
    """
    breakdown = {
        'exact_unigrams': 0,
        'substring_unigrams': 0,
        'bigrams': 0
    }

    # Count exact match unigrams (with word boundaries)
    for term in EXACT_UNIGRAMS:
        pattern = r'\b' + re.escape(term) + r'\b'
        matches = re.findall(pattern, text)
        breakdown['exact_unigrams'] += len(matches)

    # Count substring match unigrams (no word boundaries)
    for term in SUBSTRING_UNIGRAMS:
        # Simple substring count (case already handled - text is lowercase)
        matches = re.findall(term, text)
        breakdown['substring_unigrams'] += len(matches)

    # Count bigrams (consecutive two-word phrases)
    for bigram in BIGRAMS:
        # Bigrams are already lowercase, text is lowercase
        # Since we've cleaned punctuation, we can do simple string matching
        matches = re.findall(re.escape(bigram), text)
        breakdown['bigrams'] += len(matches)

    total_macro_count = sum(breakdown.values())
    return total_macro_count, breakdown


def calculate_macro_discl(file_path: str) -> Dict:
    """
    Calculate MacroDiscl measure for a single 10-K file.

    Formula: MacroDiscl = (MacroWords / TotalWords) × 1,000

    Args:
        file_path: Path to the 10-K text file

    Returns:
        Dictionary with results: {
            'filename': str,
            'total_words': int,
            'macro_count': int,
            'macro_discl': float,
            'status': str,
            'breakdown': dict (optional)
        }
    """
    filename = os.path.basename(file_path)

    try:
        # Read file
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            raw_text = f.read()

        # Step 1: Clean HTML and remove tables
        cleaned_text = clean_html_and_tables(raw_text)

        # Step 2: Prepare for matching (lowercase, remove punctuation)
        processed_text = clean_text_for_matching(cleaned_text)

        # Step 3: Count total words
        total_words = count_words(processed_text)

        if total_words == 0:
            return {
                'filename': filename,
                'total_words': 0,
                'macro_count': 0,
                'macro_discl': 0.0,
                'status': 'empty_file'
            }

        # Step 4: Count macro terms
        macro_count, breakdown = count_macro_terms(processed_text)

        # Step 5: Calculate MacroDiscl
        macro_discl = (macro_count / total_words) * 1000

        return {
            'filename': filename,
            'total_words': total_words,
            'macro_count': macro_count,
            'macro_discl': round(macro_discl, 4),
            'status': 'success',
            'breakdown': breakdown
        }

    except Exception as e:
        print(f"✗ Error processing {filename}: {e}")
        return {
            'filename': filename,
            'total_words': 0,
            'macro_count': 0,
            'macro_discl': 0.0,
            'status': f'error: {str(e)}'
        }


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """
    Main execution function:
    1. Validate paths
    2. Process all 10-K files
    3. Save results to CSV
    """
    print("=" * 70)
    print("MacroDiscl Calculator (Holstead et al., 2024)")
    print("=" * 70)

    # Validate input folder
    if not os.path.exists(INPUT_FOLDER):
        print(f"\n✗ ERROR: Input folder not found: {INPUT_FOLDER}")
        print("Please update the INPUT_FOLDER variable in the script.")
        return

    print(f"\n📁 Input folder: {INPUT_FOLDER}")
    print(f"📄 Output CSV: {OUTPUT_CSV}")

    # Find all 10-K files
    file_extensions = ['*.txt', '*.html', '*.htm']
    files = []
    for ext in file_extensions:
        files.extend(Path(INPUT_FOLDER).glob(ext))

    files = [str(f) for f in files]

    if not files:
        print(f"\n✗ No files found with extensions: {file_extensions}")
        return

    print(f"\n✓ Found {len(files)} file(s) to process")
    print("\n" + "=" * 70)
    print("Processing files...")
    print("=" * 70)

    # Process each file
    results = []
    for idx, file_path in enumerate(files, 1):
        filename = os.path.basename(file_path)
        print(f"\n[{idx}/{len(files)}] Processing: {filename}")

        result = calculate_macro_discl(file_path)
        results.append(result)

        if result['status'] == 'success':
            print(f"  ✓ Total words: {result['total_words']:,}")
            print(f"  ✓ Macro terms: {result['macro_count']:,}")
            print(f"  ✓ MacroDiscl: {result['macro_discl']:.4f}")
            if 'breakdown' in result:
                bd = result['breakdown']
                print(f"    - Exact unigrams: {bd['exact_unigrams']}")
                print(f"    - Substring unigrams: {bd['substring_unigrams']}")
                print(f"    - Bigrams: {bd['bigrams']}")
        else:
            print(f"  ✗ Status: {result['status']}")

    # Save results to CSV
    print("\n" + "=" * 70)
    print("Saving results...")
    print("=" * 70)

    try:
        # Ensure output directory exists
        os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Filename', 'TotalWords', 'MacroCount', 'MacroDiscl']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for result in results:
                writer.writerow({
                    'Filename': result['filename'],
                    'TotalWords': result['total_words'],
                    'MacroCount': result['macro_count'],
                    'MacroDiscl': result['macro_discl']
                })

        print(f"\n✓ Results saved to: {OUTPUT_CSV}")
        print(f"✓ Processed {len(results)} file(s)")

        # Summary statistics
        successful = [r for r in results if r['status'] == 'success']
        if successful:
            avg_discl = sum(r['macro_discl'] for r in successful) / len(successful)
            print(f"\n📊 Summary Statistics:")
            print(f"  - Files processed successfully: {len(successful)}")
            print(f"  - Average MacroDiscl: {avg_discl:.4f}")
            print(f"  - Min MacroDiscl: {min(r['macro_discl'] for r in successful):.4f}")
            print(f"  - Max MacroDiscl: {max(r['macro_discl'] for r in successful):.4f}")

    except Exception as e:
        print(f"\n✗ Error saving CSV: {e}")
        return

    print("\n" + "=" * 70)
    print("✓ Processing complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
