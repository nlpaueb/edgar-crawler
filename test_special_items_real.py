"""
Quick test script to validate special items extraction on a real 10-K filing.
"""
import json
import os
import zipfile
import pandas as pd
import numpy as np
from extract_items import ExtractItems

# Extract test filings
def extract_zip(input_zip, output_dir):
    """Extract zip file to output directory."""
    zf = zipfile.ZipFile(input_zip)
    zf.extractall(path=output_dir)
    print(f"Extracted {input_zip} to {output_dir}")

# Setup
test_dir = "/tmp/edgar-test-special-items"
os.makedirs(test_dir, exist_ok=True)

# Extract test RAW_FILINGS
extract_zip("tests/fixtures/RAW_FILINGS/10-K.zip", os.path.join(test_dir, "RAW_FILINGS"))

# Load metadata
filings_metadata_df = pd.read_csv("tests/fixtures/FILINGS_METADATA_TEST.csv", dtype=str)
filings_metadata_df = filings_metadata_df[filings_metadata_df["Type"] == "10-K"]
filings_metadata_df = filings_metadata_df.replace({np.nan: None})

print(f"Found {len(filings_metadata_df)} 10-K filings to test\n")

# Configure special items extraction
special_items_config = {
    'enabled': True,
    'scan_item_7_mda': False,
    'confidence_threshold': 0.3,
    'debug_logging': True,  # Enable debug logging
    'keywords': {
        'restructuring': ['restructuring', 'reorganization', 'workforce reduction', 'severance', 'facility closure'],
        'impairment': ['impairment', 'write-down', 'write-off', 'write down', 'write off', 'goodwill impairment', 'asset impairment'],
        'litigation': ['litigation', 'settlement', 'legal proceeding', 'legal settlement', 'jury award', 'arbitration'],
        'discontinued_ops': ['discontinued operation', 'disposal of business', 'disposal group', 'held for sale'],
        'unusual': ['unusual item', 'nonrecurring', 'non-recurring', 'one-time', 'special charge', 'special item', 'items impacting comparability', 'non-gaap adjustment', 'non-operating'],
        'other': ['other income', 'other expense', 'gain on sale', 'loss on sale', 'debt extinguishment', 'pension settlement']
    }
}

# Create extractor with special items enabled
extraction = ExtractItems(
    remove_tables=True,
    items_to_extract=['7', '8'],  # Only extract Item 7 and 8 for speed
    include_signature=False,
    raw_files_folder=os.path.join(test_dir, "RAW_FILINGS"),
    extracted_files_folder=os.path.join(test_dir, "EXTRACTED_FILINGS"),
    skip_extracted_filings=False,
    special_items_config=special_items_config,
)

# Test on a recent filing - Tyson Foods 2022 is likely to have special items
# Filter for Tyson Foods 2022
tyson_filings = filings_metadata_df[
    (filings_metadata_df['Company'].str.contains('TYSON', case=False, na=False)) &
    (filings_metadata_df['Date'].str.contains('2022', na=False))
]

if len(tyson_filings) > 0:
    filing_metadata = list(zip(*tyson_filings.iterrows()))[1][0]
else:
    # Fallback to most recent filing
    filings_metadata_df = filings_metadata_df.sort_values('Date', ascending=False)
    filing_metadata = list(zip(*filings_metadata_df.iterrows()))[1][0]
print(f"Testing on filing: {filing_metadata['Company']} ({filing_metadata['filename']})")
print(f"Filing date: {filing_metadata['Date']}\n")

extraction.determine_items_to_extract(filing_metadata)
extracted_filing = extraction.extract_items(filing_metadata)

if extracted_filing:
    # Display special items found
    special_items = extracted_filing.get('special_items', [])

    print(f"\n{'='*80}")
    print(f"SPECIAL ITEMS EXTRACTION RESULTS")
    print(f"{'='*80}\n")

    if special_items:
        print(f"Found {len(special_items)} special items:\n")

        for i, item in enumerate(special_items, 1):
            print(f"{i}. {item['type'].upper()}")
            print(f"   Keywords matched: {', '.join(item['keywords_matched'])}")
            print(f"   Confidence: {item['confidence']:.2f}")
            print(f"   Source: {item['source_section']}")
            if item['amount_raw']:
                print(f"   Amount: {item['amount_raw']} (value: {item['amount_value']} {item['amount_scale']})")
            if item['footnote_reference']:
                print(f"   Footnote: {item['footnote_reference']}")
            print(f"   Context: {item['context'][:150]}...")
            print()
    else:
        print("No special items found in this filing.")
        print("This could mean:")
        print("- The company had no special items to report in this period")
        print("- Special items were not disclosed in a detectable format")
        print("- Keywords need adjustment for this company's terminology")

    print(f"\n{'='*80}")

    # Save full output for inspection
    output_file = os.path.join(test_dir, "test_output.json")
    with open(output_file, 'w') as f:
        json.dump(extracted_filing, f, indent=2)
    print(f"\nFull extracted filing saved to: {output_file}")

else:
    print("ERROR: Failed to extract filing")

print("\nTest complete!")
