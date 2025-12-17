#!/usr/bin/env python3
"""
Progressive Reorganization Patch for extract_items.py

This script patches extract_items.py to support year-based subdirectories
for extracted JSON files, avoiding Google Drive's ~10,000 file limit.

What it does:
1. For each file to extract:
   - Check if it exists in OLD location (root: EXTRACTED_FILINGS/10-K/)
   - If yes: MOVE to year folder (EXTRACTED_FILINGS/10-K/2020/), skip extraction
   - If no: Check if already in year folder
   - If no: Extract directly to year folder

This works even when os.listdir() fails with [Errno 5]!
"""

import os
import re

def apply_patch():
    file_path = 'extract_items.py'

    print("🔧 Applying progressive reorganization patch to extract_items.py\n")

    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return False

    with open(file_path, 'r') as f:
        content = f.read()

    # Check if already patched
    if 'Progressive reorganization' in content:
        print("✅ extract_items.py already has progressive reorganization patch")
        return True

    # Find the actual code structure in process_filing method
    old_code = '''        # Create the absolute path for the JSON file
        absolute_json_filename = os.path.join(
            self.extracted_files_folder, filing_metadata["Type"], json_filename
        )

        # Skip processing if the extracted JSON file already exists and skip flag is enabled
        if self.skip_extracted_filings and os.path.exists(absolute_json_filename):
            return 0'''

    new_code = '''        # Create the absolute path for the JSON file
        # Progressive reorganization: organize by year to avoid Drive limits
        filing_type_folder = os.path.join(self.extracted_files_folder, filing_metadata["Type"])

        # Extract year from filename and create year subfolder
        year_match = re.search(r'_10K_(\\d{4})_', json_filename)

        if year_match:
            year_subfolder = year_match.group(1)
            year_folder = os.path.join(filing_type_folder, year_subfolder)
            os.makedirs(year_folder, exist_ok=True)
            absolute_json_filename = os.path.join(year_folder, json_filename)
        else:
            # Fallback: use root if year not found
            absolute_json_filename = os.path.join(filing_type_folder, json_filename)

        # Check if file exists in OLD location (root) - move it!
        old_location = os.path.join(filing_type_folder, json_filename)
        if year_match and os.path.exists(old_location) and old_location != absolute_json_filename:
            try:
                # Move from old location to year subfolder
                os.rename(old_location, absolute_json_filename)
                print(f"📦 Moved: {json_filename} → {year_subfolder}/")
                if self.skip_extracted_filings:
                    return 0  # Already extracted, just moved
            except Exception as e:
                # If move fails, continue with extraction
                pass

        # Skip processing if the extracted JSON file already exists and skip flag is enabled
        if self.skip_extracted_filings and os.path.exists(absolute_json_filename):
            return 0'''

    if old_code in content:
        content = content.replace(old_code, new_code)

        # Write patched content
        with open(file_path, 'w') as f:
            f.write(content)

        print("✅ Progressive reorganization patch applied successfully!\n")
        print("=" * 60)
        print("What this patch does:")
        print("=" * 60)
        print("For each of 83,628 files in metadata:")
        print("  1. Check if exists in root (datasets/EXTRACTED_FILINGS/10-K/)")
        print("  2. If YES: Move to year folder, skip extraction")
        print("  3. If NO: Check if already in year folder")
        print("  4. If NO: Extract to year folder")
        print("\nResult:")
        print("  • ~75,000 existing files: MOVED to year folders")
        print("  • ~8,000 remaining files: EXTRACTED to year folders")
        print("  • Total time: Same as normal extraction (~10-15 hours)")
        print("\nWorks even when os.listdir() fails with [Errno 5]!")
        print("=" * 60)
        return True
    else:
        print("❌ Could not find expected code structure in extract_items.py")
        print("\nLooking for:")
        print(old_code[:100] + "...")
        print("\nPlease check if extract_items.py has been modified.")
        return False

if __name__ == '__main__':
    success = apply_patch()
    if success:
        print("\n🎉 Ready to run extraction!")
        print("   Use: python flexible_extractor.py --config extraction_configs/mda_only.json")
    else:
        print("\n⚠️ Manual patching may be required")
