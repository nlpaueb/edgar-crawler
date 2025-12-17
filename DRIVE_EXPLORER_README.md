# Google Drive Folder Explorer

A comprehensive tool to examine and document your Google Drive folder structure. Perfect for understanding complex Drive folders with many files and subfolders.

## What It Does

The Drive Explorer helps you:

- 📊 **Scan** all files and folders in your Google Drive
- 📈 **Analyze** file types, sizes, and distributions
- 🌳 **Visualize** folder structure in a tree view
- 📝 **Generate** comprehensive guides and reports
- 💾 **Export** structure data as JSON for further analysis

## Quick Start (Google Colab)

### Option 1: Use the Jupyter Notebook (Recommended)

1. Open `drive_explorer_notebook.ipynb` in Google Colab
2. Mount your Google Drive
3. Run all cells
4. Get a comprehensive guide of your Drive structure!

### Option 2: Use Python Script

```python
# In Google Colab
from google.colab import drive
drive.mount('/content/drive')

# Import the explorer
from drive_explorer import DriveExplorer

# Create explorer and generate guide
explorer = DriveExplorer('/content/drive/MyDrive')
explorer.generate_guide('my_drive_guide.txt')

print(f"Total Files: {explorer.total_files:,}")
print(f"Total Folders: {explorer.total_folders:,}")
print(f"Total Size: {explorer._format_size(explorer.total_size)}")
```

## Features

### 1. Comprehensive Guide Generation

Generate a detailed text guide with:
- Summary statistics (total files, folders, size)
- File type distribution
- Tree view of folder structure
- Detailed folder-by-folder report

```python
explorer = DriveExplorer('/content/drive/MyDrive')
explorer.generate_guide(
    output_path='drive_guide.txt',
    include_tree=True,
    include_detailed=True,
    max_depth=None  # Or set to a number like 3
)
```

### 2. JSON Export

Export your Drive structure as JSON for programmatic access:

```python
explorer.export_json('drive_structure.json')
```

The JSON structure includes:
```json
{
  "name": "MyDrive",
  "path": "/content/drive/MyDrive",
  "files": 1234,
  "folders": 56,
  "size": 1073741824,
  "file_types": {
    ".txt": 450,
    ".csv": 200,
    ".pdf": 150
  },
  "subfolders": [...]
}
```

### 3. Quick Folder Summary

Get a quick overview of a specific folder without full exploration:

```python
summary = explorer.get_folder_summary('/content/drive/MyDrive/MyFolder')
print(summary)
```

Output:
```
{
  'path': '/content/drive/MyDrive/MyFolder',
  'name': 'MyFolder',
  'files': 42,
  'folders': 7,
  'size': 104857600,
  'file_types': {'.txt': 20, '.csv': 15, '.pdf': 7}
}
```

## Configuration Options

### Basic Configuration

```python
# Explore specific folder
explorer = DriveExplorer('/content/drive/MyDrive/EDGAR_Project')

# Generate guide with options
guide = explorer.generate_guide(
    output_path='guide.txt',      # Where to save the guide
    include_tree=True,             # Include tree view
    include_detailed=True,         # Include detailed reports
    max_depth=3                    # Limit depth (None = unlimited)
)
```

### For Large Folder Structures

If you have many files and folders:

1. **Limit Depth**: Set `max_depth` to explore only a few levels
   ```python
   explorer.generate_guide(max_depth=3)
   ```

2. **Skip Detailed Report**: Only generate tree view
   ```python
   explorer.generate_guide(include_detailed=False)
   ```

3. **Explore Subfolders Separately**: Create multiple explorers
   ```python
   subfolder_explorer = DriveExplorer('/content/drive/MyDrive/Subfolder')
   ```

## Command-Line Usage

You can also run the explorer from the command line:

```bash
# Basic usage
python drive_explorer.py --path /content/drive/MyDrive

# With options
python drive_explorer.py \
    --path /content/drive/MyDrive/EDGAR_Project \
    --output my_guide.txt \
    --max-depth 3 \
    --json structure.json

# Skip tree view
python drive_explorer.py --path /content/drive/MyDrive --no-tree

# Skip detailed report
python drive_explorer.py --path /content/drive/MyDrive --no-detailed
```

### Command-Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--path` | Root path to explore | `/content/drive/MyDrive` |
| `--output` | Output file path | `drive_structure_guide.txt` |
| `--max-depth` | Maximum depth to explore | Unlimited |
| `--json` | Also export as JSON | None |
| `--no-tree` | Skip tree view | Include tree |
| `--no-detailed` | Skip detailed report | Include detailed |

## Example Output

### Summary Statistics
```
================================================================================
SUMMARY STATISTICS
================================================================================
Total Files: 15,234
Total Folders: 342
Total Size: 25.67 GB

File Types Distribution:
  .txt: 5,432 files (35.7%)
  .csv: 3,210 files (21.1%)
  .pdf: 2,876 files (18.9%)
  .json: 1,543 files (10.1%)
  .html: 987 files (6.5%)
  no_extension: 654 files (4.3%)
  .log: 532 files (3.5%)
```

### Tree View
```
================================================================================
FOLDER STRUCTURE (TREE VIEW)
================================================================================
MyDrive/
├── EDGAR_Project/ (1,234 files, 15 folders, 12.3 GB)
│   ├── RAW_FILINGS/ (800 files, 10 folders, 8.5 GB)
│   │   ├── 10-K/ (500 files, 5 folders, 5.2 GB)
│   │   │   ├── 2020/ (100 files, 0 folders, 1.1 GB)
│   │   │   ├── 2021/ (150 files, 0 folders, 1.5 GB)
│   │   │   ├── 2022/ (125 files, 0 folders, 1.3 GB)
│   │   │   └── 2023/ (125 files, 0 folders, 1.3 GB)
│   │   └── 10-Q/ (300 files, 5 folders, 3.3 GB)
│   └── EXTRACTED_FILINGS/ (434 files, 5 folders, 3.8 GB)
└── My_10K_Files/ (500 files, 0 folders, 5.5 GB)
```

### Detailed Report
```
================================================================================
Folder: RAW_FILINGS
Path: /content/drive/MyDrive/EDGAR_Project/RAW_FILINGS
Files: 800
Subfolders: 10
Total Size: 8.50 GB
File Types:
  .txt: 600 files
  .html: 150 files
  .json: 50 files
================================================================================
```

## Use Cases

### 1. Understanding Your Drive Structure

After downloading many EDGAR filings, understand what you have:

```python
explorer = DriveExplorer('/content/drive/MyDrive/EDGAR_Project')
explorer.generate_guide('edgar_structure.txt')
```

### 2. Finding Specific File Types

Identify all CSV files for data processing:

```python
structure = explorer.explore()
# Parse structure to find .csv files
```

### 3. Space Management

Find what's taking up space:

```python
explorer = DriveExplorer('/content/drive/MyDrive')
guide = explorer.generate_guide('space_analysis.txt')
# Review the detailed report to see folder sizes
```

### 4. Documentation

Create documentation for your project:

```python
explorer = DriveExplorer('/content/drive/MyDrive/MyProject')
explorer.generate_guide('PROJECT_STRUCTURE.txt')
# Share this file with collaborators
```

### 5. Data Audit

Before reorganizing or cleaning up:

```python
# Create "before" snapshot
explorer = DriveExplorer('/content/drive/MyDrive')
explorer.generate_guide('before_cleanup.txt')
explorer.export_json('before_cleanup.json')

# ... do cleanup ...

# Create "after" snapshot
explorer.generate_guide('after_cleanup.txt')
explorer.export_json('after_cleanup.json')

# Compare the two to see what changed
```

## Integration with EDGAR Crawler

The Drive Explorer integrates seamlessly with the EDGAR Crawler project:

### After Downloading Filings

```python
# Download filings
from download_manager import DownloadManager
manager = DownloadManager('config.json')
manager.download_filings()

# Explore what was downloaded
from drive_explorer import DriveExplorer
explorer = DriveExplorer('/content/drive/MyDrive/EDGAR_Project')
explorer.generate_guide('downloads_summary.txt')
```

### After Extracting MD&A Sections

```python
# After running MDA extraction
from drive_explorer import DriveExplorer

# Check raw filings
raw_explorer = DriveExplorer('/content/drive/MyDrive/RAW_FILINGS')
raw_explorer.generate_guide('raw_filings_structure.txt')

# Check extracted filings
extracted_explorer = DriveExplorer('/content/drive/MyDrive/EXTRACTED_FILINGS')
extracted_explorer.generate_guide('extracted_filings_structure.txt')

print(f"Raw files: {raw_explorer.total_files}")
print(f"Extracted files: {extracted_explorer.total_files}")
print(f"Extraction progress: {extracted_explorer.total_files / raw_explorer.total_files * 100:.1f}%")
```

### Before Reorganizing

```python
# Before running reorganize_filings.py
from drive_explorer import DriveExplorer
explorer = DriveExplorer('/content/drive/MyDrive/EDGAR_Project')
explorer.generate_guide('before_reorganize.txt')

# Run reorganization
from reorganize_filings import reorganize_into_years
reorganize_into_years('/content/drive/MyDrive/EDGAR_Project/RAW_FILINGS/10-K')

# Check results
explorer.generate_guide('after_reorganize.txt')
```

## Performance Tips

1. **For Large Structures**: Use `max_depth` to limit exploration
   ```python
   explorer.generate_guide(max_depth=3)  # Only go 3 levels deep
   ```

2. **For Quick Checks**: Use `get_folder_summary()` instead of full exploration
   ```python
   summary = explorer.get_folder_summary()  # Fast, non-recursive
   ```

3. **Skip Detailed Reports**: For huge structures, skip detailed reports
   ```python
   explorer.generate_guide(include_detailed=False)
   ```

4. **Explore Subfolders**: Create separate explorers for specific subfolders
   ```python
   # Instead of exploring entire Drive
   explorer = DriveExplorer('/content/drive/MyDrive/SpecificFolder')
   ```

## Troubleshooting

### Issue: "Path does not exist"

Make sure you've mounted Google Drive:
```python
from google.colab import drive
drive.mount('/content/drive')
```

### Issue: Takes too long

Set `max_depth` to limit exploration:
```python
explorer.generate_guide(max_depth=2)
```

### Issue: Permission errors

Some system folders may not be accessible. The explorer will skip these and note them in the report.

### Issue: Out of memory in Colab

For very large structures:
1. Explore subfolders separately
2. Use `max_depth` to limit scope
3. Skip detailed reports: `include_detailed=False`

## Files Included

- **`drive_explorer.py`**: Main Python module with `DriveExplorer` class
- **`drive_explorer_notebook.ipynb`**: Interactive Jupyter notebook for Colab
- **`DRIVE_EXPLORER_README.md`**: This documentation file

## Requirements

- Python 3.6+
- Google Colab (for Drive mounting)
- No additional packages required (uses standard library)

## Advanced Usage

### Custom Analysis

Use the JSON export for custom analysis:

```python
import json

explorer = DriveExplorer('/content/drive/MyDrive')
explorer.export_json('structure.json')

# Load and analyze
with open('structure.json') as f:
    structure = json.load(f)

# Find folders with most files
def find_largest_folders(folder, min_files=100):
    results = []
    if folder['files'] >= min_files:
        results.append((folder['path'], folder['files']))
    for subfolder in folder.get('subfolders', []):
        results.extend(find_largest_folders(subfolder, min_files))
    return results

large_folders = find_largest_folders(structure)
print("Folders with 100+ files:")
for path, count in sorted(large_folders, key=lambda x: x[1], reverse=True):
    print(f"  {count:,} files: {path}")
```

### Monitoring Changes Over Time

Track how your Drive changes:

```python
import json
from datetime import datetime

# Create snapshot
explorer = DriveExplorer('/content/drive/MyDrive')
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

explorer.generate_guide(f'snapshot_{timestamp}.txt')
explorer.export_json(f'snapshot_{timestamp}.json')

# Compare with previous snapshot
# (implement comparison logic based on your needs)
```

## Support

For issues or questions:
1. Check this README
2. Review the code comments in `drive_explorer.py`
3. Try the notebook for interactive guidance
4. Check the EDGAR Crawler documentation for integration examples

## License

Part of the EDGAR Crawler project. Use freely for your research and analysis needs.
