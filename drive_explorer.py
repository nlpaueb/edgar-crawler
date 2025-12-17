#!/usr/bin/env python3
"""
Google Drive Folder Explorer

This script examines the documents and subfolders within a Google Drive folder
and creates a comprehensive guide showing the structure, file counts, and organization.

Usage:
    In Google Colab:
        from google.colab import drive
        drive.mount('/content/drive')

        from drive_explorer import DriveExplorer
        explorer = DriveExplorer('/content/drive/MyDrive')
        explorer.generate_guide('drive_structure_guide.txt')

    Or run directly:
        python drive_explorer.py --path /content/drive/MyDrive --output guide.txt
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
import json


class DriveExplorer:
    """Explores Google Drive folder structure and generates reports."""

    def __init__(self, root_path: str):
        """
        Initialize the Drive Explorer.

        Args:
            root_path: Root path to explore (e.g., '/content/drive/MyDrive')
        """
        self.root_path = Path(root_path)
        if not self.root_path.exists():
            raise ValueError(f"Path does not exist: {root_path}")

        self.folder_stats = {}
        self.file_extensions = {}
        self.total_files = 0
        self.total_folders = 0
        self.total_size = 0

    def _get_size(self, path: Path) -> int:
        """Get size of a file or directory in bytes."""
        try:
            if path.is_file():
                return path.stat().st_size
            elif path.is_dir():
                total = 0
                for item in path.rglob('*'):
                    if item.is_file():
                        try:
                            total += item.stat().st_size
                        except (PermissionError, OSError):
                            pass
                return total
        except (PermissionError, OSError):
            return 0
        return 0

    def _format_size(self, size_bytes: int) -> str:
        """Convert bytes to human-readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"

    def _explore_directory(self, path: Path, depth: int = 0, max_depth: int = None) -> Dict:
        """
        Recursively explore a directory and gather statistics.

        Args:
            path: Path to explore
            depth: Current depth level
            max_depth: Maximum depth to explore (None for unlimited)

        Returns:
            Dictionary with folder statistics
        """
        if max_depth is not None and depth > max_depth:
            return None

        try:
            items = list(path.iterdir())
        except (PermissionError, OSError) as e:
            return {
                'name': path.name,
                'path': str(path),
                'error': str(e),
                'files': 0,
                'folders': 0,
                'size': 0
            }

        folder_info = {
            'name': path.name,
            'path': str(path),
            'files': 0,
            'folders': 0,
            'size': 0,
            'subfolders': [],
            'file_types': {},
            'depth': depth
        }

        files = []
        folders = []

        for item in items:
            if item.is_file():
                files.append(item)
            elif item.is_dir():
                folders.append(item)

        # Process files
        for file_path in files:
            try:
                file_size = file_path.stat().st_size
                folder_info['files'] += 1
                folder_info['size'] += file_size
                self.total_files += 1
                self.total_size += file_size

                # Track file extensions
                ext = file_path.suffix.lower() or 'no_extension'
                folder_info['file_types'][ext] = folder_info['file_types'].get(ext, 0) + 1
                self.file_extensions[ext] = self.file_extensions.get(ext, 0) + 1

            except (PermissionError, OSError):
                pass

        # Process folders recursively
        for folder_path in folders:
            self.total_folders += 1
            folder_info['folders'] += 1

            subfolder_info = self._explore_directory(folder_path, depth + 1, max_depth)
            if subfolder_info:
                folder_info['subfolders'].append(subfolder_info)
                # Add subfolder sizes to parent
                folder_info['size'] += subfolder_info.get('size', 0)

        return folder_info

    def explore(self, max_depth: int = None) -> Dict:
        """
        Explore the Drive structure.

        Args:
            max_depth: Maximum depth to explore (None for unlimited)

        Returns:
            Dictionary with complete folder structure
        """
        print(f"Exploring: {self.root_path}")
        print("This may take a while for large folder structures...")

        # Reset counters
        self.folder_stats = {}
        self.file_extensions = {}
        self.total_files = 0
        self.total_folders = 0
        self.total_size = 0

        structure = self._explore_directory(self.root_path, depth=0, max_depth=max_depth)

        return structure

    def _generate_tree_view(self, folder_info: Dict, prefix: str = "", is_last: bool = True) -> List[str]:
        """Generate a tree-style view of the folder structure."""
        lines = []

        # Current folder line
        connector = "└── " if is_last else "├── "
        folder_name = folder_info['name']

        if folder_info.get('error'):
            folder_line = f"{prefix}{connector}{folder_name}/ [ERROR: {folder_info['error']}]"
        else:
            file_count = folder_info['files']
            folder_count = folder_info['folders']
            size_str = self._format_size(folder_info['size'])
            folder_line = f"{prefix}{connector}{folder_name}/ ({file_count} files, {folder_count} folders, {size_str})"

        lines.append(folder_line)

        # Update prefix for children
        if is_last:
            new_prefix = prefix + "    "
        else:
            new_prefix = prefix + "│   "

        # Add subfolders
        subfolders = folder_info.get('subfolders', [])
        for i, subfolder in enumerate(subfolders):
            is_last_subfolder = (i == len(subfolders) - 1)
            lines.extend(self._generate_tree_view(subfolder, new_prefix, is_last_subfolder))

        return lines

    def _generate_detailed_report(self, folder_info: Dict, indent: int = 0) -> List[str]:
        """Generate a detailed report of folder contents."""
        lines = []
        indent_str = "  " * indent

        folder_name = folder_info['name']
        folder_path = folder_info['path']

        lines.append(f"{indent_str}{'='*80}")
        lines.append(f"{indent_str}Folder: {folder_name}")
        lines.append(f"{indent_str}Path: {folder_path}")

        if folder_info.get('error'):
            lines.append(f"{indent_str}ERROR: {folder_info['error']}")
        else:
            lines.append(f"{indent_str}Files: {folder_info['files']}")
            lines.append(f"{indent_str}Subfolders: {folder_info['folders']}")
            lines.append(f"{indent_str}Total Size: {self._format_size(folder_info['size'])}")

            # File types in this folder
            if folder_info['file_types']:
                lines.append(f"{indent_str}File Types:")
                for ext, count in sorted(folder_info['file_types'].items(), key=lambda x: x[1], reverse=True):
                    lines.append(f"{indent_str}  {ext}: {count} files")

        lines.append(f"{indent_str}{'='*80}")
        lines.append("")

        # Process subfolders
        for subfolder in folder_info.get('subfolders', []):
            lines.extend(self._generate_detailed_report(subfolder, indent + 1))

        return lines

    def generate_guide(self, output_path: str = None, include_tree: bool = True,
                      include_detailed: bool = True, max_depth: int = None) -> str:
        """
        Generate a comprehensive guide of the Drive structure.

        Args:
            output_path: Path to save the guide (None for console only)
            include_tree: Include tree-style view
            include_detailed: Include detailed folder-by-folder report
            max_depth: Maximum depth to explore

        Returns:
            The generated guide as a string
        """
        # Explore the structure
        structure = self.explore(max_depth=max_depth)

        # Generate report
        lines = []
        lines.append("="*80)
        lines.append("GOOGLE DRIVE STRUCTURE GUIDE")
        lines.append("="*80)
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Root Path: {self.root_path}")
        lines.append("")

        # Summary statistics
        lines.append("="*80)
        lines.append("SUMMARY STATISTICS")
        lines.append("="*80)
        lines.append(f"Total Files: {self.total_files:,}")
        lines.append(f"Total Folders: {self.total_folders:,}")
        lines.append(f"Total Size: {self._format_size(self.total_size)}")
        lines.append("")

        # File type distribution
        lines.append("File Types Distribution:")
        for ext, count in sorted(self.file_extensions.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / self.total_files * 100) if self.total_files > 0 else 0
            lines.append(f"  {ext}: {count:,} files ({percentage:.1f}%)")
        lines.append("")

        # Tree view
        if include_tree:
            lines.append("="*80)
            lines.append("FOLDER STRUCTURE (TREE VIEW)")
            lines.append("="*80)
            lines.append(f"{structure['name']}/")

            subfolders = structure.get('subfolders', [])
            for i, subfolder in enumerate(subfolders):
                is_last = (i == len(subfolders) - 1)
                lines.extend(self._generate_tree_view(subfolder, "", is_last))
            lines.append("")

        # Detailed report
        if include_detailed:
            lines.append("="*80)
            lines.append("DETAILED FOLDER REPORT")
            lines.append("="*80)
            lines.append("")
            lines.extend(self._generate_detailed_report(structure))

        # Join all lines
        guide_text = "\n".join(lines)

        # Save to file if requested
        if output_path:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(guide_text)
            print(f"\nGuide saved to: {output_path}")

        return guide_text

    def export_json(self, output_path: str):
        """Export the structure as JSON for programmatic access."""
        structure = self.explore()

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(structure, f, indent=2)

        print(f"JSON structure saved to: {output_path}")

    def get_folder_summary(self, folder_path: str = None) -> Dict:
        """Get a quick summary of a specific folder."""
        if folder_path is None:
            folder_path = self.root_path
        else:
            folder_path = Path(folder_path)

        if not folder_path.exists():
            raise ValueError(f"Folder does not exist: {folder_path}")

        summary = {
            'path': str(folder_path),
            'name': folder_path.name,
            'files': 0,
            'folders': 0,
            'size': 0,
            'file_types': {}
        }

        try:
            for item in folder_path.iterdir():
                if item.is_file():
                    summary['files'] += 1
                    summary['size'] += item.stat().st_size
                    ext = item.suffix.lower() or 'no_extension'
                    summary['file_types'][ext] = summary['file_types'].get(ext, 0) + 1
                elif item.is_dir():
                    summary['folders'] += 1
        except (PermissionError, OSError) as e:
            summary['error'] = str(e)

        return summary


def main():
    """Command-line interface for the Drive Explorer."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Explore Google Drive folder structure and generate a guide'
    )
    parser.add_argument(
        '--path',
        default='/content/drive/MyDrive',
        help='Root path to explore (default: /content/drive/MyDrive)'
    )
    parser.add_argument(
        '--output',
        default='drive_structure_guide.txt',
        help='Output file path (default: drive_structure_guide.txt)'
    )
    parser.add_argument(
        '--max-depth',
        type=int,
        default=None,
        help='Maximum depth to explore (default: unlimited)'
    )
    parser.add_argument(
        '--json',
        default=None,
        help='Also export as JSON to specified file'
    )
    parser.add_argument(
        '--no-tree',
        action='store_true',
        help='Skip tree view in the guide'
    )
    parser.add_argument(
        '--no-detailed',
        action='store_true',
        help='Skip detailed report in the guide'
    )

    args = parser.parse_args()

    try:
        explorer = DriveExplorer(args.path)

        print(f"Starting exploration of: {args.path}")
        if args.max_depth:
            print(f"Maximum depth: {args.max_depth}")

        guide = explorer.generate_guide(
            output_path=args.output,
            include_tree=not args.no_tree,
            include_detailed=not args.no_detailed,
            max_depth=args.max_depth
        )

        if args.json:
            explorer.export_json(args.json)

        print("\n" + "="*80)
        print("EXPLORATION COMPLETE!")
        print("="*80)
        print(f"Total Files: {explorer.total_files:,}")
        print(f"Total Folders: {explorer.total_folders:,}")
        print(f"Total Size: {explorer._format_size(explorer.total_size)}")
        print(f"\nGuide saved to: {args.output}")

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
