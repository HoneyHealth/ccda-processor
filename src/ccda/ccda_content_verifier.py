#!/usr/bin/env python3
"""
CCDA Reformatting Verification Script

This script:
1. Randomly selects 20 files from the reformatted directory
2. Compares them with their original versions
3. Reports any differences after removing whitespace
"""

import os
import random
import logging
from pathlib import Path
from lxml import etree
import difflib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def normalize_xml(file_path):
    """Parse XML and return normalized string without whitespace."""
    try:
        parser = etree.XMLParser(remove_blank_text=True)
        tree = etree.parse(file_path, parser)
        # Convert to string and remove all whitespace
        xml_str = etree.tostring(tree, encoding='utf-8', xml_declaration=True)
        return xml_str.decode('utf-8').replace(' ', '').replace('\n', '')
    except Exception as e:
        logger.error(f"Failed to parse {file_path}: {str(e)}")
        return None

def compare_files(original_path, reformatted_path):
    """Compare two XML files after normalizing."""
    original_xml = normalize_xml(original_path)
    reformatted_xml = normalize_xml(reformatted_path)
    
    if original_xml is None or reformatted_xml is None:
        return False, "Failed to parse one or both files"
    
    if original_xml == reformatted_xml:
        return True, None
    
    # If different, generate diff
    diff = difflib.unified_diff(
        original_xml.splitlines(),
        reformatted_xml.splitlines(),
        fromfile=str(original_path),
        tofile=str(reformatted_path),
        lineterm=''
    )
    return False, '\n'.join(list(diff)[:10])  # Show first 10 lines of diff

def main():
    # Paths
    reformatted_dir = Path("output/reformatted")
    original_dir = Path("input/ccda")
    
    # Get all reformatted files
    reformatted_files = list(reformatted_dir.glob("*.xml"))
    
    # Randomly select 20 files
    sample_size = min(20, len(reformatted_files))
    selected_files = random.sample(reformatted_files, sample_size)
    
    # Compare each file
    results = {
        'total': sample_size,
        'matches': 0,
        'differences': 0,
        'errors': 0
    }
    
    logger.info(f"Comparing {sample_size} randomly selected files...")
    
    for reformatted_file in selected_files:
        original_file = original_dir / reformatted_file.name
        
        if not original_file.exists():
            logger.error(f"Original file not found: {original_file}")
            results['errors'] += 1
            continue
        
        logger.info(f"Comparing {reformatted_file.name}...")
        identical, diff = compare_files(original_file, reformatted_file)
        
        if identical:
            logger.info("✓ Files are identical")
            results['matches'] += 1
        else:
            logger.error("✗ Files differ:")
            if diff:
                logger.error(diff)
            results['differences'] += 1
    
    # Print summary
    logger.info("\nVerification Summary:")
    logger.info(f"Total files checked: {results['total']}")
    logger.info(f"Identical files: {results['matches']}")
    logger.info(f"Different files: {results['differences']}")
    logger.info(f"Errors: {results['errors']}")
    
    if results['matches'] == results['total']:
        logger.info("\nAll checked files are identical! ✓")
    else:
        logger.error("\nSome files have differences! ✗")

if __name__ == '__main__':
    main() 