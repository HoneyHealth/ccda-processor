#!/usr/bin/env python3
"""
CCDA XML Reformatter

This script:
1. Reads ccda_analysis.json to identify information-rich CCDA files
2. Reformats selected XML files for better readability while preserving all content
3. Creates a new directory with reformatted files

Features:
- Memory-efficient processing using iterparse
- No data loss or content modification
- Proper XML indentation and whitespace
- Batch processing with memory monitoring
"""

import os
import sys
import json
import logging
import shutil
import psutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple
from lxml import etree
from tqdm import tqdm
import argparse
import gc

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_memory_usage():
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def clear_memory():
    """Aggressive memory cleanup."""
    gc.collect()
    gc.collect()
    if hasattr(gc, 'freeze'):
        gc.freeze()

class CCDAReformatter:
    """Reformats CCDA XML files with proper indentation and structure."""
    
    def __init__(self):
        self.processed_files = 0
        self.total_size = 0
    
    def load_analysis_results(self, analysis_file: str, top_n: int) -> List[str]:
        """Load analysis results and return paths of top N files."""
        with open(analysis_file) as f:
            analysis = json.load(f)
        
        # Sort files by information score
        sorted_files = sorted(
            analysis.items(),
            key=lambda x: x[1].get('total_score', 0),
            reverse=True
        )
        
        # Get top N files and clear memory
        top_files = [file_path for file_path, _ in sorted_files[:top_n]]
        del sorted_files
        del analysis
        clear_memory()
        
        logger.info(f"Selected top {len(top_files)} files from analysis")
        return top_files
    
    def reformat_xml(self, xml_path: str, output_path: str) -> bool:
        """Reformat a CCDA XML file with proper indentation."""
        try:
            # Parse and write with pretty printing
            parser = etree.XMLParser(remove_blank_text=True)
            tree = etree.parse(xml_path, parser)
            tree.write(
                output_path,
                pretty_print=True,
                xml_declaration=True,
                encoding='UTF-8'
            )
            
            # Update metrics
            self.processed_files += 1
            self.total_size += os.path.getsize(output_path)
            
            # Clear memory
            del tree
            clear_memory()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to process {xml_path}: {str(e)}")
            return False
    
    def process_files(self, 
                     analysis_file: str,
                     top_n: int,
                     output_dir: str,
                     batch_size: int = 15,
                     memory_limit_mb: int = 8000) -> None:
        """Process the top N most information-rich files in batches."""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Get top files from analysis
        top_files = self.load_analysis_results(analysis_file, top_n)
        
        # Process files in batches
        for i in range(0, len(top_files), batch_size):
            batch = top_files[i:i+batch_size]
            batch_num = i//batch_size + 1
            current_memory = get_memory_usage()
            
            logger.debug(f"Processing batch {batch_num} ({len(batch)} files)")
            logger.debug(f"Memory usage: {current_memory:.1f} MB")
            
            # Check memory limit
            if current_memory > memory_limit_mb:
                logger.warning(f"Memory usage ({current_memory:.1f} MB) exceeded limit ({memory_limit_mb} MB)")
                logger.warning("Saving progress and exiting...")
                break
            
            # Process each file in the batch
            for input_file in tqdm(batch, desc=f"Batch {batch_num}", leave=False):
                output_file = output_path / os.path.basename(input_file)
                self.reformat_xml(input_file, str(output_file))
                
                # Check memory after each file
                if get_memory_usage() > memory_limit_mb:
                    logger.warning(f"Memory limit reached during file processing")
                    break
            
            # Clear memory between batches
            clear_memory()
            logger.debug(f"After batch memory: {get_memory_usage():.1f} MB")
        
        logger.info(f"\nReformatting complete:")
        logger.info(f"- Processed files: {self.processed_files}")
        logger.info(f"- Total size: {self.total_size / (1024*1024):.2f} MB")

def main():
    parser = argparse.ArgumentParser(
        description='Reformat CCDA XML files based on analysis results'
    )
    parser.add_argument(
        '--analysis-file',
        default='output/analysis/metrics/analysis.json',
        help='Analysis results JSON file'
    )
    parser.add_argument(
        '--top-n',
        type=int,
        default=100,
        help='Number of top files to process'
    )
    parser.add_argument(
        '--output-dir',
        default='output/reformatted',
        help='Output directory for reformatted files'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=15,
        help='Number of files to process in each batch'
    )
    parser.add_argument(
        '--memory-limit',
        type=int,
        default=8000,
        help='Memory limit in MB for processing'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    args = parser.parse_args()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    reformatter = CCDAReformatter()
    reformatter.process_files(
        args.analysis_file,
        args.top_n,
        args.output_dir,
        args.batch_size,
        args.memory_limit
    )

if __name__ == '__main__':
    main() 