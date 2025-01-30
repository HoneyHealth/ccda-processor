#!/usr/bin/env python3
"""
CCDA XML Analyzer for Large Datasets

This script analyzes CCDA XML files to determine information richness,
optimized for processing large datasets (30,000+ files) with memory efficiency.

Features:
- Incremental processing with periodic result saving
- Memory-efficient XML parsing
- Progress tracking and recovery
- Detailed logging and error handling
"""

import os
import sys
import json
import logging
import gc
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple, Generator
import xml.etree.ElementTree as ET
from collections import defaultdict
import argparse
from lxml import etree
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# CCDA namespace
CCDA_NS = {
    'h': 'urn:hl7-org:v3',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
}

class CCDAAnalyzer:
    """Analyzes CCDA XML files for information richness."""
    
    def __init__(self, checkpoint_dir: str = 'analysis_checkpoints'):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(exist_ok=True)
        self.results = {}
        self.current_batch = 0
        self.processed_files = set()
        self.load_checkpoints()
    
    def load_checkpoints(self):
        """Load any existing checkpoint files."""
        checkpoint_files = list(self.checkpoint_dir.glob('analysis_batch_*.json'))
        if checkpoint_files:
            logger.info(f"Found {len(checkpoint_files)} checkpoint files")
            for cp_file in checkpoint_files:
                with open(cp_file) as f:
                    batch_results = json.load(f)
                self.results.update(batch_results)
                self.processed_files.update(batch_results.keys())
            self.current_batch = len(checkpoint_files)
            logger.info(f"Loaded {len(self.results)} analyzed files from checkpoints")
    
    def save_checkpoint(self, batch_results: Dict, batch_num: int):
        """Save analysis results for the current batch."""
        checkpoint_file = self.checkpoint_dir / f'analysis_batch_{batch_num}.json'
        with open(checkpoint_file, 'w') as f:
            json.dump(batch_results, f, indent=2)
        logger.debug(f"Saved checkpoint {batch_num} with {len(batch_results)} files")
    
    def merge_checkpoints(self, output_file: str):
        """Merge all checkpoint files into final results file."""
        logger.info("Merging checkpoint files...")
        
        # Sort results by score
        sorted_results = dict(sorted(
            self.results.items(),
            key=lambda x: x[1].get('total_score', 0),
            reverse=True
        ))
        
        # Save merged results
        with open(output_file, 'w') as f:
            json.dump(sorted_results, f, indent=2)
        
        logger.info(f"Analysis results saved to {output_file}")
        
        # Print summary of top files
        logger.info("\nTop 10 Most Information-Rich Files:")
        for i, (file_path, metrics) in enumerate(list(sorted_results.items())[:10], 1):
            logger.info(f"{i}. {file_path}")
            logger.info(f"   Score: {metrics['total_score']:.2f}")
            logger.info(f"   Unique Sections: {metrics['unique_sections']}")
    
    def calculate_section_score(self, section_elem: etree._Element) -> float:
        """Calculate a score for a single section."""
        try:
            score = 0.0
            
            # Count entries (weight: 0.5 per entry)
            entries = section_elem.xpath('.//h:entry', namespaces=CCDA_NS)
            score += len(entries) * 0.5
            
            # Check for coded elements (weight: 0.3 per code)
            coded_elements = section_elem.xpath('.//*[@code]', namespaces=CCDA_NS)
            score += len(coded_elements) * 0.3
            
            # Analyze text content (weight: 0.1 per word)
            text_elements = section_elem.xpath('.//h:text//text()', namespaces=CCDA_NS)
            text_content = ' '.join(text for text in text_elements if text.strip())
            score += len(text_content.split()) * 0.1
            
            return score
            
        except Exception as e:
            logger.error(f"Error calculating section score: {str(e)}")
            return 0.0
    
    def analyze_file(self, file_path: str) -> Dict:
        """Analyze a single CCDA XML file."""
        try:
            metrics = {
                'file_size': os.path.getsize(file_path),
                'section_scores': defaultdict(float),
                'total_score': 0.0,
                'unique_sections': 0,
                'error': None
            }
            
            # Use iterparse for memory efficiency
            context = etree.iterparse(file_path, events=('end',), tag='{urn:hl7-org:v3}section')
            
            for event, elem in context:
                if elem.tag.endswith('section'):
                    template_ids = elem.xpath('.//h:templateId/@root', namespaces=CCDA_NS)
                    if template_ids:
                        section_id = template_ids[0]
                        section_score = self.calculate_section_score(elem)
                        metrics['section_scores'][section_id] = section_score
                
                # Clear element to free memory
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
            
            # Calculate final metrics
            metrics['unique_sections'] = len(metrics['section_scores'])
            metrics['total_score'] = sum(metrics['section_scores'].values())
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {str(e)}")
            metrics['error'] = str(e)
            return metrics
    
    def process_batch(self, files: List[Path], batch_num: int) -> Dict:
        """Process a batch of files and return their results."""
        batch_results = {}
        
        for xml_file in tqdm(files, desc=f"Batch {batch_num}", leave=False):
            if str(xml_file) in self.processed_files:
                continue
                
            try:
                metrics = self.analyze_file(str(xml_file))
                batch_results[str(xml_file)] = metrics
            except Exception as e:
                logger.error(f"Error in batch {batch_num} processing {xml_file}: {str(e)}")
        
        return batch_results
    
    def analyze_directory(self, 
                         input_dir: str,
                         output_file: str = 'ccda_analysis.json',
                         batch_size: int = 100,
                         memory_limit: int = 8000) -> None:
        """
        Analyze all XML files in the input directory with memory-efficient batch processing.
        """
        input_path = Path(input_dir)
        xml_files = list(input_path.glob('*.xml'))
        total_files = len(xml_files)
        
        logger.info(f"Found {total_files} XML files in {input_dir}")
        logger.info(f"Already processed: {len(self.processed_files)} files")
        
        # Process files in batches
        for i in range(0, total_files, batch_size):
            batch = xml_files[i:i+batch_size]
            batch_num = self.current_batch + (i // batch_size) + 1
            
            # Skip if all files in batch are already processed
            if all(str(f) in self.processed_files for f in batch):
                continue
            
            logger.debug(f"Processing batch {batch_num} ({len(batch)} files)")
            batch_results = self.process_batch(batch, batch_num)
            
            # Update results and save checkpoint
            self.results.update(batch_results)
            self.save_checkpoint(batch_results, batch_num)
            
            # Clear memory
            gc.collect()
        
        # Merge all checkpoints into final results
        self.merge_checkpoints(output_file)

def main():
    parser = argparse.ArgumentParser(
        description='Analyze CCDA XML files for information richness'
    )
    parser.add_argument(
        '--input-dir',
        default='input/ccda',
        help='Directory containing CCDA XML files'
    )
    parser.add_argument(
        '--output-file',
        default='output/analysis/metrics/analysis.json',
        help='Output JSON file for analysis results'
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
    
    analyzer = CCDAAnalyzer()
    analyzer.analyze_directory(
        args.input_dir,
        args.output_file,
        args.batch_size,
        args.memory_limit
    )

if __name__ == '__main__':
    main() 