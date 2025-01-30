#!/usr/bin/env python3
"""
CCDA Section Analyzer

This script analyzes CCDA XML files to create a comprehensive index of all sections,
their frequencies, and associated metadata (templateIds, codes, titles, etc.).
This helps ensure completeness of section analysis and proper weight assignment.
"""

import os
import sys
import json
import logging
from pathlib import Path
from typing import Dict, List, Set, Counter
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

class CCDASectionAnalyzer:
    """Analyzes CCDA sections across multiple files to build a comprehensive index."""
    
    def __init__(self):
        self.section_index = defaultdict(lambda: {
            'count': 0,
            'files': set(),
            'template_ids': set(),
            'codes': set(),
            'titles': set(),
            'total_entries': 0,
            'total_coded_elements': 0,
            'total_text_length': 0,
            'example_files': []  # Will store up to 5 example files
        })
        self.total_files = 0
        
    def analyze_section(self, section: etree._Element, file_path: str) -> Dict:
        """
        Analyze a single section element and extract all relevant metadata.
        """
        # Get section identifiers
        template_ids = section.xpath('.//h:templateId/@root', namespaces=CCDA_NS)
        codes = section.xpath('./h:code/@code', namespaces=CCDA_NS)
        code_systems = section.xpath('./h:code/@codeSystem', namespaces=CCDA_NS)
        titles = section.xpath('./h:title/text()', namespaces=CCDA_NS)
        
        # Get section content metrics
        entries = section.xpath('.//h:entry', namespaces=CCDA_NS)
        coded_elements = section.xpath('.//*[@code]', namespaces=CCDA_NS)
        text_elements = section.xpath('.//h:text//text()', namespaces=CCDA_NS)
        text_content = ' '.join(text for text in text_elements if text.strip())
        
        return {
            'template_ids': template_ids,
            'codes': list(zip(codes, code_systems)) if len(codes) == len(code_systems) else codes,
            'titles': titles,
            'entry_count': len(entries),
            'coded_element_count': len(coded_elements),
            'text_length': len(text_content.split())
        }
        
    def update_section_index(self, section_data: Dict, section_id: str, file_path: str):
        """
        Update the section index with new section data.
        """
        index_entry = self.section_index[section_id]
        index_entry['count'] += 1
        index_entry['files'].add(file_path)
        index_entry['template_ids'].update(section_data['template_ids'])
        index_entry['codes'].update(section_data['codes'])
        index_entry['titles'].update(section_data['titles'])
        index_entry['total_entries'] += section_data['entry_count']
        index_entry['total_coded_elements'] += section_data['coded_element_count']
        index_entry['total_text_length'] += section_data['text_length']
        
        # Keep track of example files (up to 5)
        if len(index_entry['example_files']) < 5:
            index_entry['example_files'].append(file_path)
    
    def analyze_file(self, file_path: str) -> Dict:
        """
        Analyze sections in a single CCDA XML file.
        """
        try:
            # Parse the XML file
            parser = etree.XMLParser(remove_blank_text=True)
            tree = etree.parse(file_path, parser)
            root = tree.getroot()
            
            # Find all sections
            sections = root.xpath('//h:section', namespaces=CCDA_NS)
            
            file_sections = {}
            for section in sections:
                # Get section identifiers
                template_ids = section.xpath('.//h:templateId/@root', namespaces=CCDA_NS)
                codes = section.xpath('./h:code/@code', namespaces=CCDA_NS)
                
                # Use template ID or code as section identifier
                section_id = template_ids[0] if template_ids else (codes[0] if codes else None)
                
                if section_id:
                    section_data = self.analyze_section(section, file_path)
                    self.update_section_index(section_data, section_id, file_path)
                    file_sections[section_id] = section_data
            
            return file_sections
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {str(e)}")
            return {}
    
    def analyze_directory(self, input_dir: str, output_file: str = 'ccda_section_index.json', batch_size: int = 15, memory_limit: int = 8000):
        """
        Analyze all XML files in the directory and build a comprehensive section index.
        """
        input_path = Path(input_dir)
        xml_files = list(input_path.glob('*.xml'))
        self.total_files = len(xml_files)
        
        logger.info(f"Analyzing sections in {self.total_files} CCDA XML files...")
        
        for xml_file in tqdm(xml_files, desc="Processing files"):
            self.analyze_file(str(xml_file))
        
        # Convert sets to lists for JSON serialization
        serializable_index = {}
        for section_id, data in self.section_index.items():
            serializable_index[section_id] = {
                'count': data['count'],
                'frequency': data['count'] / self.total_files,
                'files': len(data['files']),
                'template_ids': list(data['template_ids']),
                'codes': list(data['codes']),
                'titles': list(data['titles']),
                'total_entries': data['total_entries'],
                'avg_entries': data['total_entries'] / data['count'] if data['count'] > 0 else 0,
                'total_coded_elements': data['total_coded_elements'],
                'avg_coded_elements': data['total_coded_elements'] / data['count'] if data['count'] > 0 else 0,
                'total_text_length': data['total_text_length'],
                'avg_text_length': data['total_text_length'] / data['count'] if data['count'] > 0 else 0,
                'example_files': data['example_files'][:5]  # Include up to 5 example files
            }
        
        # Sort sections by frequency
        sorted_index = dict(sorted(
            serializable_index.items(),
            key=lambda x: x[1]['count'],
            reverse=True
        ))
        
        # Save the index
        with open(output_file, 'w') as f:
            json.dump(sorted_index, f, indent=2)
        
        # Print summary
        logger.info(f"\nAnalysis complete. Found {len(self.section_index)} unique sections across {self.total_files} files.")
        logger.info("\nTop 10 Most Common Sections:")
        for i, (section_id, data) in enumerate(list(sorted_index.items())[:10], 1):
            titles = data['titles']
            title = titles[0] if titles else 'Untitled'
            logger.info(f"{i}. {title}")
            logger.info(f"   ID: {section_id}")
            logger.info(f"   Frequency: {data['frequency']:.2%} ({data['count']} files)")
            logger.info(f"   Avg. Entries: {data['avg_entries']:.1f}")
            logger.info(f"   Avg. Coded Elements: {data['avg_coded_elements']:.1f}")
            logger.info(f"   Avg. Text Length: {data['avg_text_length']:.1f} words")

def main():
    parser = argparse.ArgumentParser(
        description='Analyze CCDA sections and their content'
    )
    parser.add_argument(
        '--input-dir',
        default='input/ccda',
        help='Directory containing CCDA XML files'
    )
    parser.add_argument(
        '--output-file',
        default='output/analysis/metrics/section_analysis.json',
        help='Output JSON file for section analysis results'
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
    
    analyzer = CCDASectionAnalyzer()
    analyzer.analyze_directory(
        args.input_dir,
        args.output_file,
        args.batch_size,
        args.memory_limit
    )

if __name__ == '__main__':
    main() 