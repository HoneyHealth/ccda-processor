#!/usr/bin/env python3
"""
CCDA Configuration Generator

This script reads the section analysis results and generates a configuration file
containing all CCDA sections with their weights and metadata.
"""

import json
import logging
from pathlib import Path
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def calculate_section_weight(section_data):
    """
    Calculate an appropriate weight for a section based on its metrics.
    Uses a combination of frequency, content density, and clinical importance.
    """
    # Base weight starts at 0.3
    weight = 0.3
    
    # Frequency bonus (up to 0.2)
    if section_data['frequency'] >= 0.95:  # Present in 95%+ of files
        weight += 0.2
    elif section_data['frequency'] >= 0.75:  # Present in 75%+ of files
        weight += 0.1
    
    # Content density bonus (up to 0.3)
    content_score = (
        section_data['avg_entries'] * 0.1 +
        section_data['avg_coded_elements'] * 0.05 +
        section_data['avg_text_length'] * 0.001
    )
    weight += min(0.3, content_score)
    
    # Clinical importance bonus (up to 0.2)
    # Based on section type and typical clinical workflow
    clinical_importance = {
        '2.16.840.1.113883.10.20.22.2.3.1': 0.2,  # Results
        '2.16.840.1.113883.10.20.22.2.5.1': 0.2,  # Problems
        '2.16.840.1.113883.10.20.22.2.1.1': 0.2,  # Medications
        '2.16.840.1.113883.10.20.22.2.6': 0.2,    # Allergies
        '2.16.840.1.113883.10.20.22.2.4.1': 0.15, # Vital Signs
        '2.16.840.1.113883.10.20.22.2.22.1': 0.15,# Encounters
        '2.16.840.1.113883.10.20.22.2.7.1': 0.15, # Procedures
    }
    weight += clinical_importance.get(section_data['id'], 0.0)
    
    # Round to 2 decimal places and cap at 1.0
    return min(1.0, round(weight, 2))

def generate_config(analysis_file: str, output_file: str):
    """
    Generate CCDA configuration file from analysis results.
    """
    # Read analysis results
    with open(analysis_file) as f:
        analysis_data = json.load(f)
    
    # Generate configuration
    config = {
        "version": "1.0",
        "description": "CCDA sections configuration generated from analysis of actual CCDA files",
        "sections": {}
    }
    
    for section_id, data in analysis_data.items():
        # Add section ID to the data for weight calculation
        data['id'] = section_id
        weight = calculate_section_weight(data)
        
        # Get the most common title
        title = data['titles'][0] if data['titles'] else 'Unknown Section'
        
        config['sections'][section_id] = {
            "title": title,
            "weight": weight,
            "frequency": data['frequency'],
            "metrics": {
                "avg_entries": data['avg_entries'],
                "avg_coded_elements": data['avg_coded_elements'],
                "avg_text_length": data['avg_text_length']
            },
            "comment": generate_section_comment(data, weight)
        }
    
    # Save configuration
    with open(output_file, 'w') as f:
        json.dump(config, f, indent=2)
    
    logger.info(f"Generated configuration file: {output_file}")
    logger.info("\nSection weight summary:")
    for section_id, data in sorted(
        config['sections'].items(),
        key=lambda x: x[1]['weight'],
        reverse=True
    ):
        logger.info(f"{data['title']:<30} Weight: {data['weight']:.2f}")

def generate_section_comment(data, weight):
    """Generate a helpful comment explaining the weight assignment."""
    comments = []
    
    # Frequency comment
    if data['frequency'] >= 0.95:
        comments.append("Core section (present in 95%+ of files)")
    elif data['frequency'] >= 0.75:
        comments.append("Common section (present in 75%+ of files)")
    else:
        comments.append(f"Present in {data['frequency']:.1%} of files")
    
    # Content density comment
    if data['avg_entries'] > 10 or data['avg_coded_elements'] > 100:
        comments.append("High content density")
    elif data['avg_entries'] > 5 or data['avg_coded_elements'] > 50:
        comments.append("Moderate content density")
    
    # Text content comment
    if data['avg_text_length'] > 1000:
        comments.append("Rich narrative content")
    elif data['avg_text_length'] > 100:
        comments.append("Moderate narrative content")
    
    return ". ".join(comments)

def main():
    parser = argparse.ArgumentParser(
        description='Generate CCDA sections configuration from analysis results'
    )
    parser.add_argument(
        '--analysis-file',
        default='ccda_section_index.json',
        help='Input analysis results file'
    )
    parser.add_argument(
        '--output-file',
        default='ccda_sections_config.json',
        help='Output configuration file'
    )
    
    args = parser.parse_args()
    generate_config(args.analysis_file, args.output_file)

if __name__ == '__main__':
    main() 