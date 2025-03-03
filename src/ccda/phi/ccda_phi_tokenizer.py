#!/usr/bin/env python3
"""
CCDA PHI Tokenizer

This script extends the PHI extractor to prepare PHI data for tokenization.
It ensures all PHI elements are properly extracted and structured for tokenization purposes.
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional, Set, Tuple
from lxml import etree
from tqdm import tqdm

# Import the PHI extractor
from ccda_phi_extractor import CCDAPHIExtractor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CCDAPHITokenizer:
    """Prepares PHI data for tokenization."""
    
    def __init__(self):
        """Initialize the tokenizer."""
        self.extractor = CCDAPHIExtractor()
        self.processed_files = 0
        self.failed_files = 0
        self.all_tokens = set()
    
    def normalize_name(self, name_data: Dict[str, Any]) -> Dict[str, str]:
        """
        Normalize name data for tokenization.
        
        Args:
            name_data: Raw name data from PHI extractor
            
        Returns:
            Normalized name data for tokenization
        """
        if not name_data:
            return {}
        
        # Create normalized name with combined components
        normalized = {
            "name": name_data.get('formatted', ''),
            "name_prefix": ' '.join(name_data.get('prefix', [])),
            "name_given": ' '.join(name_data.get('given', [])),
            "name_family": ' '.join(name_data.get('family', [])),
            "name_suffix": ' '.join(name_data.get('suffix', []))
        }
        
        # Add these to the token set
        for key, value in normalized.items():
            if value:
                self.all_tokens.add(value)
        
        return normalized
    
    def normalize_address(self, address_data: Dict[str, Any]) -> Dict[str, str]:
        """
        Normalize address data for tokenization.
        
        Args:
            address_data: Raw address data from PHI extractor
            
        Returns:
            Normalized address data for tokenization
        """
        if not address_data:
            return {}
        
        # Extract individual components
        street_lines = address_data.get('street_lines', [])
        street = ' '.join(street_lines) if street_lines else ''
        
        normalized = {
            "address": address_data.get('formatted', ''),
            "address_street": street,
            "address_city": address_data.get('city', ''),
            "address_state": address_data.get('state', ''),
            "address_zip": address_data.get('postal_code', ''),
            "address_country": address_data.get('country', '')
        }
        
        # Add these to the token set
        for key, value in normalized.items():
            if value:
                self.all_tokens.add(value)
        
        return normalized
    
    def normalize_telecom(self, telecom_data: Dict[str, Any]) -> Dict[str, str]:
        """
        Normalize telecom data for tokenization.
        
        Args:
            telecom_data: Raw telecom data from PHI extractor
            
        Returns:
            Normalized telecom data for tokenization
        """
        if not telecom_data:
            return {}
        
        value = telecom_data.get('value', '')
        if value:
            self.all_tokens.add(value)
        
        return {"contact": value}
    
    def normalize_date(self, date_data: Dict[str, Any]) -> Dict[str, str]:
        """
        Normalize date data for tokenization.
        
        Args:
            date_data: Raw date data from PHI extractor
            
        Returns:
            Normalized date data for tokenization
        """
        if not date_data:
            return {}
        
        value = date_data.get('value', '')
        
        # Format the date (YYYYMMDD to YYYY-MM-DD)
        formatted = ''
        if value and len(value) == 8:
            try:
                formatted = f"{value[0:4]}-{value[4:6]}-{value[6:8]}"
            except:
                formatted = value
        else:
            formatted = value
        
        if formatted:
            self.all_tokens.add(formatted)
        
        return {"date": formatted}
    
    def normalize_identifier(self, id_data: Dict[str, Any]) -> Dict[str, str]:
        """
        Normalize identifier data for tokenization.
        
        Args:
            id_data: Raw identifier data from PHI extractor
            
        Returns:
            Normalized identifier data for tokenization
        """
        if not id_data:
            return {}
        
        id_value = id_data.get('extension', '')
        if id_value:
            self.all_tokens.add(id_value)
        
        return {"identifier": id_value}
    
    def prepare_phi_for_tokenization(self, phi_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare PHI data for tokenization.
        
        Args:
            phi_data: Raw PHI data from extractor
            
        Returns:
            Prepared PHI data for tokenization
        """
        tokenization_data = {
            "names": [],
            "addresses": [],
            "contacts": [],
            "dates": [],
            "identifiers": [],
            "demographics": {},
            "all_tokens": []
        }
        
        # Process names
        for name in phi_data.get('names', []):
            normalized = self.normalize_name(name)
            if normalized:
                tokenization_data["names"].append(normalized)
        
        # Process addresses
        for address in phi_data.get('addresses', []):
            normalized = self.normalize_address(address)
            if normalized:
                tokenization_data["addresses"].append(normalized)
        
        # Process telecoms (phone numbers, emails)
        for telecom in phi_data.get('telecoms', []):
            normalized = self.normalize_telecom(telecom)
            if normalized:
                tokenization_data["contacts"].append(normalized)
        
        # Process birthdate
        birthdate = phi_data.get('birthtime', {})
        if birthdate:
            normalized = self.normalize_date(birthdate)
            if normalized:
                tokenization_data["dates"].append(normalized)
                tokenization_data["demographics"]["birthdate"] = normalized["date"]
        
        # Process identifiers
        for id_item in phi_data.get('ids', []):
            normalized = self.normalize_identifier(id_item)
            if normalized:
                tokenization_data["identifiers"].append(normalized)
        
        # Process gender
        gender = phi_data.get('gender', {})
        if gender:
            gender_code = gender.get('code', '')
            gender_display = gender.get('display_name', '')
            if gender_code:
                tokenization_data["demographics"]["gender_code"] = gender_code
                self.all_tokens.add(gender_code)
            if gender_display:
                tokenization_data["demographics"]["gender"] = gender_display
                self.all_tokens.add(gender_display)
        
        # Process other demographic data
        demographic_fields = ['marital_status', 'race', 'ethnicity', 'language']
        for field in demographic_fields:
            data = phi_data.get(field, {})
            if data:
                code = data.get('code', '')
                display = data.get('display_name', '')
                if code:
                    tokenization_data["demographics"][f"{field}_code"] = code
                    self.all_tokens.add(code)
                if display:
                    tokenization_data["demographics"][field] = display
                    self.all_tokens.add(display)
        
        # Add all tokens
        tokenization_data["all_tokens"] = list(self.all_tokens)
        
        return tokenization_data
    
    def process_file(self, file_path: str) -> Dict[str, Any]:
        """
        Process a single CCDA file for tokenization.
        
        Args:
            file_path: Path to the CCDA XML file
            
        Returns:
            Tokenization data
        """
        try:
            # Clear tokens for this file
            self.all_tokens = set()
            
            # Extract PHI
            extraction_result = self.extractor.extract_phi_from_file(file_path)
            phi_data = extraction_result["phi_data"]
            
            # Prepare for tokenization
            tokenization_data = self.prepare_phi_for_tokenization(phi_data)
            
            # Add file metadata
            result = {
                "file_name": os.path.basename(file_path),
                "tokenization_data": tokenization_data
            }
            
            self.processed_files += 1
            return result
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {str(e)}")
            self.failed_files += 1
            return {
                "file_name": os.path.basename(file_path),
                "tokenization_data": {},
                "error": str(e)
            }
    
    def process_directory(self, input_dir: str, output_file: str):
        """
        Process all CCDA files in a directory for tokenization.
        
        Args:
            input_dir: Directory containing CCDA XML files
            output_file: Path to save the tokenization data
        """
        input_path = Path(input_dir)
        xml_files = list(input_path.glob('*.xml'))
        
        logger.info(f"Found {len(xml_files)} CCDA XML files to process")
        
        results = []
        unique_tokens = set()
        
        for xml_file in tqdm(xml_files, desc="Preparing for tokenization"):
            result = self.process_file(str(xml_file))
            results.append(result)
            
            # Collect all unique tokens across files
            if "tokenization_data" in result and "all_tokens" in result["tokenization_data"]:
                unique_tokens.update(result["tokenization_data"]["all_tokens"])
        
        # Create output directory if it doesn't exist
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save the results
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        # Save the unique token list separately
        token_file = output_path.parent / 'unique_tokens.json'
        with open(token_file, 'w') as f:
            json.dump(list(unique_tokens), f, indent=2)
        
        logger.info(f"\nTokenization preparation complete:")
        logger.info(f"- Successfully processed: {self.processed_files} files")
        logger.info(f"- Failed: {self.failed_files} files")
        logger.info(f"- Total unique tokens: {len(unique_tokens)}")
        logger.info(f"- Output: {output_file}")
        logger.info(f"- Token list: {token_file}")

def main():
    parser = argparse.ArgumentParser(
        description='Prepare PHI data from CCDA XML files for tokenization'
    )
    parser.add_argument(
        '--input-dir',
        default='input/ccda/to_process',
        help='Directory containing CCDA XML files'
    )
    parser.add_argument(
        '--output-file',
        default='output/phi/tokenization_data.json',
        help='Output JSON file for tokenization data'
    )
    parser.add_argument(
        '--sample-size',
        type=int,
        default=0,
        help='Process only a sample of files (0 for all files)'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    args = parser.parse_args()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    tokenizer = CCDAPHITokenizer()
    
    if args.sample_size > 0:
        # Use a subset of files
        import random
        input_path = Path(args.input_dir)
        xml_files = list(input_path.glob('*.xml'))
        
        if len(xml_files) <= args.sample_size:
            sampled_files = xml_files
        else:
            sampled_files = random.sample(xml_files, args.sample_size)
        
        logger.info(f"Sampled {len(sampled_files)} files for tokenization preparation")
        
        results = []
        unique_tokens = set()
        
        for xml_file in tqdm(sampled_files, desc="Preparing for tokenization"):
            result = tokenizer.process_file(str(xml_file))
            results.append(result)
            
            # Collect all unique tokens across files
            if "tokenization_data" in result and "all_tokens" in result["tokenization_data"]:
                unique_tokens.update(result["tokenization_data"]["all_tokens"])
        
        # Create output directory if it doesn't exist
        output_path = Path(args.output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save the results
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2)
        
        # Save the unique token list separately
        token_file = output_path.parent / 'unique_tokens.json'
        with open(token_file, 'w') as f:
            json.dump(list(unique_tokens), f, indent=2)
        
        logger.info(f"\nTokenization preparation complete:")
        logger.info(f"- Successfully processed: {tokenizer.processed_files} files")
        logger.info(f"- Failed: {tokenizer.failed_files} files")
        logger.info(f"- Total unique tokens: {len(unique_tokens)}")
        logger.info(f"- Output: {args.output_file}")
        logger.info(f"- Token list: {token_file}")
    else:
        # Process all files
        tokenizer.process_directory(args.input_dir, args.output_file)

if __name__ == '__main__':
    main() 