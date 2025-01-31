#!/usr/bin/env python3
"""
CCDA Patient Matcher

This script:
1. Reads the top N most information-rich CCDA files from analysis results
2. Extracts patient demographics (first name, last name, DOB)
3. Queries AWS OpenSearch to find matching patient records
4. Generates a report of matches found

Features:
- Memory-efficient processing
- Batch processing with progress tracking
- Detailed match reporting
"""

import os
import sys
import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from lxml import etree
from tqdm import tqdm
import argparse
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

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

class CCDAPatientMatcher:
    """Matches CCDA patients with OpenSearch records."""
    
    def __init__(self, opensearch_endpoint: str, region: str = 'us-east-1'):
        """Initialize with OpenSearch connection."""
        # Remove http(s):// and :443 if present in the endpoint
        opensearch_endpoint = opensearch_endpoint.replace('https://', '').replace('http://', '').replace(':443', '')
        
        try:
            # Initialize OpenSearch client with basic auth
            self.os_client = OpenSearch(
                hosts=[{'host': opensearch_endpoint, 'port': 443}],
                http_auth=('ostest', 'qweasdZXC!23'),  # Basic auth credentials
                use_ssl=True,
                verify_certs=True,
                connection_class=RequestsHttpConnection,
                timeout=30
            )
            
            # Initialize DynamoDB client
            self.dynamodb = boto3.resource('dynamodb', region_name=region)
            self.glucose_table = self.dynamodb.Table('GlucoseDataRawV3')
            
            # Test OpenSearch connection
            try:
                self.os_client.info()
                logger.info("Successfully connected to OpenSearch")
                
                # Test DynamoDB connection
                self.glucose_table.table_status
                logger.info("Successfully connected to DynamoDB")
            except Exception as e:
                raise ConnectionError(
                    f"Failed to connect to services:\n"
                    f"Please verify:\n"
                    f"1. The OpenSearch endpoint is correct\n"
                    f"2. The username and password are correct\n"
                    f"3. Your network allows access to AWS services\n"
                    f"Error: {str(e)}"
                ) from e
                
        except Exception as e:
            raise ConnectionError(f"Failed to initialize clients: {str(e)}") from e
            
        self.matches = []
        self.processed_files = 0
        
    def extract_patient_info(self, xml_file: str) -> Optional[Dict]:
        """Extract patient demographics from CCDA file."""
        try:
            parser = etree.XMLParser(remove_blank_text=True)
            tree = etree.parse(xml_file, parser)
            root = tree.getroot()
            
            # Find recordTarget/patientRole/patient
            patient = root.find('.//h:recordTarget/h:patientRole/h:patient', namespaces=CCDA_NS)
            if patient is None:
                logger.warning(f"No patient information found in {xml_file}")
                return None
            
            # Extract name components
            name = patient.find('.//h:name', namespaces=CCDA_NS)
            if name is None:
                logger.warning(f"No patient name found in {xml_file}")
                return None
                
            given = name.findtext('.//h:given', namespaces=CCDA_NS)
            family = name.findtext('.//h:family', namespaces=CCDA_NS)
            
            # Extract birth date
            birth_time = patient.find('.//h:birthTime', namespaces=CCDA_NS)
            if birth_time is None or 'value' not in birth_time.attrib:
                logger.warning(f"No birth date found in {xml_file}")
                return None
                
            # Convert CCDA date format (YYYYMMDD) to ISO format (YYYY-MM-DD)
            dob_str = birth_time.get('value')
            dob = f"{dob_str[:4]}-{dob_str[4:6]}-{dob_str[6:8]}"
            
            return {
                'firstName': given,
                'lastName': family,
                'dob': dob,
                'source_file': xml_file
            }
            
        except Exception as e:
            logger.error(f"Error processing {xml_file}: {str(e)}")
            return None
            
    def get_latest_glucose_data(self, user_id: str) -> Optional[Dict]:
        """Get the latest glucose data for a patient from DynamoDB."""
        try:
            # Query the base table for the user's records
            response = self.glucose_table.query(
                KeyConditionExpression='userId = :uid',
                ExpressionAttributeValues={
                    ':uid': user_id
                },
                ProjectionExpression='userId, systemTime',  # Only retrieve needed attributes
                ScanIndexForward=False,  # Sort in descending order
                Limit=100  # Get latest 100 records
            )
            
            items = response.get('Items', [])
            if items:
                # Sort by systemTime in descending order (just in case)
                sorted_items = sorted(items, key=lambda x: x.get('systemTime', ''), reverse=True)
                latest_record = sorted_items[0]  # First item is the most recent
                
                return {
                    'has_data': True,
                    'latest_record_time': latest_record.get('systemTime'),
                    'record_count': len(items)
                }
            
            return {
                'has_data': False,
                'latest_record_time': None,
                'record_count': 0
            }
            
        except Exception as e:
            logger.error(f"Error querying DynamoDB for user {user_id}: {str(e)}")
            return None
            
    def search_patient(self, patient_info: Dict) -> Optional[Dict]:
        """Search for patient in OpenSearch and check DynamoDB for glucose data."""
        try:
            response = self.os_client.search(
                index='patients',
                body={
                    'query': {
                        'bool': {
                            'must': [
                                {
                                    'term': {
                                        'dob': patient_info['dob']
                                    }
                                }
                            ],
                            'should': [
                                # Fuzzy match on first name
                                {
                                    'match': {
                                        'firstName': {
                                            'query': patient_info['firstName'],
                                            'fuzziness': 'AUTO'
                                        }
                                    }
                                },
                                # Fuzzy match on full last name
                                {
                                    'match': {
                                        'lastName': {
                                            'query': patient_info['lastName'],
                                            'fuzziness': 'AUTO'
                                        }
                                    }
                                },
                                # Additional match for compound last names
                                {
                                    'match_phrase': {
                                        'lastName': {
                                            'query': patient_info['lastName'],
                                            'slop': 1
                                        }
                                    }
                                }
                            ],
                            'minimum_should_match': 1
                        }
                    }
                }
            )
            
            hits = response.get('hits', {}).get('hits', [])
            if hits:
                match = hits[0]['_source']
                patient_id = match.get('patientId')
                
                # Get glucose data if patient ID exists
                glucose_data = None
                if patient_id:
                    glucose_data = self.get_latest_glucose_data(patient_id)
                
                return {
                    'ccda_patient': {
                        'firstName': patient_info['firstName'],
                        'lastName': patient_info['lastName'],
                        'dob': patient_info['dob'],
                        'source_file': patient_info['source_file']
                    },
                    'opensearch_match': {
                        'firstName': match.get('firstName'),
                        'lastName': match.get('lastName'),
                        'dob': match.get('dob'),
                        'patientId': patient_id,
                        'score': hits[0]['_score']
                    },
                    'glucose_data': glucose_data if glucose_data else {
                        'has_data': False,
                        'latest_record_time': None,
                        'record_count': 0
                    }
                }
                
            return None
            
        except Exception as e:
            logger.error(f"OpenSearch query failed: {str(e)}")
            return None
            
    def process_files(self, analysis_file: str, top_n: int, output_file: str):
        """Process top N files from analysis results."""
        # Load analysis results
        with open(analysis_file) as f:
            analysis = json.load(f)
            
        # Sort files by information score and get top N
        sorted_files = sorted(
            analysis.items(),
            key=lambda x: x[1].get('total_score', 0),
            reverse=True
        )[:top_n]
        
        logger.info(f"Processing top {len(sorted_files)} files...")
        
        # Process each file
        for file_path, _ in tqdm(sorted_files):
            self.processed_files += 1
            
            # Extract patient info
            patient_info = self.extract_patient_info(file_path)
            if not patient_info:
                continue
                
            # Search for match
            match = self.search_patient(patient_info)
            if match:
                self.matches.append(match)
                
        # Generate report
        self.generate_report(output_file)
        
    def generate_report(self, output_file: str):
        """Generate a JSON report of matches found."""
        report = {
            'summary': {
                'total_files_processed': self.processed_files,
                'total_matches_found': len(self.matches),
                'match_rate': len(self.matches) / self.processed_files if self.processed_files > 0 else 0,
                'patients_with_glucose_data': sum(1 for m in self.matches if m.get('glucose_data', {}).get('has_data', False))
            },
            'matches': self.matches
        }
        
        # Save report
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)
            
        logger.info(f"\nMatching complete:")
        logger.info(f"- Files processed: {self.processed_files}")
        logger.info(f"- Matches found: {len(self.matches)}")
        logger.info(f"- Match rate: {report['summary']['match_rate']:.1%}")
        logger.info(f"- Patients with glucose data: {report['summary']['patients_with_glucose_data']}")
        logger.info(f"- Report saved to: {output_file}")
        
        # Log a sample match if available
        if self.matches:
            sample = self.matches[0]
            logger.info("\nSample match:")
            logger.info("CCDA Patient:")
            logger.info(f"  Name: {sample['ccda_patient']['firstName']} {sample['ccda_patient']['lastName']}")
            logger.info(f"  DOB: {sample['ccda_patient']['dob']}")
            logger.info("OpenSearch Match:")
            logger.info(f"  Name: {sample['opensearch_match']['firstName']} {sample['opensearch_match']['lastName']}")
            logger.info(f"  DOB: {sample['opensearch_match']['dob']}")
            logger.info(f"  Score: {sample['opensearch_match']['score']:.2f}")
            if sample.get('glucose_data', {}).get('has_data'):
                logger.info("Glucose Data:")
                logger.info(f"  Latest Record: {sample['glucose_data']['latest_record_time']}")
                logger.info(f"  Recent Records: {sample['glucose_data']['record_count']}")

def main():
    parser = argparse.ArgumentParser(
        description='Match CCDA patients with OpenSearch records'
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
        '--output-file',
        default='output/analysis/metrics/patient_matches.json',
        help='Output JSON file for match results'
    )
    parser.add_argument(
        '--opensearch-endpoint',
        required=True,
        help='AWS OpenSearch endpoint'
    )
    parser.add_argument(
        '--region',
        default='us-east-1',
        help='AWS region'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    args = parser.parse_args()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    matcher = CCDAPatientMatcher(args.opensearch_endpoint, args.region)
    matcher.process_files(args.analysis_file, args.top_n, args.output_file)

if __name__ == '__main__':
    main() 