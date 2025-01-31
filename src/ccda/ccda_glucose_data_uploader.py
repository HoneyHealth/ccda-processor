#!/usr/bin/env python3
"""
CCDA Glucose Data Uploader

This script:
1. Reads patient matches from patient_matches.json
2. For each patient with glucose data:
   - Retrieves their records from DynamoDB GlucoseDataRawV3 table
   - Generates a CSV file with their glucose readings
   - Uploads the CSV to specified S3 bucket
3. Handles memory efficiently and provides detailed logging

Features:
- Memory-efficient processing with temporary files
- Batch processing with progress tracking
- Detailed logging with memory usage stats
"""

import json
import os
import csv
import psutil
import boto3
from datetime import datetime, timedelta
from decimal import Decimal
import logging
from typing import Dict, List, Optional
from pathlib import Path
import tempfile
from botocore.exceptions import ClientError
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GlucoseDataUploader:
    def __init__(
        self,
        dynamodb_table_name: str = "GlucoseDataRawV3",
        s3_bucket: str = None,
        time_range_days: int = 365
    ):
        """Initialize the GlucoseDataUploader.
        
        Args:
            dynamodb_table_name: Name of the DynamoDB table containing glucose data
            s3_bucket: Name of the S3 bucket for CSV upload
            time_range_days: Number of days of data to fetch (default: 365)
        """
        # Initialize DynamoDB client in us-east-1 for GlucoseDataRawV3
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        # Initialize S3 client in us-west-2 for data upload
        self.s3 = boto3.client('s3', region_name='us-west-2')
        self.table = self.dynamodb.Table(dynamodb_table_name)
        self.s3_bucket = s3_bucket
        self.time_range_days = time_range_days
        
        # Track processing stats
        self.processed_patients = 0
        self.successful_uploads = 0
        self.failed_uploads = 0
        
        # Verify S3 bucket access
        try:
            self.s3.head_bucket(Bucket=self.s3_bucket)
            logger.info(f"Successfully verified access to S3 bucket: {self.s3_bucket}")
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == '404':
                raise ValueError(f"S3 bucket {self.s3_bucket} does not exist")
            elif error_code == '403':
                raise ValueError(f"No permission to access S3 bucket {self.s3_bucket}")
            else:
                raise ValueError(f"Error accessing S3 bucket {self.s3_bucket}: {str(e)}")
        
        # Log initial memory usage
        self._log_memory_usage("Initial")

    def _log_memory_usage(self, stage: str):
        """Log memory usage statistics."""
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        
        logger.info(
            f"\nMemory Usage ({stage}):\n"
            f"- RSS: {memory_info.rss / 1024 / 1024:.2f} MB\n"
            f"- VMS: {memory_info.vms / 1024 / 1024:.2f} MB"
        )

    def _get_time_range(self) -> tuple[str, str]:
        """Calculate the time range for data extraction.
        
        Returns:
            Tuple of (start_time, end_time) in ISO format
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(days=self.time_range_days)
        return (
            start_time.strftime("%Y-%m-%dT%H:%M:%S"),
            end_time.strftime("%Y-%m-%dT%H:%M:%S")
        )

    def query_patient_data(self, user_id: str) -> List[Dict]:
        """Query glucose data for a specific patient within the time range.
        
        Args:
            user_id: The user's ID to query
            
        Returns:
            List of glucose readings
        """
        start_time, end_time = self._get_time_range()
        
        try:
            response = self.table.query(
                KeyConditionExpression='userId = :uid AND systemTime BETWEEN :start AND :end',
                ExpressionAttributeValues={
                    ':uid': user_id,
                    ':start': start_time,
                    ':end': end_time
                },
                ScanIndexForward=False  # Sort in descending order
            )
            
            # Handle pagination
            items = response['Items']
            while 'LastEvaluatedKey' in response:
                response = self.table.query(
                    KeyConditionExpression='userId = :uid AND systemTime BETWEEN :start AND :end',
                    ExpressionAttributeValues={
                        ':uid': user_id,
                        ':start': start_time,
                        ':end': end_time
                    },
                    ScanIndexForward=False,  # Sort in descending order
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items.extend(response['Items'])
            
            logger.info(f"Retrieved {len(items)} records for user {user_id}")
            return items
        
        except ClientError as e:
            logger.error(f"Error querying DynamoDB for user {user_id}: {str(e)}")
            return []

    def write_csv(self, data: List[Dict], output_path: str):
        """Write glucose data to a CSV file.
        
        Args:
            data: List of glucose readings
            output_path: Path to write the CSV file
        """
        if not data:
            logger.warning(f"No data to write to {output_path}")
            return

        # Ensure the directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Use exact field names from DynamoDB
        fieldnames = [
            'systemTime',
            'dataSource',
            'displayTime',
            'value',
            'transmitterTime',
            'isTimeChange'
        ]
        
        try:
            with open(output_path, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                # Sort data by systemTime
                sorted_data = sorted(data, key=lambda x: x['systemTime'], reverse=True)
                
                for item in sorted_data:
                    # Only include the specified fields, leaving optional ones blank if not present
                    row = {field: item.get(field, '') for field in fieldnames}
                    writer.writerow(row)
            
            logger.info(f"Successfully wrote {len(sorted_data)} records to {output_path}")
        
        except Exception as e:
            logger.error(f"Error writing CSV file {output_path}: {str(e)}")
            raise

    def _get_s3_prefix(self, data: List[Dict]) -> str:
        """Determine the S3 prefix based on the dataSource.
        
        Args:
            data: List of glucose readings
            
        Returns:
            S3 prefix path
        """
        # Get unique data sources from the records
        data_sources = set(item.get('dataSource', '').lower() for item in data if item.get('dataSource'))
        
        if not data_sources:
            logger.warning("No dataSource found in records, using default path")
            return ''
        elif len(data_sources) > 1:
            logger.warning(f"Multiple dataSources found in records: {data_sources}, using the first one")
        
        # Get the first data source (after sorting for consistency)
        data_source = sorted(data_sources)[0]
        
        if data_source == 'clarity':
            return 'device/cgm_dexcom/'
        elif data_source == 'libreview':
            return 'device/cgm_freestyle_libre/'
        else:
            logger.warning(f"Unknown dataSource: {data_source}, using default path")
            return ''

    def upload_to_s3(self, file_path: str, s3_key: str, data: List[Dict]):
        """Upload a file to S3 in the appropriate folder based on dataSource.
        
        Args:
            file_path: Local path to the file
            s3_key: Base S3 object key for the upload
            data: List of glucose readings used to determine the folder
        """
        try:
            # Get the appropriate S3 prefix based on dataSource
            prefix = self._get_s3_prefix(data)
            full_s3_key = f"{prefix}{s3_key}" if prefix else s3_key
            
            self.s3.upload_file(file_path, self.s3_bucket, full_s3_key)
            logger.info(f"Successfully uploaded to s3://{self.s3_bucket}/{full_s3_key}")
            self.successful_uploads += 1
        except ClientError as e:
            logger.error(f"Error uploading to S3: {str(e)}")
            self.failed_uploads += 1
            raise

    def process_patient_matches(self, matches_file: str):
        """Process all patients from the matches file.
        
        Args:
            matches_file: Path to the patient_matches.json file
        """
        try:
            with open(matches_file, 'r') as f:
                matches_data = json.load(f)
        except Exception as e:
            logger.error(f"Error reading matches file: {str(e)}")
            return

        matches = matches_data.get('matches', [])
        total_matches = len(matches)
        logger.info(f"\nStarting processing of {total_matches} patient matches")
        self._log_memory_usage("Before Processing")
        
        # Create a temporary directory for CSV files
        with tempfile.TemporaryDirectory() as temp_dir:
            for match in tqdm(matches, desc="Processing patients"):
                # Skip if patient doesn't have glucose data
                if not match.get('glucose_data', {}).get('has_data', False):
                    continue

                self.processed_patients += 1
                user_id = match['opensearch_match']['patientId']
                source_file = match['ccda_patient']['source_file']
                
                # Generate CSV filename from source XML filename
                csv_filename = Path(source_file).stem + '.csv'
                temp_csv_path = os.path.join(temp_dir, csv_filename)
                
                try:
                    # Get patient's glucose data
                    glucose_data = self.query_patient_data(user_id)
                    
                    # Write to CSV
                    self.write_csv(glucose_data, temp_csv_path)
                    
                    # Upload to S3 with appropriate prefix
                    if os.path.exists(temp_csv_path):
                        self.upload_to_s3(temp_csv_path, csv_filename, glucose_data)
                    
                    # Log memory usage every 100 patients
                    if self.processed_patients % 100 == 0:
                        self._log_memory_usage(f"After {self.processed_patients} patients")
                    
                except Exception as e:
                    logger.error(f"Error processing patient {user_id}: {str(e)}")
                    continue

        # Log final statistics
        self._log_memory_usage("After Processing")
        logger.info(f"\nProcessing complete:")
        logger.info(f"- Total patients processed: {self.processed_patients}")
        logger.info(f"- Successful uploads: {self.successful_uploads}")
        logger.info(f"- Failed uploads: {self.failed_uploads}")

def main():
    """Main entry point for the script."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Upload glucose data to S3 as CSV files')
    parser.add_argument('--matches-file', required=True,
                      help='Path to patient_matches.json file')
    parser.add_argument('--s3-bucket', required=True,
                      help='S3 bucket name for CSV upload')
    parser.add_argument('--time-range', type=int, default=365,
                      help='Number of days of data to fetch (default: 365)')
    parser.add_argument('--debug', action='store_true',
                      help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    uploader = GlucoseDataUploader(
        s3_bucket=args.s3_bucket,
        time_range_days=args.time_range
    )
    
    uploader.process_patient_matches(args.matches_file)

if __name__ == '__main__':
    main() 