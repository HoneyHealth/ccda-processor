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

    def _get_time_range(self, latest_record_time: str) -> tuple[str, str]:
        """Calculate the time range for data extraction based on latest record time.
        
        Args:
            latest_record_time: The patient's latest glucose record timestamp
            
        Returns:
            Tuple of (start_time, end_time) in ISO format
        """
        # Parse the latest record time as the end time
        end_time = datetime.fromisoformat(latest_record_time.replace('Z', '+00:00'))
        # Calculate start time by going back time_range_days from the latest record
        start_time = end_time - timedelta(days=self.time_range_days)
        return (
            start_time.strftime("%Y-%m-%dT%H:%M:%S"),
            end_time.strftime("%Y-%m-%dT%H:%M:%S")
        )

    def query_patient_data(self, user_id: str, latest_record_time: str) -> List[Dict]:
        """Query glucose data for a specific patient within the time range.
        
        Args:
            user_id: The user's ID to query
            latest_record_time: The patient's latest glucose record timestamp
            
        Returns:
            List of glucose readings
        """
        start_time, end_time = self._get_time_range(latest_record_time)
        
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
            
            logger.info(f"Retrieved {len(items)} records for user {user_id} from {start_time} to {end_time}")
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
            with open(matches_file) as f:
                data = json.load(f)
            
            # Extract matches list and metadata
            matches = data.get('matches', [])
            total_matches = len(matches)
            
            # Filter patients with glucose data upfront
            glucose_patients = [m for m in matches if m.get('glucose_data', {}).get('has_data', False)]
            patients_with_glucose = len(glucose_patients)
            
            if patients_with_glucose == 0:
                logger.warning("No patients found with glucose data")
                return
            
            logger.info(f"\nProcessing Status:")
            logger.info(f"- Total matches found: {total_matches}")
            logger.info(f"- Patients with glucose data: {patients_with_glucose}")
            logger.info(f"- Time range: {self.time_range_days} days\n")
            
            # Create a temporary directory for CSV files
            with tempfile.TemporaryDirectory() as temp_dir:
                # Initialize progress bar
                progress = tqdm(
                    glucose_patients,
                    desc="Processing glucose data",
                    unit="patient",
                    total=patients_with_glucose
                )
                
                for match_data in progress:
                    self.processed_patients += 1
                    
                    # Extract patient information
                    ccda_patient = match_data.get('ccda_patient', {})
                    glucose_data = match_data.get('glucose_data', {})
                    opensearch_match = match_data.get('opensearch_match', {})
                    
                    # Get patient ID and name
                    source_file = ccda_patient.get('source_file', '')
                    patient_id = source_file.split('/')[-1].split('.')[0] if source_file else None
                    patient_name = f"{ccda_patient.get('firstName', '')} {ccda_patient.get('lastName', '')}".strip()
                    
                    # Update progress description
                    progress.set_description(f"Processing {patient_name or patient_id or 'Unknown'}")
                    
                    if not patient_id:
                        progress.write(f"⚠️  Skipping patient: No source file found")
                        continue
                    
                    try:
                        # Verify we have all required data
                        latest_record_time = glucose_data.get('latest_record_time')
                        if not latest_record_time:
                            progress.write(f"⚠️  Skipping {patient_id}: No latest record time")
                            continue
                            
                        user_id = opensearch_match.get('patientId')
                        if not user_id:
                            progress.write(f"⚠️  Skipping {patient_id}: No OpenSearch patient ID")
                            continue
                        
                        # Query glucose data using latest record time
                        glucose_readings = self.query_patient_data(user_id, latest_record_time)
                        
                        if not glucose_readings:
                            progress.write(f"⚠️  No glucose readings found for {patient_id}")
                            continue
                        
                        # Update progress with record count
                        actual_count = len(glucose_readings)
                        progress.set_postfix({
                            'records': actual_count
                        })
                        
                        # Generate CSV file
                        csv_path = os.path.join(temp_dir, f"{patient_id}_glucose.csv")
                        self.write_csv(glucose_readings, csv_path)
                        
                        # Upload to S3
                        s3_key = f"{patient_id}_glucose.csv"
                        self.upload_to_s3(csv_path, s3_key, glucose_readings)
                        
                        # Update progress with success
                        progress.write(f"✅ {patient_id}: Successfully processed {actual_count} records")
                        
                    except Exception as e:
                        progress.write(f"❌ Error processing {patient_id}: {str(e)}")
                        continue
                
                progress.close()
            
            # Log final stats with success rate
            success_rate = (self.successful_uploads / patients_with_glucose * 100) if patients_with_glucose > 0 else 0
            logger.info("\nProcessing Summary:")
            logger.info(f"- Patients with glucose data: {patients_with_glucose}")
            logger.info(f"- Successfully processed: {self.successful_uploads}")
            logger.info(f"- Failed to process: {self.failed_uploads}")
            logger.info(f"- Success rate: {success_rate:.1f}%")
            self._log_memory_usage("Final")
            
        except Exception as e:
            logger.error(f"Error processing matches file: {str(e)}")
            raise

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