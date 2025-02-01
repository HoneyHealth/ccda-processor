#!/usr/bin/env python3
"""
CCDA EHR Data Uploader

This script:
1. Reads analysis results from analysis.json
2. Extracts top N most information-rich patient records
3. Uploads their original CCDA XML files to specified S3 bucket
4. Provides detailed logging and progress tracking

Features:
- Memory-efficient processing
- Progress tracking with tqdm
- Detailed logging with memory usage stats
"""

import json
import os
import psutil
import boto3
import logging
from pathlib import Path
from typing import Dict, List, Optional
from botocore.exceptions import ClientError
from tqdm import tqdm
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EHRDataUploader:
    def __init__(
        self,
        s3_bucket: str,
        top_n: int,
        s3_folder: str = "ehr/"
    ):
        """Initialize the EHR Data Uploader.
        
        Args:
            s3_bucket: Name of the S3 bucket for XML upload
            top_n: Number of top patients to process
            s3_folder: S3 folder for uploads (default: "ehr/")
            
        Raises:
            ValueError: If top_n is not a positive integer or s3_bucket is empty
        """
        if not isinstance(top_n, int) or top_n <= 0:
            raise ValueError("top_n must be a positive integer")
        
        if not s3_bucket:
            raise ValueError("s3_bucket cannot be empty")
            
        # Initialize S3 client in us-west-2
        self.s3 = boto3.client('s3', region_name='us-west-2')
        self.s3_bucket = s3_bucket
        
        # Handle s3_folder more robustly
        if not s3_folder:
            self.s3_folder = ""  # Root of bucket
        else:
            # Remove leading/trailing slashes and add single trailing slash
            self.s3_folder = s3_folder.strip("/")
            if self.s3_folder:
                self.s3_folder += "/"
        
        self.top_n = top_n
        
        # Track processing stats
        self.processed_files = 0
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

    def upload_to_s3(self, file_path: str, s3_key: str) -> bool:
        """Upload a file to S3.
        
        Args:
            file_path: Local path to the file
            s3_key: S3 object key for the upload
            
        Returns:
            bool: True if upload was successful, False otherwise
        """
        # Ensure the file exists
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            self.failed_uploads += 1
            return False
        
        try:
            # Add folder to s3_key
            full_s3_key = f"{self.s3_folder}{s3_key}"
            
            self.s3.upload_file(file_path, self.s3_bucket, full_s3_key)
            logger.info(f"Successfully uploaded to s3://{self.s3_bucket}/{full_s3_key}")
            self.successful_uploads += 1
            return True
            
        except ClientError as e:
            logger.error(f"Error uploading {file_path} to S3: {str(e)}")
            self.failed_uploads += 1
            return False
        except Exception as e:
            logger.error(f"Unexpected error uploading {file_path} to S3: {str(e)}")
            self.failed_uploads += 1
            return False

    def process_analysis_file(self, analysis_file: str):
        """Process the analysis file and upload top N patient XML files.
        
        Args:
            analysis_file: Path to analysis.json file
            
        Raises:
            ValueError: If analysis_file doesn't exist or is invalid JSON
        """
        if not os.path.exists(analysis_file):
            raise ValueError(f"Analysis file not found: {analysis_file}")
            
        try:
            with open(analysis_file, 'r') as f:
                analysis_data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in analysis file: {str(e)}")
        except Exception as e:
            raise ValueError(f"Error reading analysis file: {str(e)}")

        # Sort files by total_score in descending order
        sorted_files = sorted(
            analysis_data.items(),
            key=lambda x: x[1].get('total_score', 0),
            reverse=True
        )

        # Take top N files
        top_files = sorted_files[:self.top_n]
        total_files = len(sorted_files)
        
        if not top_files:
            logger.warning("No files to process")
            return
            
        logger.info(f"\nStarting processing of top {self.top_n} files from {total_files} total files")
        self._log_memory_usage("Before Processing")
        
        for file_path, file_info in tqdm(top_files, desc="Processing files"):
            self.processed_files += 1
            
            # Generate S3 key from file path
            s3_key = Path(file_path).name
            
            # Upload to S3 (no need to catch exceptions here as they're handled in upload_to_s3)
            self.upload_to_s3(file_path, s3_key)
            
            # Log memory usage every 100 files
            if self.processed_files % 100 == 0:
                self._log_memory_usage(f"After {self.processed_files} files")

        # Log final statistics
        self._log_memory_usage("After Processing")
        logger.info(f"\nProcessing complete:")
        logger.info(f"- Total files processed: {self.processed_files}")
        logger.info(f"- Successful uploads: {self.successful_uploads}")
        logger.info(f"- Failed uploads: {self.failed_uploads}")

def main():
    """Main entry point for the script."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Upload EHR XML files for top N patients to S3')
    parser.add_argument('--analysis-file', required=True,
                      help='Path to analysis.json file')
    parser.add_argument('--s3-bucket', required=True,
                      help='S3 bucket name for XML upload')
    parser.add_argument('--top-n', type=int, required=True,
                      help='Number of top patients to process (must be positive)')
    parser.add_argument('--s3-folder', default='ehr/',
                      help='S3 folder for uploads (default: ehr/)')
    parser.add_argument('--debug', action='store_true',
                      help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    try:
        uploader = EHRDataUploader(
            s3_bucket=args.s3_bucket,
            top_n=args.top_n,
            s3_folder=args.s3_folder
        )
        
        uploader.process_analysis_file(args.analysis_file)
        
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main() 