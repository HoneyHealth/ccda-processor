# CCDA Processor

A comprehensive toolkit for processing, analyzing, and reformatting Clinical Document Architecture (CCDA) XML files.

## Features

- **CCDA Analysis**: Analyze CCDA files for information richness and content structure
- **XML Reformatting**: Improve readability of CCDA files while preserving content
- **Section Analysis**: Detailed analysis of CCDA sections and their contents
- **Patient Matching**: Match CCDA patients with records in OpenSearch and check their glucose data
- **Memory Efficient**: Batch processing with memory monitoring
- **Content Verification**: Tools to verify content preservation during processing

## Setup

1. Create a Python virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Create required directories:
   ```bash
   mkdir -p input/ccda output/analysis/metrics output/reformatted output/temp
   ```

4. Copy CCDA files to process into `input/ccda/`

## End-to-End Workflow

### Step 1: Analyze CCDA Sections
First, scan all CCDA files to identify their sections and build a comprehensive section index:

```bash
python src/ccda/ccda_section_analyzer.py \
    --input-dir input/ccda/* \
    --output-file output/analysis/metrics/section_analysis.json \
    --batch-size 15 \
    --memory-limit 8000 \
    --debug
```

This will:
- Scan all XML files in the input directory
- Identify all unique sections and their templateIds
- Calculate section frequencies and statistics
- Generate a detailed section analysis report

### Step 2: Generate Configuration
Generate a configuration file that defines section weights and scoring criteria:

```bash
python src/ccda/ccda_config_generator.py \
    --analysis-file output/analysis/metrics/section_analysis.json \
    --output-file output/analysis/metrics/ccda_config.json
```

This will:
- Analyze section frequencies and importance
- Generate appropriate weights for scoring
- Create a configuration file for the analyzer

### Step 3: Analyze Information Richness
Process all CCDA files to identify the most information-rich documents:

```bash
python src/ccda/ccda_information_analyzer.py \
    --input-dir input/ccda/* \
    --output-file output/analysis/metrics/analysis.json \
    --config-file output/analysis/metrics/ccda_config.json \
    --checkpoint-dir output/temp/analysis_checkpoints \
    --batch-size 100 \
    --memory-limit 8000 \
    --debug
```

This will:
- Score each file based on configuration weights and:
  - Section importance for ML/LLM training
  - Rich narrative content (weighted by section)
  - Presence of coded elements
  - Number of entries
  - Combined narrative and structured data bonus
- Use section weights from the configuration file for:
  - Clinical notes and assessments
  - Professional observations
  - Procedure details
  - Diagnostic information
- Generate a ranked list of files by information score
- Create checkpoints in `output/temp/analysis_checkpoints` for recovery
- Merge results into a final analysis file

Arguments:
- `--input-dir`: Directory containing CCDA XML files
- `--output-file`: Path for the analysis results JSON file
- `--config-file`: Path to the configuration file with section weights
- `--checkpoint-dir`: Directory for storing analysis checkpoints
- `--batch-size`: Number of files to process in each batch
- `--memory-limit`: Memory limit in MB for processing
- `--debug`: Enable debug logging

The scoring system prioritizes:
- Sections with high value for ML/LLM training
- Rich narrative content from healthcare professionals
- Clinical reasoning and decision-making
- Detailed medical assessments and observations
- Combined structured and unstructured data

### Step 4: Match Patients with Records
Match patients from the most information-rich CCDA files with OpenSearch records and check their glucose data:

```bash
python src/ccda/ccda_patient_matcher.py \
    --analysis-file output/analysis/metrics/analysis.json \
    --top-n 100 \
    --output-file output/analysis/metrics/patient_matches.json \
    --opensearch-endpoint your-opensearch-endpoint \
    --region us-east-1 \
    --debug
```

This will:
- Process the top N most information-rich CCDA files
- Extract patient demographics (first name, last name, DOB)
- Search for matching patients in OpenSearch using:
  - Exact match on DOB
  - Fuzzy matching on names
- Check DynamoDB for glucose data records
- Generate a detailed matching report including:
  - Match statistics
  - Patient information from both sources
  - Latest glucose data timestamps

### Step 5: Upload Original EHR CCDA Files to S3
Upload the original CCDA XML files for the top N most information-rich patients to S3:

```bash
python src/ccda/ccda_ehr_data_uploader.py \
    --analysis-file output/analysis/metrics/analysis.json \
    --s3-bucket hh-protege-sample-bucket-1 \
    --top-n 100 \
    --s3-folder ehr/ \
    --debug
```

This will:
- Read the analysis results from analysis.json
- Sort patients by their information richness score
- Take the top N most information-rich patients
- Upload their original CCDA XML files to S3:
  - Files are stored in the specified S3 folder (default: ehr/)
  - Original filenames are preserved
- Track and report:
  - Memory usage
  - Processing progress
  - Success/failure statistics

Arguments:
- `--analysis-file`: Path to the analysis.json file containing information richness scores
- `--s3-bucket`: Name of the S3 bucket for XML upload
- `--top-n`: Number of top patients to process (must be positive)
- `--s3-folder`: S3 folder for uploads (default: ehr/)
- `--debug`: Enable debug logging

Features:
- Memory-efficient processing
- Progress tracking with tqdm
- Detailed logging with memory usage stats
- Robust error handling
- Configurable S3 folder structure

The script uses:
- S3 in us-west-2 for XML storage
- Configurable S3 folder for organization
- Input validation for all parameters
- Automatic error recovery and continuation

### Step 6: Upload Glucose Data to S3
For patients with glucose data matches, export their readings to CSV files and upload to S3:

```bash
python src/ccda/ccda_glucose_data_uploader.py \
    --matches-file output/analysis/metrics/patient_matches.json \
    --s3-bucket hh-protege-sample-bucket-1 \
    --time-range 365 \
    --debug
```

This will:
- Process all matched patients with glucose data
- For each patient:
  - Query their glucose readings from DynamoDB (GlucoseDataRawV3 table)
  - Create a CSV file containing:
    - systemTime
    - dataSource
    - displayTime
    - value
    - transmitterTime
    - isTimeChange
  - Upload the CSV to S3 in device-specific folders:
    - Dexcom data: `device/cgm_dexcom/`
    - Freestyle Libre data: `device/cgm_freestyle_libre/`
- Track and report:
  - Memory usage
  - Processing progress
  - Success/failure statistics

The script uses:
- DynamoDB in us-east-1 for glucose data
- S3 in us-west-2 for CSV storage
- Temporary local storage for CSV generation

### Optional Step: Reformat Selected Files for Sanity Check
Reformat the most information-rich files for better readability:

```bash
python src/ccda/ccda_xml_reformatter.py \
    --output-dir output/reformatted \
    --analysis-file output/analysis/metrics/analysis.json \
    --top-n 1500 \
    --batch-size 100 \
    --memory-limit 8000 \
    --debug
```

This will:
- Select top N files based on information score
- Apply proper XML indentation
- Preserve all original content
- Process files in memory-efficient batches

### Optional Step: Verify Content Preservation
Verify that the reformatting process preserved all content:

```bash
python src/ccda/ccda_content_verifier.py \
    --original-dir input/ccda/* \
    --reformatted-dir output/reformatted \
    --sample-size 200 \
    --debug
```

This will:
- Randomly select files for verification
- Compare original and reformatted versions
- Verify no content was lost or modified
- Generate a verification report

## Output Structure

- `output/analysis/metrics/`: Contains analysis results
  - `section_analysis.json`: Detailed section analysis
  - `analysis.json`: Information richness analysis
  - `patient_matches.json`: Patient matching results
- `output/analysis/checkpoints/`: Temporary checkpoints during analysis
- `output/reformatted/`: Reformatted CCDA XML files
- `output/temp/`: Temporary files (cleared between runs)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 