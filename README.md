# CCDA Processor

A comprehensive toolkit for processing, analyzing, and reformatting Clinical Document Architecture (CCDA) XML files.

## Features

- **CCDA Analysis**: Analyze CCDA files for information richness and content structure
- **XML Reformatting**: Improve readability of CCDA files while preserving content
- **Section Analysis**: Detailed analysis of CCDA sections and their contents
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

3. Copy CCDA files to process into `input/ccda/`

## End-to-End Workflow

### Step 1: Analyze CCDA Sections
First, scan all CCDA files to identify their sections and build a comprehensive section index:

```bash
python src/ccda/ccda_section_analyzer.py \
    --input-dir input/ccda \
    --output-file output/analysis/metrics/section_analysis.json \
    --batch-size 15 \
    --debug
```

This will:
- Scan all XML files in the input directory
- Identify all unique sections and their templateIds
- Calculate section frequencies and statistics
- Generate a detailed section analysis report

### Step 2: Generate Configuration
Based on the section analysis, generate a configuration file that defines section weights and scoring criteria:

```bash
python src/ccda/ccda_config_generator.py \
    --section-analysis output/analysis/metrics/section_analysis.json \
    --output-file output/analysis/metrics/ccda_config.json \
    --min-frequency 0.1
```

This will:
- Analyze section frequencies and importance
- Generate appropriate weights for scoring
- Create a configuration file for the analyzer

### Step 3: Analyze Information Richness
Process all CCDA files to identify the most information-rich documents:

```bash
python src/ccda/ccda_information_analyzer.py \
    --input-dir input/ccda \
    --output-file output/analysis/metrics/analysis.json \
    --batch-size 15 \
    --memory-limit 8000 \
    --debug
```

This will:
- Score each file based on:
  - Number of entries per section
  - Presence of coded elements
  - Text content richness
  - Section completeness
- Generate a ranked list of files by information score

### Step 4: Reformat Selected Files
Reformat the most information-rich files for better readability:

```bash
python src/ccda/ccda_xml_reformatter.py \
    --analysis-file output/analysis/metrics/analysis.json \
    --top-n 1500 \
    --output-dir output/reformatted \
    --batch-size 10 \
    --memory-limit 6000 \
    --debug
```

This will:
- Select top N files based on information score
- Apply proper XML indentation
- Preserve all original content
- Process files in memory-efficient batches

### Step 5: Verify Content Preservation
Verify that the reformatting process preserved all content:

```bash
python src/ccda/ccda_content_verifier.py
```

This will:
- Randomly select 20 files
- Compare original and reformatted versions
- Verify no content was lost or modified
- Generate a verification report

## Script Details

### CCDA Section Analyzer
```bash
python src/ccda/ccda_section_analyzer.py --help
```
Options:
- `--input-dir`: Directory containing CCDA files
- `--output-file`: Output JSON file for section analysis
- `--batch-size`: Number of files to process in each batch
- `--memory-limit`: Memory limit in MB
- `--debug`: Enable debug logging

### CCDA Configuration Generator
```bash
python src/ccda/ccda_config_generator.py --help
```
Options:
- `--section-analysis`: Input section analysis JSON file
- `--output-file`: Output configuration JSON file
- `--min-frequency`: Minimum section frequency threshold

### CCDA Information Analyzer
```bash
python src/ccda/ccda_information_analyzer.py --help
```
Options:
- `--input-dir`: Directory containing CCDA files
- `--output-file`: Output analysis JSON file
- `--batch-size`: Number of files to process in each batch
- `--memory-limit`: Memory limit in MB
- `--debug`: Enable debug logging

### CCDA XML Reformatter
```bash
python src/ccda/ccda_xml_reformatter.py --help
```
Options:
- `--analysis-file`: Analysis results JSON file
- `--top-n`: Number of top files to process
- `--output-dir`: Output directory for reformatted files
- `--batch-size`: Number of files to process in each batch
- `--memory-limit`: Memory limit in MB
- `--debug`: Enable debug logging

### CCDA Content Verifier
```bash
python src/ccda/ccda_content_verifier.py --help
```
Options:
- Uses default paths:
  - Original files: `input/ccda/`
  - Reformatted files: `output/reformatted/`

## Output Structure

- `output/analysis/metrics/`: Contains analysis results
  - `section_analysis.json`: Detailed section analysis
  - `ccda_config.json`: Generated configuration
  - `analysis.json`: Information richness analysis
- `output/analysis/reports/`: Generated reports and summaries
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