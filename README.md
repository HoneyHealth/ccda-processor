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
    --checkpoint-dir output/temp/analysis_checkpoints \
    --batch-size 100 \
    --memory-limit 8000 \
    --debug
```

This will:
- Score each file based on:
  - Number of entries per section (weight: 0.5 per entry)
  - Presence of coded elements (weight: 0.3 per code)
  - Text content richness (weight: 0.1 per word)
  - Section completeness
- Generate a ranked list of files by information score
- Create checkpoints in `output/temp/analysis_checkpoints` for recovery
- Merge results into a final analysis file

### Step 4: Reformat Selected Files
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

### Step 5: Verify Content Preservation
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