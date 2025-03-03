# CCDA PHI Extraction and Tokenization Tools

This directory contains tools for extracting Protected Health Information (PHI) from CCDA (Consolidated Clinical Document Architecture) XML files and preparing the extracted data for tokenization.

## Overview

- **`ccda_phi_extractor.py`**: Extracts all PHI elements from CCDA XML files into a structured format
- **`ccda_phi_tokenizer.py`**: Processes the extracted PHI data to prepare it for tokenization

These tools are designed to work with large datasets of CCDA files while being memory-efficient and maintaining complete PHI extraction.

## Requirements

- Python 3.6+
- Required packages:
  - lxml
  - tqdm
  - argparse
  - pathlib

Install dependencies:

```bash
pip install lxml tqdm
```

## Usage

### PHI Extraction

The PHI extractor identifies and extracts all patient identifiers and demographic information from CCDA XML files.

#### Basic Usage

```bash
python ccda_phi_extractor.py --input-dir /path/to/ccda/files --output-file output/phi/phi_data.json
```

#### Process a Single File

```bash
python ccda_phi_extractor.py --file /path/to/ccda/file.xml --output-file output/phi/single_file_phi.json
```

#### Enable Debug Logging

```bash
python ccda_phi_extractor.py --input-dir /path/to/ccda/files --output-file output/phi/phi_data.json --debug
```

#### Example Output

The PHI extractor produces a JSON file containing structured PHI data:

```json
[
  {
    "file_name": "patient-123456.xml",
    "phi_data": {
      "ids": [
        {
          "type": "MRN",
          "root": "2.16.840.1.113883.4.1",
          "extension": "123456",
          "xpath": "*/*[15]/*/*[1]"
        }
      ],
      "addresses": [
        {
          "use": "HP",
          "street_lines": ["123 Main Street", "Apt 4B"],
          "city": "New York",
          "state": "NY",
          "postal_code": "10001",
          "country": "US",
          "xpath": "*/*[15]/*/*[3]",
          "formatted": "123 Main Street, Apt 4B, New York, NY, 10001, US"
        }
      ],
      "telecoms": [
        {
          "type": "phone",
          "value": "555-123-4567",
          "use": "HP",
          "xpath": "*/*[15]/*/*[4]"
        }
      ],
      "names": [
        {
          "use": "",
          "prefix": ["Mr."],
          "given": ["John", "A"],
          "family": ["Smith"],
          "suffix": [],
          "xpath": "*[15]/*/*[5]/*[1]",
          "formatted": "Mr. John A Smith"
        }
      ],
      "gender": {
        "type": "gender",
        "code": "M",
        "display_name": "Male",
        "code_system": "2.16.840.1.113883.5.1",
        "code_system_name": "AdministrativeGender",
        "xpath": "*[15]/*/*[5]/*[2]"
      },
      "birthtime": {
        "value": "19800101",
        "xpath": "*[15]/*/*[5]/*[3]"
      },
      "marital_status": {},
      "race": {},
      "ethnicity": {},
      "language": {},
      "guardian": {},
      "provider_organization": {}
    }
  }
]
```

### PHI Tokenization

The PHI tokenizer prepares the extracted PHI data for tokenization by normalizing fields and generating token lists.

#### Basic Usage

```bash
python ccda_phi_tokenizer.py --input-dir /path/to/ccda/files --output-file output/phi/tokenization_data.json
```

#### Process a Sample of Files

```bash
python ccda_phi_tokenizer.py --sample-size 100 --output-file output/phi/sample_tokenization_data.json
```

#### Example Output

The PHI tokenizer produces a JSON file containing tokenization-ready data and a separate file with unique tokens:

**tokenization_data.json:**
```json
[
  {
    "file_name": "patient-123456.xml",
    "tokenization_data": {
      "names": [
        {
          "name": "Mr. John A Smith",
          "name_prefix": "Mr.",
          "name_given": "John A",
          "name_family": "Smith",
          "name_suffix": ""
        }
      ],
      "addresses": [
        {
          "address": "123 Main Street, Apt 4B, New York, NY, 10001, US",
          "address_street": "123 Main Street Apt 4B",
          "address_city": "New York",
          "address_state": "NY",
          "address_zip": "10001",
          "address_country": "US"
        }
      ],
      "contacts": [
        {
          "contact": "555-123-4567"
        }
      ],
      "dates": [
        {
          "date": "1980-01-01"
        }
      ],
      "identifiers": [
        {
          "identifier": "123456"
        }
      ],
      "demographics": {
        "birthdate": "1980-01-01",
        "gender_code": "M",
        "gender": "Male"
      },
      "all_tokens": [
        "Mr.",
        "John A",
        "Smith",
        "Mr. John A Smith",
        "123 Main Street Apt 4B",
        "New York",
        "NY",
        "10001",
        "US",
        "123 Main Street, Apt 4B, New York, NY, 10001, US",
        "555-123-4567",
        "1980-01-01",
        "123456",
        "M",
        "Male"
      ]
    }
  }
]
```

**unique_tokens.json:**
```json
[
  "10001",
  "123 Main Street Apt 4B",
  "123 Main Street, Apt 4B, New York, NY, 10001, US",
  "123456",
  "1980-01-01",
  "555-123-4567",
  "John A",
  "M",
  "Male",
  "Mr.",
  "Mr. John A Smith",
  "NY",
  "New York",
  "Smith",
  "US"
]
```

## Advanced Usage

### Memory Management

Both tools are designed to handle large CCDA files efficiently. Here are some tips for processing large datasets:

- Process files in smaller batches if memory is a concern
- Increase or decrease batch size based on your system's capabilities

### Customizing Extraction

If you need to extract additional data beyond standard PHI, you can modify:
- The `extract_patient_phi` method in `ccda_phi_extractor.py`
- The XPath expressions used to locate elements

### Integration with Other Systems

To integrate with tokenization systems:
1. Run the PHI extractor to generate PHI data
2. Run the PHI tokenizer to prepare data for tokenization
3. Use the `unique_tokens.json` file as input to your tokenization system
4. Replace tokens in your documents using the mappings

## Common Issues and Solutions

### Missing or Incomplete Data

**Problem**: Some PHI elements are missing in the output.  
**Solution**: Check if the elements exist in the source XML. Not all CCDA files contain all PHI elements.

### Memory Errors

**Problem**: "MemoryError" when processing large files.  
**Solution**: Try reducing the batch size or processing files individually.

### XML Parsing Errors

**Problem**: "XMLSyntaxError" when parsing CCDA files.  
**Solution**: Verify the XML file is valid and well-formed. Some files may need preprocessing.

## Complete Processing Pipeline Example

Here's a complete example of extracting PHI and preparing it for tokenization:

```bash
# 1. Extract PHI from all CCDA files
python ccda_phi_extractor.py --input-dir /workspaces/CCDA-processor/input/ccda/to_process --output-file output/phi/phi_data.json

# 2. Prepare PHI data for tokenization (using a sample of 100 files)
python ccda_phi_tokenizer.py --sample-size 100 --output-file output/phi/tokenization_data.json

# 3. Review the extracted tokens
cat output/phi/unique_tokens.json
```
