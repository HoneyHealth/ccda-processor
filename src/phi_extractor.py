#!/usr/bin/env python3
"""
PHI Extractor for CCDA XML Files

This script extracts Protected Health Information (PHI) from CCDA XML files.
It focuses on the patientRole element which contains patient identifiers,
demographics, and contact information.

The script handles the HL7 namespace and extracts all PHI elements into a
structured dictionary format, preserving the source path for traceability.
"""

import xml.etree.ElementTree as ET
import os
import json
import argparse
from typing import Dict, List, Any, Optional, Union


def extract_phi_from_ccda(file_path: str) -> Dict[str, Any]:
    """
    Extract Protected Health Information (PHI) from a CCDA XML file.
    
    Args:
        file_path: Path to the CCDA XML file
        
    Returns:
        Dictionary containing structured PHI data with source paths
    """
    try:
        # Initialize the PHI dictionary
        phi_data = {
            'source_file': os.path.basename(file_path),
            'patient_identifiers': [],
            'patient_name': {},
            'patient_addresses': [],
            'patient_telecoms': [],
            'patient_demographics': {},
            'source_paths': {}  # Store XPath for each element
        }
        
        # Parse the XML file using a namespace-aware approach
        with open(file_path, 'r', encoding='utf-8') as f:
            xml_content = f.read()
        
        # Register the namespace
        ET.register_namespace('', 'urn:hl7-org:v3')
        
        # Parse the XML
        root = ET.fromstring(xml_content)
        
        # Extract namespace from the root element
        ns_uri = ''
        if '}' in root.tag:
            ns_uri = root.tag.split('}')[0].strip('{')
        
        # Find the patientRole element using tag matching
        patient_role = None
        for elem in root.iter():
            if elem.tag.endswith('recordTarget'):
                for child in elem:
                    if child.tag.endswith('patientRole'):
                        patient_role = child
                        break
                if patient_role:
                    break
        
        if patient_role is None:
            return phi_data
        
        # Extract patient identifiers
        extract_patient_identifiers(patient_role, phi_data, ns_uri)
        
        # Extract patient name
        extract_patient_name(patient_role, phi_data, ns_uri)
        
        # Extract patient addresses
        extract_patient_addresses(patient_role, phi_data, ns_uri)
        
        # Extract telecom information
        extract_patient_telecoms(patient_role, phi_data, ns_uri)
        
        # Extract patient demographics
        extract_patient_demographics(patient_role, phi_data, ns_uri)
        
        return phi_data
    
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return {
            'source_file': os.path.basename(file_path),
            'error': str(e)
        }


def extract_patient_identifiers(patient_role: ET.Element, phi_data: Dict[str, Any], ns_uri: str) -> None:
    """
    Extract patient identifiers from the patientRole element.
    
    Args:
        patient_role: The patientRole XML element
        phi_data: Dictionary to store the extracted PHI
        ns_uri: Namespace URI for XML parsing
    """
    identifiers = []
    source_paths = []
    
    for child in patient_role:
        if child.tag.endswith('id'):
            identifier = {
                'root': child.get('root', ''),
                'extension': child.get('extension', ''),
                'assigningAuthorityName': child.get('assigningAuthorityName', '')
            }
            identifiers.append(identifier)
            source_paths.append('/ClinicalDocument/recordTarget/patientRole/id')
    
    phi_data['patient_identifiers'] = identifiers
    phi_data['source_paths']['patient_identifiers'] = source_paths


def extract_patient_name(patient_role: ET.Element, phi_data: Dict[str, Any], ns_uri: str) -> None:
    """
    Extract patient name from the patientRole/patient/name element.
    
    Args:
        patient_role: The patientRole XML element
        phi_data: Dictionary to store the extracted PHI
        ns_uri: Namespace URI for XML parsing
    """
    patient = None
    for child in patient_role:
        if child.tag.endswith('patient'):
            patient = child
            break
    
    if patient is None:
        return
    
    names = []
    source_paths = []
    
    for child in patient:
        if child.tag.endswith('name'):
            name_elem = child
            given_names = []
            family_name = ''
            
            for name_part in name_elem:
                if name_part.tag.endswith('given') and name_part.text:
                    given_names.append(name_part.text)
                elif name_part.tag.endswith('family') and name_part.text:
                    family_name = name_part.text
            
            name_data = {
                'given': given_names,
                'family': family_name
            }
            names.append(name_data)
            source_paths.append('/ClinicalDocument/recordTarget/patientRole/patient/name')
    
    if names:
        # Use the first name as the primary name
        phi_data['patient_name'] = names[0]
        # Store all names if there are multiple
        if len(names) > 1:
            phi_data['patient_name']['additional_names'] = names[1:]
    
    phi_data['source_paths']['patient_name'] = source_paths


def extract_patient_addresses(patient_role: ET.Element, phi_data: Dict[str, Any], ns_uri: str) -> None:
    """
    Extract patient addresses from the patientRole/addr elements.
    
    Args:
        patient_role: The patientRole XML element
        phi_data: Dictionary to store the extracted PHI
        ns_uri: Namespace URI for XML parsing
    """
    addresses = []
    source_paths = []
    
    for child in patient_role:
        if child.tag.endswith('addr'):
            addr_elem = child
            street_lines = []
            city = ''
            state = ''
            postal_code = ''
            country = ''
            
            for addr_part in addr_elem:
                if addr_part.tag.endswith('streetAddressLine') and addr_part.text:
                    street_lines.append(addr_part.text)
                elif addr_part.tag.endswith('city') and addr_part.text:
                    city = addr_part.text
                elif addr_part.tag.endswith('state') and addr_part.text:
                    state = addr_part.text
                elif addr_part.tag.endswith('postalCode') and addr_part.text:
                    postal_code = addr_part.text
                elif addr_part.tag.endswith('country') and addr_part.text:
                    country = addr_part.text
            
            address = {
                'use': addr_elem.get('use', ''),
                'street': street_lines,
                'city': city,
                'state': state,
                'postalCode': postal_code,
                'country': country
            }
            addresses.append(address)
            source_paths.append('/ClinicalDocument/recordTarget/patientRole/addr')
    
    phi_data['patient_addresses'] = addresses
    phi_data['source_paths']['patient_addresses'] = source_paths


def extract_patient_telecoms(patient_role: ET.Element, phi_data: Dict[str, Any], ns_uri: str) -> None:
    """
    Extract patient telecom information from the patientRole/telecom elements.
    
    Args:
        patient_role: The patientRole XML element
        phi_data: Dictionary to store the extracted PHI
        ns_uri: Namespace URI for XML parsing
    """
    telecoms = []
    source_paths = []
    
    for child in patient_role:
        if child.tag.endswith('telecom'):
            telecom_data = {
                'value': child.get('value', ''),
                'use': child.get('use', '')
            }
            telecoms.append(telecom_data)
            source_paths.append('/ClinicalDocument/recordTarget/patientRole/telecom')
    
    phi_data['patient_telecoms'] = telecoms
    phi_data['source_paths']['patient_telecoms'] = source_paths


def extract_patient_demographics(patient_role: ET.Element, phi_data: Dict[str, Any], ns_uri: str) -> None:
    """
    Extract patient demographics from the patientRole/patient element.
    
    Args:
        patient_role: The patientRole XML element
        phi_data: Dictionary to store the extracted PHI
        ns_uri: Namespace URI for XML parsing
    """
    patient = None
    for child in patient_role:
        if child.tag.endswith('patient'):
            patient = child
            break
    
    if patient is None:
        return
    
    demographics = {}
    source_paths = {}
    
    for child in patient:
        # Extract birth time
        if child.tag.endswith('birthTime'):
            demographics['birthTime'] = child.get('value', '')
            source_paths['birthTime'] = '/ClinicalDocument/recordTarget/patientRole/patient/birthTime'
        
        # Extract gender
        elif child.tag.endswith('administrativeGenderCode'):
            demographics['gender'] = {
                'code': child.get('code', ''),
                'displayName': child.get('displayName', '')
            }
            source_paths['gender'] = '/ClinicalDocument/recordTarget/patientRole/patient/administrativeGenderCode'
        
        # Extract marital status
        elif child.tag.endswith('maritalStatusCode'):
            demographics['maritalStatus'] = {
                'code': child.get('code', ''),
                'displayName': child.get('displayName', '')
            }
            source_paths['maritalStatus'] = '/ClinicalDocument/recordTarget/patientRole/patient/maritalStatusCode'
        
        # Extract race
        elif child.tag.endswith('raceCode'):
            demographics['race'] = {
                'code': child.get('code', ''),
                'displayName': child.get('displayName', '')
            }
            source_paths['race'] = '/ClinicalDocument/recordTarget/patientRole/patient/raceCode'
        
        # Extract ethnicity
        elif child.tag.endswith('ethnicGroupCode'):
            demographics['ethnicity'] = {
                'code': child.get('code', ''),
                'displayName': child.get('displayName', '')
            }
            source_paths['ethnicity'] = '/ClinicalDocument/recordTarget/patientRole/patient/ethnicGroupCode'
        
        # Extract religious affiliation
        elif child.tag.endswith('religiousAffiliationCode'):
            demographics['religiousAffiliation'] = {
                'code': child.get('code', ''),
                'displayName': child.get('displayName', '')
            }
            source_paths['religiousAffiliation'] = '/ClinicalDocument/recordTarget/patientRole/patient/religiousAffiliationCode'
        
        # Extract language
        elif child.tag.endswith('languageCommunication'):
            for lang_part in child:
                if lang_part.tag.endswith('languageCode'):
                    demographics['language'] = lang_part.get('code', '')
                    source_paths['language'] = '/ClinicalDocument/recordTarget/patientRole/patient/languageCommunication/languageCode'
    
    phi_data['patient_demographics'] = demographics
    phi_data['source_paths']['patient_demographics'] = source_paths


def process_directory(input_dir: str, output_file: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Process all XML files in a directory and extract PHI.
    
    Args:
        input_dir: Directory containing CCDA XML files
        output_file: Optional path to save the output JSON
        
    Returns:
        List of dictionaries containing PHI data for each file
    """
    results = []
    
    # Process all XML files in the directory
    for filename in os.listdir(input_dir):
        if filename.endswith('.xml'):
            file_path = os.path.join(input_dir, filename)
            print(f"Processing {filename}...")
            phi_data = extract_phi_from_ccda(file_path)
            results.append(phi_data)
    
    # Save results to JSON file if output_file is specified
    if output_file:
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {output_file}")
    
    return results


def main():
    """Main function to parse arguments and process files."""
    parser = argparse.ArgumentParser(description='Extract PHI from CCDA XML files')
    parser.add_argument('--input-dir', help='Directory containing CCDA XML files')
    parser.add_argument('--output-file', help='Path to save the output JSON')
    parser.add_argument('--single-file', help='Process a single file instead of a directory')
    
    args = parser.parse_args()
    
    if args.single_file:
        # Process a single file
        phi_data = extract_phi_from_ccda(args.single_file)
        if args.output_file:
            with open(args.output_file, 'w') as f:
                json.dump(phi_data, f, indent=2)
            print(f"Results saved to {args.output_file}")
        else:
            print(json.dumps(phi_data, indent=2))
    elif args.input_dir:
        # Process a directory
        process_directory(args.input_dir, args.output_file)
    else:
        parser.error("Either --input-dir or --single-file is required")


if __name__ == "__main__":
    main()
