#!/usr/bin/env python3
"""
CCDA PHI Extractor

This script extracts Protected Health Information (PHI) from CCDA XML files.
It focuses on patient demographic information and identifiers found primarily
in the patientRole element at /ClinicalDocument/recordTarget/patientRole.

Features:
- Complete extraction of all PHI elements
- Structured output with source paths for traceability
- Batch processing capability
- Memory-efficient XML parsing
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional
from lxml import etree
from tqdm import tqdm

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

class CCDAPHIExtractor:
    """Extracts Protected Health Information (PHI) from CCDA XML files."""
    
    def __init__(self):
        """Initialize the PHI extractor."""
        self.processed_files = 0
        self.failed_files = 0
    
    def extract_name(self, name_element: Optional[etree._Element]) -> Dict[str, Any]:
        """
        Extract structured name information from a name element.
        
        Args:
            name_element: The XML element containing name information
            
        Returns:
            Dictionary with structured name data
        """
        if name_element is None:
            return {}
        
        # Extract name components
        result = {
            "use": name_element.get("use", ""),
            "prefix": [prefix.text for prefix in name_element.xpath("./h:prefix", namespaces=CCDA_NS) if prefix.text],
            "given": [given.text for given in name_element.xpath("./h:given", namespaces=CCDA_NS) if given.text],
            "family": [family.text for family in name_element.xpath("./h:family", namespaces=CCDA_NS) if family.text],
            "suffix": [suffix.text for suffix in name_element.xpath("./h:suffix", namespaces=CCDA_NS) if suffix.text],
            "xpath": "/".join(name_element.getroottree().getpath(name_element).split("/")[-4:])
        }
        
        # Create a formatted full name for convenience
        components = []
        if result["prefix"]:
            components.extend(result["prefix"])
        if result["given"]:
            components.extend(result["given"])
        if result["family"]:
            components.extend(result["family"])
        if result["suffix"]:
            components.extend(result["suffix"])
        
        result["formatted"] = " ".join(components)
        return result
    
    def extract_address(self, addr_element: Optional[etree._Element]) -> Dict[str, Any]:
        """
        Extract structured address information from an addr element.
        
        Args:
            addr_element: The XML element containing address information
            
        Returns:
            Dictionary with structured address data
        """
        if addr_element is None:
            return {}
        
        # Extract address components
        street_lines = [line.text for line in addr_element.xpath("./h:streetAddressLine", namespaces=CCDA_NS) if line.text]
        
        result = {
            "use": addr_element.get("use", ""),
            "street_lines": street_lines,
            "city": next(iter([city.text for city in addr_element.xpath("./h:city", namespaces=CCDA_NS) if city.text]), ""),
            "state": next(iter([state.text for state in addr_element.xpath("./h:state", namespaces=CCDA_NS) if state.text]), ""),
            "postal_code": next(iter([zip.text for zip in addr_element.xpath("./h:postalCode", namespaces=CCDA_NS) if zip.text]), ""),
            "country": next(iter([country.text for country in addr_element.xpath("./h:country", namespaces=CCDA_NS) if country.text]), ""),
            "xpath": "/".join(addr_element.getroottree().getpath(addr_element).split("/")[-4:])
        }
        
        # Create a formatted address for convenience
        components = []
        if street_lines:
            components.extend(street_lines)
        if result["city"]:
            city_state = []
            if result["city"]:
                city_state.append(result["city"])
            if result["state"]:
                city_state.append(result["state"])
            components.append(", ".join(city_state))
        if result["postal_code"]:
            components.append(result["postal_code"])
        if result["country"]:
            components.append(result["country"])
        
        result["formatted"] = ", ".join(components)
        return result
    
    def extract_telecom(self, telecom_element: Optional[etree._Element]) -> Dict[str, str]:
        """
        Extract telecom information (phone, email, etc.)
        
        Args:
            telecom_element: The XML element containing telecom information
            
        Returns:
            Dictionary with telecom data
        """
        if telecom_element is None:
            return {}
        
        value = telecom_element.get("value", "")
        use = telecom_element.get("use", "")
        
        # Determine the telecom type
        telecom_type = "unknown"
        if value.startswith("tel:"):
            telecom_type = "phone"
            value = value.replace("tel:", "")
        elif value.startswith("mailto:"):
            telecom_type = "email"
            value = value.replace("mailto:", "")
        
        return {
            "type": telecom_type,
            "value": value,
            "use": use,
            "xpath": "/".join(telecom_element.getroottree().getpath(telecom_element).split("/")[-4:])
        }
    
    def extract_identifier(self, id_element: Optional[etree._Element]) -> Dict[str, str]:
        """
        Extract identifier information (MRN, SSN, etc.)
        
        Args:
            id_element: The XML element containing identifier information
            
        Returns:
            Dictionary with identifier data
        """
        if id_element is None:
            return {}
        
        root = id_element.get("root", "")
        extension = id_element.get("extension", "")
        
        # Try to determine the ID type based on OIDs
        id_type = "unknown"
        if root == "2.16.840.1.113883.4.1":
            id_type = "SSN"
        elif root == "2.16.840.1.113883.4.572":
            id_type = "Medicare"
        elif "MR" in root or "MRN" in root:
            id_type = "MRN"
        
        return {
            "type": id_type,
            "root": root,
            "extension": extension,
            "xpath": "/".join(id_element.getroottree().getpath(id_element).split("/")[-4:])
        }
    
    def extract_coded_value(self, element: Optional[etree._Element], value_type: str) -> Dict[str, str]:
        """
        Extract a coded value (gender, race, etc.)
        
        Args:
            element: The XML element containing the coded value
            value_type: Type of value being extracted
            
        Returns:
            Dictionary with coded value data
        """
        if element is None:
            return {}
        
        return {
            "type": value_type,
            "code": element.get("code", ""),
            "display_name": element.get("displayName", ""),
            "code_system": element.get("codeSystem", ""),
            "code_system_name": element.get("codeSystemName", ""),
            "xpath": "/".join(element.getroottree().getpath(element).split("/")[-4:])
        }
    
    def extract_patient_phi(self, patient_role: etree._Element) -> Dict[str, Any]:
        """
        Extract all PHI elements from a patientRole element.
        
        Args:
            patient_role: The patientRole XML element
            
        Returns:
            Dictionary with all extracted PHI
        """
        if patient_role is None:
            return {}
        
        patient = patient_role.xpath("./h:patient", namespaces=CCDA_NS)
        patient = patient[0] if patient else None
        
        phi_data = {
            "ids": [],
            "addresses": [],
            "telecoms": [],
            "names": [],
            "gender": {},
            "birthtime": {},
            "marital_status": {},
            "race": {},
            "ethnicity": {},
            "language": {},
            "guardian": {},
            "provider_organization": {}
        }
        
        # Extract identifiers
        for id_element in patient_role.xpath("./h:id", namespaces=CCDA_NS):
            phi_data["ids"].append(self.extract_identifier(id_element))
        
        # Extract addresses
        for addr_element in patient_role.xpath("./h:addr", namespaces=CCDA_NS):
            phi_data["addresses"].append(self.extract_address(addr_element))
        
        # Extract telecoms
        for telecom_element in patient_role.xpath("./h:telecom", namespaces=CCDA_NS):
            phi_data["telecoms"].append(self.extract_telecom(telecom_element))
        
        # Process patient information if available
        if patient is not None:
            # Extract names
            for name_element in patient.xpath("./h:name", namespaces=CCDA_NS):
                phi_data["names"].append(self.extract_name(name_element))
            
            # Extract birthtime
            birthtime = patient.xpath("./h:birthTime", namespaces=CCDA_NS)
            if birthtime:
                phi_data["birthtime"] = {
                    "value": birthtime[0].get("value", ""),
                    "xpath": "/".join(birthtime[0].getroottree().getpath(birthtime[0]).split("/")[-4:])
                }
            
            # Extract gender
            gender = patient.xpath("./h:administrativeGenderCode", namespaces=CCDA_NS)
            if gender:
                phi_data["gender"] = self.extract_coded_value(gender[0], "gender")
            
            # Extract marital status
            marital = patient.xpath("./h:maritalStatusCode", namespaces=CCDA_NS)
            if marital:
                phi_data["marital_status"] = self.extract_coded_value(marital[0], "marital_status")
            
            # Extract race
            race = patient.xpath("./h:raceCode", namespaces=CCDA_NS)
            if race:
                phi_data["race"] = self.extract_coded_value(race[0], "race")
            
            # Extract ethnicity
            ethnicity = patient.xpath("./h:ethnicGroupCode", namespaces=CCDA_NS)
            if ethnicity:
                phi_data["ethnicity"] = self.extract_coded_value(ethnicity[0], "ethnicity")
            
            # Extract language
            language = patient.xpath("./h:languageCommunication/h:languageCode", namespaces=CCDA_NS)
            if language:
                phi_data["language"] = {
                    "code": language[0].get("code", ""),
                    "xpath": "/".join(language[0].getroottree().getpath(language[0]).split("/")[-5:])
                }
        
        # Extract guardian information
        guardian = patient_role.xpath("./h:guardian", namespaces=CCDA_NS)
        if guardian:
            guardian_data = {
                "addresses": [],
                "telecoms": [],
                "names": []
            }
            
            # Extract guardian addresses
            for addr in guardian[0].xpath("./h:addr", namespaces=CCDA_NS):
                guardian_data["addresses"].append(self.extract_address(addr))
            
            # Extract guardian telecoms
            for telecom in guardian[0].xpath("./h:telecom", namespaces=CCDA_NS):
                guardian_data["telecoms"].append(self.extract_telecom(telecom))
            
            # Extract guardian names
            for name in guardian[0].xpath("./h:guardianPerson/h:name", namespaces=CCDA_NS):
                guardian_data["names"].append(self.extract_name(name))
            
            phi_data["guardian"] = guardian_data
        
        # Extract provider organization information
        provider_org = patient_role.xpath("./h:providerOrganization", namespaces=CCDA_NS)
        if provider_org:
            org_data = {
                "ids": [],
                "addresses": [],
                "telecoms": [],
                "names": []
            }
            
            # Extract organization IDs
            for id_element in provider_org[0].xpath("./h:id", namespaces=CCDA_NS):
                org_data["ids"].append(self.extract_identifier(id_element))
            
            # Extract organization name
            name = provider_org[0].xpath("./h:name", namespaces=CCDA_NS)
            if name:
                org_data["name"] = name[0].text
            
            # Extract organization addresses
            for addr in provider_org[0].xpath("./h:addr", namespaces=CCDA_NS):
                org_data["addresses"].append(self.extract_address(addr))
            
            # Extract organization telecoms
            for telecom in provider_org[0].xpath("./h:telecom", namespaces=CCDA_NS):
                org_data["telecoms"].append(self.extract_telecom(telecom))
            
            phi_data["provider_organization"] = org_data
        
        return phi_data
    
    def extract_phi_from_file(self, file_path: str) -> Dict[str, Any]:
        """
        Extract PHI from a single CCDA XML file.
        
        Args:
            file_path: Path to the CCDA XML file
            
        Returns:
            Dictionary with extracted PHI
        """
        try:
            # Use iterparse for memory-efficient parsing
            context = etree.iterparse(file_path, events=('end',), tag=f'{{{CCDA_NS["h"]}}}recordTarget')
            
            for event, elem in context:
                if event == 'end' and elem.tag == f'{{{CCDA_NS["h"]}}}recordTarget':
                    # Get patientRole element
                    patient_role = elem.xpath('./h:patientRole', namespaces=CCDA_NS)
                    
                    if patient_role:
                        # Extract all PHI from patientRole
                        phi_data = self.extract_patient_phi(patient_role[0])
                        
                        # Add file metadata
                        result = {
                            "file_name": os.path.basename(file_path),
                            "phi_data": phi_data
                        }
                        
                        # Clear element to save memory
                        elem.clear()
                        
                        self.processed_files += 1
                        return result
            
            # If we reach here, we didn't find a patientRole element
            logger.warning(f"No patientRole element found in {file_path}")
            self.failed_files += 1
            return {
                "file_name": os.path.basename(file_path),
                "phi_data": {},
                "error": "No patientRole element found"
            }
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {str(e)}")
            self.failed_files += 1
            return {
                "file_name": os.path.basename(file_path),
                "phi_data": {},
                "error": str(e)
            }
    
    def extract_phi_from_directory(self, input_dir: str, output_file: str = 'phi_data.json'):
        """
        Extract PHI from all CCDA XML files in a directory.
        
        Args:
            input_dir: Directory containing CCDA XML files
            output_file: Path to save the extracted PHI data as JSON
        """
        input_path = Path(input_dir)
        xml_files = list(input_path.glob('*.xml'))
        
        logger.info(f"Found {len(xml_files)} CCDA XML files to process")
        
        results = []
        for xml_file in tqdm(xml_files, desc="Extracting PHI"):
            phi_data = self.extract_phi_from_file(str(xml_file))
            results.append(phi_data)
        
        # Create output directory if it doesn't exist
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save the results
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"\nPHI extraction complete:")
        logger.info(f"- Successfully processed: {self.processed_files} files")
        logger.info(f"- Failed: {self.failed_files} files")
        logger.info(f"- Output: {output_file}")

def main():
    parser = argparse.ArgumentParser(
        description='Extract Protected Health Information (PHI) from CCDA XML files'
    )
    parser.add_argument(
        '--input-dir',
        default='input/ccda/to_process',
        help='Directory containing CCDA XML files'
    )
    parser.add_argument(
        '--output-file',
        default='output/phi/phi_data.json',
        help='Output JSON file for PHI data'
    )
    parser.add_argument(
        '--file',
        help='Process a single CCDA XML file'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    args = parser.parse_args()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    extractor = CCDAPHIExtractor()
    
    if args.file:
        # Process a single file
        result = extractor.extract_phi_from_file(args.file)
        
        # Save the result
        output_path = Path(args.output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump([result], f, indent=2)
            
        logger.info(f"PHI extracted from {args.file} and saved to {args.output_file}")
    else:
        # Process all files in directory
        extractor.extract_phi_from_directory(args.input_dir, args.output_file)

if __name__ == '__main__':
    main() 