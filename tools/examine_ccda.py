import xml.etree.ElementTree as ET
import sys

def examine_patient_role(file_path):
    """
    Parse a CCDA XML file and print the patientRole element.
    """
    try:
        # Parse the XML file
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        # Extract namespace from the root element
        ns_uri = root.tag.split('}')[0].strip('{') if '}' in root.tag else None
        
        # Define the HL7 namespace
        ns = {'hl7': 'urn:hl7-org:v3'}
        
        # Find the patientRole element
        patient_role = root.find('.//hl7:recordTarget/hl7:patientRole', ns)
        
        if patient_role is not None:
            # Convert to string and print the first 2000 characters
            patient_role_str = ET.tostring(patient_role, encoding='unicode')
            print(f"First 2000 characters of patientRole element:")
            print(patient_role_str[:2000])
            print("\n" + "-" * 80 + "\n")
            
            # Print patient identifiers
            print("Patient IDs:")
            for id_elem in patient_role.findall('./hl7:id', ns):
                root_attr = id_elem.get('root', 'N/A')
                extension = id_elem.get('extension', 'N/A')
                auth_name = id_elem.get('assigningAuthorityName', 'N/A')
                print(f"  ID - Root: {root_attr}, Extension: {extension}, Authority: {auth_name}")
            
            # Print patient name
            print("\nPatient Name:")
            patient = patient_role.find('./hl7:patient', ns)
            if patient is not None:
                for name in patient.findall('./hl7:name', ns):
                    given_elements = name.findall('./hl7:given', ns)
                    given = []
                    for g in given_elements:
                        if g.text:
                            given.append(g.text)
                    
                    family_element = name.find('./hl7:family', ns)
                    family = family_element.text if family_element is not None and family_element.text else 'N/A'
                    
                    print(f"  Given: {given}, Family: {family}")
            
            # Print patient address
            print("\nPatient Address:")
            for addr in patient_role.findall('./hl7:addr', ns):
                use = addr.get('use', 'N/A')
                
                street_elements = addr.findall('./hl7:streetAddressLine', ns)
                street = []
                for s in street_elements:
                    if s.text:
                        street.append(s.text)
                
                city_element = addr.find('./hl7:city', ns)
                city = city_element.text if city_element is not None and city_element.text else 'N/A'
                
                state_element = addr.find('./hl7:state', ns)
                state = state_element.text if state_element is not None and state_element.text else 'N/A'
                
                zip_element = addr.find('./hl7:postalCode', ns)
                zip_code = zip_element.text if zip_element is not None and zip_element.text else 'N/A'
                
                country_element = addr.find('./hl7:country', ns)
                country = country_element.text if country_element is not None and country_element.text else 'N/A'
                
                print(f"  Use: {use}")
                print(f"  Street: {street}")
                print(f"  City: {city}")
                print(f"  State: {state}")
                print(f"  Zip: {zip_code}")
                print(f"  Country: {country}")
            
            # Print telecom information
            print("\nTelecom Information:")
            for telecom in patient_role.findall('./hl7:telecom', ns):
                value = telecom.get('value', 'N/A')
                use = telecom.get('use', 'N/A')
                print(f"  Value: {value}, Use: {use}")
            
            # Print patient demographics
            print("\nPatient Demographics:")
            if patient is not None:
                birth_time = patient.find('./hl7:birthTime', ns)
                if birth_time is not None:
                    print(f"  Birth Time: {birth_time.get('value', 'N/A')}")
                
                gender = patient.find('./hl7:administrativeGenderCode', ns)
                if gender is not None:
                    print(f"  Gender: {gender.get('code', 'N/A')}, {gender.get('displayName', 'N/A')}")
                
                marital_status = patient.find('./hl7:maritalStatusCode', ns)
                if marital_status is not None:
                    print(f"  Marital Status: {marital_status.get('code', 'N/A')}, {marital_status.get('displayName', 'N/A')}")
                
                race = patient.find('./hl7:raceCode', ns)
                if race is not None:
                    print(f"  Race: {race.get('code', 'N/A')}, {race.get('displayName', 'N/A')}")
                
                ethnicity = patient.find('./hl7:ethnicGroupCode', ns)
                if ethnicity is not None:
                    print(f"  Ethnicity: {ethnicity.get('code', 'N/A')}, {ethnicity.get('displayName', 'N/A')}")
                
                religious_affiliation = patient.find('./hl7:religiousAffiliationCode', ns)
                if religious_affiliation is not None:
                    print(f"  Religious Affiliation: {religious_affiliation.get('code', 'N/A')}, {religious_affiliation.get('displayName', 'N/A')}")
                
                language_element = patient.find('.//hl7:languageCommunication/hl7:languageCode', ns)
                if language_element is not None:
                    print(f"  Language: {language_element.get('code', 'N/A')}")
        else:
            print("No patientRole element found in the document.")
    
    except Exception as e:
        print(f"Error parsing XML file: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        examine_patient_role(file_path)
    else:
        print("Please provide a CCDA XML file path as an argument.")
