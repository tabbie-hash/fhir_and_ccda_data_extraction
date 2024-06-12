import json
from fhir.resources.observation import Observation
from fhir.resources.patient import Patient
from fhir.resources.medicationrequest import MedicationRequest
from fhir.resources.condition import Condition
import pandas as pd 
import numpy as np 
import os
import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta


class FhirToPandas:
    """
    A class to handle the conversion of FHIR (Fast Healthcare Interoperability Resources) files to pandas DataFrames.

    This class provides methods to initialize the conversion process and ensures that the necessary directories for 
    storing the resulting CSV files are created.

    Attributes:
    file (str): The name of the FHIR file to be processed.
    """

    def __init__(self, file):
        """
        Initializes the FhirToPandas with the specified file.

        This method sets the file attribute and creates the required directory structure if it does not already exist.

        Parameters:
        file (str): The name of the FHIR file to be processed.
        """
        self.file = file

        if not os.path.exists('./fhir/fhir_csv_files'):
            os.makedirs('./fhir/fhir_csv_files')

    def read_json(self):
        """
        Reads a JSON file from a specified directory and loads its content into a Python dictionary.

        This function is designed to load JSON data from files stored in a predefined directory.

        Parameters:
        - file (str): The name of the file to read. This should include the file extension,
                    and the file must be located in the './fhir/' directory.

        Returns:
        - dict: A dictionary containing the loaded JSON data from the file.
        """
        # Define the directory where the JSON files are stored
        directory = './fhir/'

        # Open the JSON file located at the specified directory path
        with open(directory + self.file) as open_json:
            # Load the JSON content from the file into a Python dictionary
            jsondata = json.load(open_json)

        # Return the Python dictionary containing the JSON data
        return jsondata

    def fhir_observation_dataframe(self):
        """
        Constructs and returns a single combined pandas DataFrame containing patient observation details
        extracted from each encounter as recorded in raw FHIR data files specified by `self.fileslist`.

        This function processes each specified file to extract observation details such as the patient's ID,
        encounter ID, status of the observation, and the results of laboratory tests, among other data. These
        details are then combined into a unified DataFrame, which is subsequently saved to a CSV file.

        Returns:
            str: The file path to the CSV file containing the combined patient observation data.
        """

        if not os.path.exists('./fhir/fhir_csv_files/observations'):
            os.makedirs('./fhir/fhir_csv_files/observations')

        # for file in self.fileslist:
        filedata = self.read_json()  # Read the JSON data from file

        # Prepare dictionaries to store patient data
        patients_dict = {}
        patient_name = []
        patient_id = []

        # Extract patient data from the 'entry' section of the JSON
        for i in filedata['entry']:
            # Check if the current entry pertains to a Patient
            if i['request']['url'] in ['Patient']:
                fhir_patient_dict = i['resource']
                parse_patient_data = Patient.parse_obj(fhir_patient_dict)

                # Extract and construct the full patient name or assign 'N/A' if not available
                if parse_patient_data.name:
                    legal_name = parse_patient_data.name[0]
                    full_name = " ".join(legal_name.given) + " " + legal_name.family
                    patient_name.append(full_name)
                else:
                    patient_name.append('N/A')

                # Extract patient ID or assign 'N/A' if not available
                if parse_patient_data.id:
                    patient_id.append(parse_patient_data.id)
                else:
                    patient_id.append('N/A')

        # Populate the dictionary with collected patient IDs and names
        patients_dict['patient_id'] = patient_id
        patients_dict['patient_name'] = patient_name
        patients_dataframe = pd.DataFrame(patients_dict)

        # Initialize lists to collect observation data
        observation_dict = {}
        patient_id = []
        encounter_id = []
        observation_status = []
        observation_category = []
        lab_test = []
        lab_test_code = []
        effective_date = []
        effective_time = []
        issued_date = []
        issued_time = []
        test_result = []

        # Extract and process observation data from each entry
        for i in filedata['entry']:
            if i['request']['url'] in ['Observation']:
                fhir_observation_dict = i['resource']
                parse_observation_data = Observation.parse_obj(fhir_observation_dict)

                # Parse and append patient and encounter IDs, handling missing data
                patient_id.append(parse_observation_data.subject.reference.split(':')[2] if parse_observation_data.subject.reference else 'N/A')
                encounter_id.append(parse_observation_data.encounter.reference.split(':')[2] if parse_observation_data.encounter.reference else 'N/A')
                observation_status.append(parse_observation_data.status if parse_observation_data.status else 'N/A')
                observation_category.append(parse_observation_data.category[0].coding[0].display if parse_observation_data.category else 'N/A')
                lab_test.append(parse_observation_data.code.text if parse_observation_data.code.text else 'N/A')
                lab_test_code.append(parse_observation_data.code.coding[0].code if parse_observation_data.code else 'N/A')

                # Process effective and issued datetime details
                if parse_observation_data.effectiveDateTime:
                    datetime_parts = str(parse_observation_data.effectiveDateTime).split(' ')
                    effective_date.append(datetime_parts[0])
                    effective_time.append(datetime_parts[1])
                else:
                    effective_date.append('N/A')
                    effective_time.append('N/A')

                if parse_observation_data.issued:
                    datetime_parts = str(parse_observation_data.issued).split(' ')
                    issued_date.append(datetime_parts[0])
                    issued_time.append(datetime_parts[1])
                else:
                    issued_date.append('N/A')
                    issued_time.append('N/A')

                # Handle different types of test results
                if parse_observation_data.valueQuantity:
                    test_result.append(f"{parse_observation_data.valueQuantity.value}{parse_observation_data.valueQuantity.unit}")
                elif parse_observation_data.valueCodeableConcept:
                    test_result.append(parse_observation_data.valueCodeableConcept.text)
                else:
                    test_result.append('N/A')

        # Create a DataFrame for this file's observations and add to list
        observation_dict['encounter_id'] = encounter_id
        observation_dict['patient_id'] = patient_id
        observation_dict['observation_category'] = observation_category
        observation_dict['observation_status'] = observation_status
        observation_dict['laboratory_test'] = lab_test
        observation_dict['laboratory_test_code'] = lab_test_code
        observation_dict['test_result'] = test_result
        observation_dict['effective_date'] = effective_date
        observation_dict['effective_time'] = effective_time
        observation_dict['issued_date'] = issued_date
        observation_dict['issued_time'] = issued_time

        observation_dataframe = pd.DataFrame(observation_dict)
        combined_df =  pd.merge(observation_dataframe, patients_dataframe, on="patient_id").fillna('N/A').sort_values(by='effective_date',ascending=True)

        # Save the combined DataFrame to a CSV file
        combined_file_path = './fhir/fhir_csv_files/observations/'+str(self.file).split('.')[0]+'_observations_data.csv'
        combined_df.to_csv(combined_file_path, index=False)

        # Return the path of the saved CSV file
        return combined_file_path

    def fhir_conditions_dataframe(self):
        """
        Constructs and returns a single combined pandas DataFrame containing detailed information about
        patient conditions from each encounter, extracted from raw FHIR data files specified by `self.fileslist`.

        This function iterates over each file, extracts relevant patient condition data such as clinical status,
        verification status, condition codes, and dates related to the onset and abatement of the condition.
        These individual DataFrames are then merged into a unified DataFrame, which is saved to a CSV file.

        Returns:
            str: The file path to the CSV file containing the combined patient conditions data.
        """

        if not os.path.exists('./fhir/fhir_csv_files/conditions'):
            os.makedirs('./fhir/fhir_csv_files/conditions')

        filedata = self.read_json()  # Read the JSON data from the file

        # Prepare dictionaries to store patient data
        patients_dict = {}
        patient_name = []
        patient_id = []

        # Extract patient data from the 'entry' section of the JSON
        for i in filedata['entry']:
            # Check if the current entry pertains to a Patient
            if i['request']['url'] in ['Patient']:
                fhir_patient_dict = i['resource']
                parse_patient_data = Patient.parse_obj(fhir_patient_dict)

                # Extract and construct the full patient name or assign 'N/A' if not available
                if parse_patient_data.name:
                    legal_name = parse_patient_data.name[0]
                    full_name = " ".join(legal_name.given) + " " + legal_name.family
                    patient_name.append(full_name)
                else:
                    patient_name.append('N/A')

                # Extract patient ID or assign 'N/A' if not available
                if parse_patient_data.id:
                    patient_id.append(parse_patient_data.id)
                else:
                    patient_id.append('N/A')

        # Populate the dictionary with collected patient IDs and names
        patients_dict['patient_id'] = patient_id
        patients_dict['patient_name'] = patient_name
        patients_dataframe = pd.DataFrame(patients_dict)

        # Initialize lists to collect condition data for each patient
        conditions_dict = {}
        patient_id = []
        encounter_id = []
        clinical_status = []
        verification_status = []
        condition_category = []
        condition_description = []
        condition_code = []
        onset_date = []
        onset_time = []
        abatement_date = []
        abatement_time = []

        # Extract and process condition data from each entry
        for i in filedata['entry']:
            if i['request']['url'] in ['Condition']:
                fhir_condition_dict = i['resource']
                parse_condition_data = Condition.parse_obj(fhir_condition_dict)

                # Collect patient and encounter IDs, handle missing data
                patient_id.append(parse_condition_data.subject.reference.split(':')[2] if parse_condition_data.subject.reference else 'N/A')
                encounter_id.append(parse_condition_data.encounter.reference.split(':')[2] if parse_condition_data.encounter.reference else 'N/A')

                # Extract condition details, handle missing data
                clinical_status.append(parse_condition_data.clinicalStatus.coding[0].code if parse_condition_data.clinicalStatus.coding else 'N/A')
                verification_status.append(parse_condition_data.verificationStatus.coding[0].code if parse_condition_data.verificationStatus.coding else 'N/A')
                condition_category.append(parse_condition_data.category[0].coding[0].display if parse_condition_data.category else 'N/A')
                condition_description.append(parse_condition_data.code.text if parse_condition_data.code.text else 'N/A')
                condition_code.append(parse_condition_data.code.coding[0].code if parse_condition_data.code else 'N/A')

                # Process datetime details for onset and abatement
                if parse_condition_data.onsetDateTime:
                    datetime_parts = str(parse_condition_data.onsetDateTime).split(' ')
                    onset_date.append(datetime_parts[0])
                    onset_time.append(datetime_parts[1])
                else:
                    onset_date.append('N/A')
                    onset_time.append('N/A')

                if parse_condition_data.abatementDateTime:
                    datetime_parts = str(parse_condition_data.abatementDateTime).split(' ')
                    abatement_date.append(datetime_parts[0])
                    abatement_time.append(datetime_parts[1])
                else:
                    abatement_date.append('N/A')
                    abatement_time.append('N/A')

        # Create DataFrame for this file's conditions and add to list
        conditions_dict['encounter_id'] = encounter_id
        conditions_dict['patient_id'] = patient_id
        conditions_dict['condition_category'] = condition_category
        conditions_dict['condition_description'] = condition_description
        conditions_dict['condition_code'] = condition_code
        conditions_dict['clinical_status'] = clinical_status
        conditions_dict['verification_status'] = verification_status
        conditions_dict['onset_date'] = onset_date
        conditions_dict['onset_time'] = onset_time
        conditions_dict['abatement_date'] = abatement_date
        conditions_dict['abatement_time'] = abatement_time

        conditions_dataframe = pd.DataFrame(conditions_dict)
        combined_df =  pd.merge(conditions_dataframe, patients_dataframe, on="patient_id").fillna('N/A').sort_values(by='onset_date',ascending=True)

        # Save the combined DataFrame to a CSV file
        combined_file_path = './fhir/fhir_csv_files/conditions/'+str(self.file).split('.')[0]+'_conditions_data.csv'
        combined_df.to_csv(combined_file_path, index=False)

        # Return the path of the saved CSV file
        return combined_file_path

    def fhir_medications_dataframe(self):
        """
        Generates a pandas DataFrame aggregating medication request details for individual patients across
        multiple encounters, sourced from FHIR data files. The resultant DataFrame is then stored as a CSV file.

        This function processes each specified FHIR data file, extracting and compiling detailed medication
        request information such as the medication type, status, intent, and authoring details into a structured
        format. The processed data from all files is combined into a single DataFrame to provide a comprehensive
        overview of medication requests.

        Returns:
            str: The file path to the CSV file containing the compiled medication request data.
        """

        if not os.path.exists('./fhir/fhir_csv_files/medications'):
            os.makedirs('./fhir/fhir_csv_files/medications')

        filedata = self.read_json()  # Read JSON data from the file

        # Prepare dictionaries to store patient data
        patients_dict = {}
        patient_name = []
        patient_id = []

        # Extract patient data from the 'entry' section of the JSON
        for i in filedata['entry']:
            # Check if the current entry pertains to a Patient
            if i['request']['url'] in ['Patient']:
                fhir_patient_dict = i['resource']
                parse_patient_data = Patient.parse_obj(fhir_patient_dict)

                # Extract and construct the full patient name or assign 'N/A' if not available
                if parse_patient_data.name:
                    legal_name = parse_patient_data.name[0]
                    full_name = " ".join(legal_name.given) + " " + legal_name.family
                    patient_name.append(full_name)
                else:
                    patient_name.append('N/A')

                # Extract patient ID or assign 'N/A' if not available
                if parse_patient_data.id:
                    patient_id.append(parse_patient_data.id)
                else:
                    patient_id.append('N/A')

        # Populate the dictionary with collected patient IDs and names
        patients_dict['patient_id'] = patient_id
        patients_dict['patient_name'] = patient_name
        patients_dataframe = pd.DataFrame(patients_dict)

        # Initialize collections to gather medication data
        medications_dict = {}
        patient_id = []
        encounter_id = []
        medication_status = []
        medication_intent = []
        medication_category = []
        medication = []
        medication_code = []
        authorized_date = []
        authorized_time = []
        physician = []

        # Iterate over each entry in the file data
        for i in filedata['entry']:
            if i['request']['url'] == 'MedicationRequest':
                fhir_medication_dict = i['resource']
                parse_medication_data = MedicationRequest.parse_obj(fhir_medication_dict)

                # Append patient and encounter IDs, handling missing values
                patient_id.append(parse_medication_data.subject.reference.split(':')[2] if parse_medication_data.subject.reference else 'N/A')
                encounter_id.append(parse_medication_data.encounter.reference.split(':')[2] if parse_medication_data.encounter.reference else 'N/A')

                # Collect status, intent, and category of the medication request
                medication_status.append(parse_medication_data.status if parse_medication_data.status else 'N/A')
                medication_intent.append(parse_medication_data.intent if parse_medication_data.intent else 'N/A')
                medication_category.append(parse_medication_data.category[0].text if parse_medication_data.category else 'N/A')
                medication_code.append(fhir_medication_dict.get('medicationCodeableConcept', {}).get('coding', [{}])[0].get('code', 'N/A'))
                # Extract the actual medication name, if available
                medication.append(fhir_medication_dict.get('medicationCodeableConcept', {}).get('text', 'N/A'))

                # Handle authorized date and time separately to account for potential missing data
                if parse_medication_data.authoredOn:
                    date, time = str(parse_medication_data.authoredOn).split(' ')
                    authorized_date.append(date)
                    authorized_time.append(time)
                else:
                    authorized_date.append('N/A')
                    authorized_time.append('N/A')

                # Retrieve physician's name if available
                physician.append(parse_medication_data.requester.display if parse_medication_data.requester.display else 'N/A')

        # Create a DataFrame from the accumulated data for this file
        medications_dict['encounter_id'] = encounter_id
        medications_dict['patient_id'] = patient_id
        medications_dict['medication_category'] = medication_category
        medications_dict['medication'] = medication
        medications_dict['medication_code'] = medication_code
        medications_dict['Medication_Status'] = medication_status
        medications_dict['medication_status'] = medication_intent
        medications_dict['authorized_date'] = authorized_date
        medications_dict['authorized_time'] = authorized_time
        medications_dict['physician'] = physician

        medications_dataframe = pd.DataFrame(medications_dict)
        combined_df =  pd.merge(medications_dataframe, patients_dataframe, on="patient_id").fillna('N/A').sort_values(by='authorized_date',ascending=True)

        # Save the combined DataFrame to a CSV file
        combined_file_path = './fhir/fhir_csv_files/medications/'+str(self.file).split('.')[0]+'_medications_data.csv'

        combined_df.to_csv(combined_file_path, index=False)

        # Return the path of the saved CSV file
        return combined_file_path

class CCDAToPandas:
    """
    A class to handle the conversion of CCDA (Consolidated Clinical Document Architecture) XML files to pandas DataFrames.

    This class provides methods to initialize the conversion process and ensures that the necessary directories for 
    storing the resulting CSV files are created.

    Attributes:
    file (str): The name of the CCDA XML file to be processed.
    """

    def __init__(self, file):
        """
        Initializes the CCDAToPandas with the specified file.

        This method sets the file attribute and creates the required directory structure if it does not already exist.

        Parameters:
        file (str): The name of the CCDA XML file to be processed.
        """
        self.file = file
        if not os.path.exists('./ccda/ccda_csv_files'):
            os.makedirs('./ccda/ccda_csv_files')

    def read_xml(self):
        """
        Reads and parses an XML file.

        This function reads an XML file from a specified directory, parses its content using BeautifulSoup,
        and returns the parsed content.

        Returns:
        BeautifulSoup object: The parsed XML content.
        """
        directory = './ccda/'  # Specify the directory where the XML file is located

        # Open and read the XML file
        with open(directory + self.file, 'r') as file:
            content = file.read()  # Read the content of the file

        # Parse the XML content using BeautifulSoup
        soup = BeautifulSoup(content, 'xml')

        return soup  # Return the parsed content as a BeautifulSoup object

    def get_text(self, element, tag):
        """
        Extracts text from a specific tag within an XML element.

        This function searches for a specified tag within an XML element, and if the tag is found, 
        it returns the text content of the tag. If the tag is not found, it returns 'N/A'.

        Parameters:
        element (BeautifulSoup element): The XML element to search within.
        tag (str): The tag to search for within the element.

        Returns:
        str: The text content of the tag if found, otherwise 'N/A'.
        """
        # Find the specified tag within the XML element
        found = element.find(tag)

        # Return the text content of the found tag, or 'N/A' if the tag is not found
        return found.get_text(strip=True) if found else 'N/A'

    def process_medication(self):
        """
        Processes medication data from an XML file and saves it as a CSV file.

        This function reads an XML file, extracts medication data, processes it to extract relevant
        information such as patient name, gender, medication start and stop dates/times, and codes.
        It then saves the processed data into a CSV file in a specified directory.

        Returns:
        str: The file path of the saved CSV file.
        """
        # Ensure the directory for storing medication CSV files exists
        if not os.path.exists('./ccda/ccda_csv_files/medications'):
            os.makedirs('./ccda/ccda_csv_files/medications')

        # Read and parse the XML file
        soup = self.read_xml()
        
        # Extract patient information
        patient_role = soup.find('patientRole')
        patient_data = {}
        if patient_role:
            name_element = patient_role.find('name')
            if name_element:
                given_name = self.get_text(name_element, 'given')
                family_name = self.get_text(name_element, 'family')
                patient_data['Name'] = f"{given_name} {family_name}"
            patient = patient_role.find('patient')
            administrative_gender = patient.find('administrativeGenderCode')['code'] if patient.find('administrativeGenderCode') else 'N/A'
            patient_data['Gender'] = administrative_gender

        # Extract medication data
        medications_data = []
        medications_section = soup.find('title', text='Medications')
        if medications_section:
            table = medications_section.find_next('table')
            if table:
                rows = table.find('tbody').find_all('tr')
                for row in rows:
                    columns = row.find_all('td')
                    medication = {
                        'record_type': 'Medications',
                        'patient_name': patient_data.get('Name', 'N/A'),
                        'patient_gender': patient_data.get('Gender', 'N/A'),
                        'authorized_datetime': str(columns[0].text),
                        'stop_datetime': str(columns[1].text),
                        'medication': columns[2].text,
                        'medication_code': columns[3].text
                    }
                    medications_data.append(medication)

        # Convert the medication data into a DataFrame and process it
        medications_df = pd.DataFrame(medications_data)
        combined_df = medications_df.sort_values(by='authorized_datetime', ascending=True).fillna('N/A')
        combined_df['authorized_date'] = combined_df['authorized_datetime'].apply(lambda x: pd.to_datetime(str(x), errors='coerce').date() if str(x) not in ['N/A', '', None, np.nan, 'NaT'] else 'N/A')
        combined_df['authorized_time'] = combined_df['authorized_datetime'].apply(lambda x: pd.to_datetime(str(x), errors='coerce').time() if str(x) not in ['N/A', '', None, np.nan, 'NaT'] else 'N/A')
        combined_df['stop_date'] = combined_df['stop_datetime'].apply(lambda x: pd.to_datetime(str(x), errors='coerce').date() if str(x) not in ['N/A', '', None, np.nan, 'NaT'] else 'N/A')
        combined_df['stop_time'] = combined_df['stop_datetime'].apply(lambda x: pd.to_datetime(str(x), errors='coerce').time() if str(x) not in ['N/A', '', None, np.nan, 'NaT'] else 'N/A')
        combined_df['medication_code'] = combined_df['medication_code'].apply(lambda x: re.sub(r'[^0-9-]+', '', str(x)) if str(x) not in ['N/A', '', np.nan, None] else 'N/A')

        # Remove the original Start and Stop columns
        del combined_df['authorized_datetime']
        del combined_df['stop_datetime']

        # Save the processed DataFrame to a CSV file
        combined_file_path = './ccda/ccda_csv_files/medications/' + str(self.file).split('.')[0] + '_medications_data.csv'
        combined_df.to_csv(combined_file_path, index=False)
        
        # Return the file path of the saved CSV file
        return combined_file_path

    def process_observations(self):
        """
        Processes observation data from an XML file and saves it as a CSV file.

        This function reads an XML file, extracts observation data, processes it to extract relevant
        information such as patient name, gender, observation category, laboratory test codes, 
        test results, and effective dates/times. It then saves the processed data into a CSV file
        in a specified directory.

        Returns:
        str: The file path of the saved CSV file.
        """
        # Ensure the directory for storing observation CSV files exists
        if not os.path.exists('./ccda/ccda_csv_files/observations'):
            os.makedirs('./ccda/ccda_csv_files/observations')

        # Read and parse the XML file
        soup = self.read_xml()
        
        # Extract patient information
        patient_role = soup.find('patientRole')
        patient_data = {}
        if patient_role:
            name_element = patient_role.find('name')
            if name_element:
                given_name = self.get_text(name_element, 'given')
                family_name = self.get_text(name_element, 'family')
                patient_data['Name'] = f"{given_name} {family_name}"
            patient = patient_role.find('patient')
            administrative_gender = patient.find('administrativeGenderCode')['code'] if patient.find('administrativeGenderCode') else 'N/A'
            patient_data['Gender'] = administrative_gender

        # Extract observation data
        observations_data = []
        observations_components = soup.find_all('component', recursive=True)
        for component in observations_components:
            observation = component.find('observation')
            if observation:
                result = {
                    'record_type': 'Observation_Results',
                    'patient_name': patient_data.get('Name', 'N/A'),
                    'patient_gender': patient_data.get('Gender', 'N/A'),
                    'observation_category': observation.find('code').get('displayName', 'N/A'),
                    'laboratory_test_code': observation.find('code').get('code', 'N/A'),
                    'test_result': observation.find('value').get('value', 'N/A'),
                    'test_result_unit': observation.find('value').get('unit', 'N/A'),
                    'effective_datetime': observation.find('effectiveTime').get('value', 'N/A')
                }
                observations_data.append(result)

        # Convert the observation data into a DataFrame and process it
        observations_df = pd.DataFrame(observations_data)
        combined_df = observations_df.sort_values(by='effective_datetime', ascending=True).fillna('N/A')
        combined_df['effective_date'] = combined_df['effective_datetime'].apply(lambda x: pd.to_datetime(str(x), errors='coerce').date() if str(x) not in ['N/A', '', None, np.nan, 'NaT'] else 'N/A')
        combined_df['effective_time'] = combined_df['effective_datetime'].apply(lambda x: pd.to_datetime(str(x), errors='coerce').time() if str(x) not in ['N/A', '', None, np.nan, 'NaT'] else 'N/A')
        del combined_df['effective_datetime']

        # Save the processed DataFrame to a CSV file
        combined_file_path = './ccda/ccda_csv_files/observations/' + str(self.file).split('.')[0] + '_observations_data.csv'
        combined_df.to_csv(combined_file_path, index=False)
        
        # Return the file path of the saved CSV file
        return combined_file_path

    def process_problem(self):
        """
        Processes problem data from an XML file and saves it as a CSV file.

        This function reads an XML file, extracts problem data, processes it to extract relevant
        information such as patient name, gender, onset/abatement dates, condition descriptions,
        and condition codes. It then saves the processed data into a CSV file in a specified directory.

        Returns:
        str: The file path of the saved CSV file.
        """
        # Ensure the directory for storing problem CSV files exists
        if not os.path.exists('./ccda/ccda_csv_files/problems'):
            os.makedirs('./ccda/ccda_csv_files/problems')

        # Read and parse the XML file
        soup = self.read_xml()
        
        # Extract patient information
        patient_role = soup.find('patientRole')
        patient_data = {}
        if patient_role:
            name_element = patient_role.find('name')
            if name_element:
                given_name = self.get_text(name_element, 'given')
                family_name = self.get_text(name_element, 'family')
                patient_data['Name'] = f"{given_name} {family_name}"
            patient = patient_role.find('patient')
            administrative_gender = patient.find('administrativeGenderCode')['code'] if patient.find('administrativeGenderCode') else 'N/A'
            patient_data['Gender'] = administrative_gender

        # Extract problem data
        problems_data = []
        problems_section = soup.find('title', text='Problems')
        if problems_section:
            table = problems_section.find_next('table')
            if table:
                rows = table.find('tbody').find_all('tr')
                for row in rows:
                    columns = row.find_all('td')
                    problem = {
                        'record_type': 'Problems',
                        'patient_name': patient_data.get('Name', 'N/A'),
                        'patient_gender': patient_data.get('Gender', 'N/A'),
                        'onset_datetime': str(columns[0].text),
                        'abatement_datetime': str(columns[1].text),
                        'condition_description': columns[2].text,
                        'condition_code': columns[3].text
                    }
                    problems_data.append(problem)

        # Convert the problem data into a DataFrame and process it
        problems_df = pd.DataFrame(problems_data)
        combined_df = problems_df.sort_values(by='onset_datetime', ascending=True).fillna('N/A')
        combined_df['onset_date'] = combined_df['onset_datetime'].apply(lambda x: pd.to_datetime(str(x), errors='coerce').date() if str(x) not in ['N/A', '', None, np.nan, 'NaT'] else 'N/A')
        combined_df['onset_time'] = combined_df['onset_datetime'].apply(lambda x: pd.to_datetime(str(x), errors='coerce').time() if str(x) not in ['N/A', '', None, np.nan, 'NaT'] else 'N/A')
        combined_df['abatement_date'] = combined_df['abatement_datetime'].apply(lambda x: pd.to_datetime(str(x), errors='coerce').date() if str(x) not in ['N/A', '', None, np.nan, 'NaT'] else 'N/A')
        combined_df['abatement_time'] = combined_df['abatement_datetime'].apply(lambda x: pd.to_datetime(str(x), errors='coerce').time() if str(x) not in ['N/A', '', None, np.nan, 'NaT'] else 'N/A')
        combined_df['condition_code'] = combined_df['condition_code'].apply(lambda x: re.sub(r'[^0-9-]+', '', str(x)) if str(x) not in ['N/A', '', np.nan, None] else 'N/A')
        del combined_df['onset_datetime']
        del combined_df['abatement_datetime']

        # Save the processed DataFrame to a CSV file
        combined_file_path = './ccda/ccda_csv_files/problems/' + str(self.file).split('.')[0] + '_problems_data.csv'
        combined_df.to_csv(combined_file_path, index=False)
        
        # Return the file path of the saved CSV file
        return combined_file_path

class CSVDataPreprocessor:
    """
    A class to preprocess CSV data files.

    This class provides methods for initializing the preprocessing of CSV files. It reads the specified CSV file
    into a pandas DataFrame and prepares it for further processing.

    Attributes:
    directory (str): The directory where the CSV file is located.
    file (str): The name of the CSV file to be processed.
    df (pd.DataFrame): The DataFrame containing the data read from the CSV file.
    """

    def __init__(self, directory, file):
        """
        Initialize the CSVDataPreprocessor with the directory and file name of the CSV file.

        This method sets the directory and file attributes, and reads the CSV file into a pandas DataFrame.

        Parameters:
        directory (str): The directory where the CSV file is located.
        file (str): The name of the CSV file to be processed.
        """
        self.directory = directory
        self.file = file
        self.df = pd.read_csv(directory + '/' + file)
    
    def check_for_words(self, column_name, words, result_column):
        """
        Check for the presence of certain words in a specified column and add the results to a new column.

        Parameters:
        column_name (str): The name of the column to check for words.
        words (list): The list of words to check for in the column.
        result_column (str): The name of the new column to store the results (True/False).
        """
        word_set = set(words)
    
        self.df[column_name] = self.df[column_name].apply(lambda x: str(x).lower())
        self.df[result_column] = self.df[column_name].apply(
            lambda cell: any(str(word).lower() in cell for word in word_set)
        )
    
    def save_to_csv(self):
        """
        Save the processed DataFrame to a new CSV file.

        Parameters:
        output_file_path (str): The path to the output CSV file.
        """
        self.df.to_csv(self.directory+'/'+self.file, index=False)
