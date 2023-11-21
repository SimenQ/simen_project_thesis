
import copy
import pandas as pd
import datetime

from preprocess.file_preprocess.open_and_clean import open_file

#Removes those segments that does have BUS,ASR ending (which are the relevant ones)
def remove_non_bus_asr_metrics(data): 
    for segments, metrics in data.items(): 
        keys_to_remove = [key for key in metrics if not (key.endswith('(BUS,ASR)') or key == 'Segment Code')]
        for key in keys_to_remove:
            del metrics[key]
    return data

#Removes those company segments that does not have segment code reported and the irrelevant segments
def filter_segments_by_code(company_data, df):
    valid_prefixes = df['Code'].astype(str).tolist()
    segments_to_remove = []

    # Iterate through each segment in the company's data
    for segment_key, segment_info in company_data.items():
        if isinstance(segment_info, dict) and 'Segment_code' in segment_info:
            segment_codes = segment_info['Segment_code']
            valid = any(code.startswith(tuple(valid_prefixes)) for code in segment_codes)

            if not valid:
                segments_to_remove.append(segment_key)

    # Remove segments that don't have a valid code
    for segment in segments_to_remove:
        company_data.pop(segment)

    return company_data

#Filters out "pure-play" companies which is in the list of valid codes either NAICS or TRBC based on the codes inputed
def filter_companies_by_code(data_dict, df, code_type):

    data_copy = copy.deepcopy(data_dict)

    if code_type == "NAICS":
        valid_codes = set(df['Code'].astype(str).values)
        code_key_1 = 'TR.NAICSIndustryGroupCode'
        code_key_2 = 'TR.NAICSNationalIndustryCode'
    elif code_type == "TRBC":
        valid_codes = set(df['Code'].astype(str).values)
        code_key_1 = 'TR.TRBCIndustryCode'
        code_key_2 = None  
    else:
        raise ValueError("Input dataframe does not contain recognized code column.")

    companies_to_remove = []

    for ticker, company_data in data_copy.items():
        code_1 = company_data['meta_data'].get(code_key_1)
        code_2 = company_data['meta_data'].get(code_key_2) if code_key_2 else None

        if not (code_1 in valid_codes or (code_2 and code_2 in valid_codes)):
            companies_to_remove.append(ticker)

    for company in companies_to_remove:
        data_copy.pop(company)

    return data_copy

#This function updates the dictionary with which segment Up,mid,down or ren the company belongs to based on either NAICS or TRBC codes
def update_company_data(companies, code_dataframe, code_type):
    updated_data = companies.copy()
    
    # Convert the dataframe to a lookup dictionary with string codes
    code_lookup = code_dataframe.astype({'Code': 'str'}).set_index('Code').to_dict(orient='index')
    
    if code_type == 'NAICS':
        code_key_1 = 'TR.NAICSIndustryGroupCode'
        code_key_2 = 'TR.NAICSNationalIndustryCode'
        description_key = 'NAICS_Description'
    elif code_type == 'TRBC':
        code_key_1 = 'TR.TRBCIndustryCode'
        code_key_2 = None
        description_key = 'TRBC_Description'
    else:
        raise ValueError("Invalid code_type. Expected 'NAICS' or 'TRBC'.")

    for ticker, data in updated_data.items():
        # Fetch the industry codes from the company's metadata
        code1 = data['meta_data'].get(code_key_1, None)
        code2 = data['meta_data'].get(code_key_2, None) if code_key_2 else None

        # Try the first code
        matched_info = code_lookup.get(code1, None)

        # If no match and there is a second code, try the second code
        if not matched_info and code2:
            matched_info = code_lookup.get(code2, None)

        if matched_info:
            segment_type = matched_info['Segment_type']
            description = matched_info['Description']

            data['meta_data']['Segment_type'] = segment_type
            data['meta_data'][description_key] = description

    return updated_data


#The NAICS code 221122 under Renweables is actually Fosil fuel 
def remove_by_naics_code(data, code='221112'):
    data_copy = copy.deepcopy(data)
    keys_to_delete = []

    # Identify the keys for deletion
    for key, value in data_copy.items():
        if value['meta_data'].get('TR.NAICSNationalIndustryCode') == code:
            keys_to_delete.append(key)

    # Delete identified keys
    for key in keys_to_delete:
        del data_copy[key]

    return data_copy

#Since the NAICS codes does not a sector code for integrated companies, we need to remove them from the upstream segment if we use NAICS
def remove_integrated_oil_and_gas(data):
    data_copy = copy.deepcopy(data)
    keys_to_delete = []

    # Identify the keys for deletion
    for key, value in data_copy.items():
        if value['meta_data'].get('TR.TRBCIndustry') == 'Integrated Oil & Gas':
            keys_to_delete.append(key)

    # Delete identified keys
    for key in keys_to_delete:
        del data_copy[key]

    return data_copy

#Extract each segment type as their own dictionaries 
def extract_by_segment(data, segment_type):
    new_dict = {}
    
    for key, value in data.items():
        if value['meta_data'].get('Segment_type') == segment_type:
            new_dict[key] = value
    
    return new_dict


#Give a list of the pureplay segment you want as segment-list and this function extract those segments into a dictionary where the segment-name is key
def extract_segments(data_path, segment_list): 
    data = open_file(data_path)
    segments_dict = {}
    for segment in segment_list: 
        segments_dict[segment] = extract_by_segment(data, segment) 
    return segments_dict

#Transforms the multisegment-data by multuplying the values by 1000 and changing from datetime to timestamps
def transform_segment_data(data):
    for company_data in data.values():
        for key, value in company_data.items():
            # Check if the value is a dictionary, then process its items
            if isinstance(value, dict):
                for inner_key, inner_value in value.items():
                    if isinstance(inner_value, list):
                        new_list = []
                        for item in inner_value:
                            if isinstance(item, tuple) and len(item) == 2:
                                dt, num = item
                                if isinstance(dt, datetime.datetime):
                                    dt = pd.Timestamp(dt)
                                if isinstance(num, (int, float)):
                                    num *= 1000
                                new_list.append((dt, num))
                            else:
                                new_list.append(item)
                        value[inner_key] = new_list
    return data

#Modify the key to be company ticker, filters on 2022 and removes those empty segments 
def extract_relevant_data_from_segment(data):
    # Creating a deep copy of the original dictionary
    new_data = copy.deepcopy(data)

    for key, value in data.items():
        # Extracting the ticker from the key
        ticker = key[key.find("(") + 1 : key.find(")")]

        # Replace the key with the ticker, keeping the structure same
        new_data[ticker] = new_data.pop(key)

        for segment, segment_data in list(new_data[ticker].items()):
            if isinstance(segment_data, dict):
                all_empty = True
                for inner_key, inner_value in segment_data.items():
                    if isinstance(inner_value, list) and inner_key != "Segment_code":
                        # Filtering for 2022 data
                        filtered_list = [
                            item for item in inner_value
                            if isinstance(item, tuple) and len(item) == 2 and item[0].year == 2022
                        ]
                        new_data[ticker][segment][inner_key] = filtered_list

                        # Check if there's any non-empty list
                        if filtered_list:
                            all_empty = False
                
                # If all metrics are empty lists, remove the segment
                if all_empty:
                    del new_data[ticker][segment]

    return new_data

    
   