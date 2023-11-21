import pickle
import pandas as pd
import copy

def open_file(path): 
    with open (path, "rb") as file: 
        data = pickle.load(file)
        return data
    
def save_file(path, data): 
    with open(path, "wb") as file: 
        pickle.dump(data, file)

def clean_nested_dict(data):
    data_copy = copy.deepcopy(data)

    for company, company_data in data_copy.items():

        # Clean time_series part
        time_series = company_data.get('time_series', {})
        for metric, metric_values in time_series.items():
            timestamps_to_remove = [timestamp for timestamp, value in metric_values.items() if pd.isna(value) or value == '' or value == "NaN"]
            for timestamp in timestamps_to_remove:
                del metric_values[timestamp]

        # Clean meta_data part
        meta_data = company_data.get('meta_data', {})
        keys_to_remove = [key for key, value in meta_data.items() if pd.isna(value) or value == '' or value == "NaN"]
        for key in keys_to_remove:
            del meta_data[key]
                
    return data_copy


def clean_company_segment_dict(data): 
    clean_data = {}
    for key, value in data.items():
        if isinstance(value, dict):
            clean_data[key] = clean_company_segment_dict(value)
        elif isinstance(value, list):
            clean_data[key] = [item for item in value if not (pd.isna(item[1]) or item[1] == '')]
        else:
            clean_data[key] = value
            
    return clean_data 