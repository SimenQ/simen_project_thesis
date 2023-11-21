
import sys 
from copy import deepcopy
from pandas import Timestamp
import pandas as pd
import json 

sys.path.append("C:/Users/aminp/OneDrive - NTNU/Dokumenter/NTNU/5år/Projektoppgave/")
    
def process_multi_segment_data(data_dict, metrics, segments, years, rename_segments, rename_metrics, company_name):
    new_dict = {}

   
   # 1. Keep only specified segments
    if segments is not None:
        for segment_name, segment_code in segments.items():
            if segment_name in data_dict:
                # Deep copy the segment data
                new_dict[segment_name] = deepcopy(data_dict[segment_name])
                # Set the Segment_code to the one from the config
                new_dict[segment_name]['Segment_code'] = [segment_code]
    else:
        new_dict = {key: deepcopy(value) for key, value in data_dict.items()}

    # 2. Keep only specified metrics for remaining keys
    for key in list(new_dict.keys()):  
        for metric in list(new_dict[key].keys()):  
            if metric not in metrics:
                del new_dict[key][metric]

    # 3. Retain only specific years for time-series values
    for key, metrics_data in new_dict.items():
        for metric, time_series in list(metrics_data.items()):
            filtered_data = [(date, val) for date, val in time_series if date.year in years]
            if not filtered_data:
                del new_dict[key][metric]
            else:
                new_dict[key][metric] = filtered_data

     # 4. Rename the segments and metrics if required, and ensure Segment_code is correctly set
    if rename_segments:
        for old_name, new_name in rename_segments.items():
            if old_name in new_dict:
                # Carry over the data to the new segment name
                new_dict[new_name] = new_dict.pop(old_name)
                # Set the Segment_code for the new segment name based on the configuration
                if old_name in segments:
                    new_dict[new_name]['Segment_code'] = [segments[old_name]]

    #Rename the metrics 
    if rename_metrics:
        for key in new_dict.keys():
            for old_metric, new_metric in rename_metrics.items():
                if old_metric in new_dict[key]:
                    new_dict[key][new_metric] = new_dict[key].pop(old_metric)

    new_dict['company_name'] = company_name

    return new_dict


def process_pureplay_data(data_dict, metrics, years, rename_metrics):
    new_dict = {}

    for company, company_data in data_dict.items():
        new_company_data = {}
        # Process time_series data
        if 'time_series' in company_data:
            time_series_data = company_data['time_series']
            new_time_series_data = {}
            for metric, time_series in time_series_data.items():
                if metric in metrics:
                    filtered_data = [(date, val) for date, val in time_series if date.year in years]
                    if filtered_data:
                        new_time_series_data[metric] = filtered_data
            new_company_data['time_series'] = new_time_series_data

        # Process meta_data
        if 'meta_data' in company_data:
            meta_data = company_data['meta_data']
            new_meta_data = {k: v for k, v in meta_data.items() if k in metrics}
            new_company_data['meta_data'] = new_meta_data

        # Add the processed data to the new dictionary
        new_dict[company] = new_company_data

    # Rename metrics if required
    for company, company_data in new_dict.items():
        for key in ['time_series', 'meta_data']:
            if key in company_data:
                for old_metric, new_metric in rename_metrics.items():
                    if old_metric in company_data[key]:
                        company_data[key][new_metric] = company_data[key].pop(old_metric)

    return new_dict


def preprocess_company_data(data, config_path, company_names):
    processed_data = {}

    with open(config_path, "r") as file: 
        configurations = json.load(file)


    if not isinstance(company_names, list):
        company_names = [company_names]

    for company_name in company_names:
        if company_name in configurations:
            company_config = configurations[company_name]

            # Extract relevant configurations
            company_name_value = company_config.get('name', None)
            segments_to_keep = company_config.get('segments_to_keep', None)
            metrics_to_keep = company_config.get('metrics_to_keep', [])
            years_to_keep = company_config.get('years_to_keep', [])
            segments_rename = company_config.get('segments_rename', None)
            metrics_rename = company_config.get('metrics_rename', {})

            if company_name == 'pureplay':
                processed_data[company_name] = process_pureplay_data(data.get(company_name, {}), metrics_to_keep, years_to_keep, metrics_rename)
            else:
                processed_data[company_name] = process_multi_segment_data(data.get(company_name, {}), metrics_to_keep, segments_to_keep, years_to_keep, segments_rename, metrics_rename, company_name_value)
        else:
            raise ValueError(f"No configuration found for company {company_name}")

    return processed_data
    

    
