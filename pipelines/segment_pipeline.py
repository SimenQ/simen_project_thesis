

import sys
import pandas as pd

sys.path.append("C:/Users/aminp/OneDrive - NTNU/Dokumenter/NTNU/5år/Projektoppgave/")


from preprocess.file_preprocess.open_and_clean import *
from preprocess.file_preprocess.filter_out_relevant_data import *
from preprocess.file_preprocess.find_and_remove_duplicates import *
from preprocess.file_preprocess.preprocess_to_df import *
from preprocess.configurations.configurations import *
from metricsUtils.calculate_new_metrics import *

def pipeline_segment_data(data_path, company_key, codes): 
    data_dict = open_file(data_path)
    company_dict = data_dict.get(company_key)

    # Check if the company's data is found
    if company_dict is None:
        raise ValueError(f"No data found for company with key '{company_key}'")

    filtered_by_segment_code = filter_segments_by_code(company_dict, codes)
    return filtered_by_segment_code

def pipeline_company_data_NAICS(data_path, codes): 
    data_dict = open_file(data_path)
    cleaned_dict = clean_nested_dict(data_dict)
    removed_221112 = remove_by_naics_code(cleaned_dict, code = "221112")
    relevant_data_naics = filter_companies_by_code(removed_221112, codes, code_type="NAICS")
    updated_data = update_company_data(relevant_data_naics, codes, code_type = "NAICS")
    data_without_integrated_firms = remove_integrated_oil_and_gas(updated_data)
    return data_without_integrated_firms

def pipeline_company_data_TRBC(data_path, codes): 
    data_dict = open_file(data_path)
    #cleaned_dict = clean_nested_dict(data_dict)
    relevant_data_trbc = filter_companies_by_code(data_dict, codes, code_type="TRBC")
    updated_data_trbc = update_company_data(relevant_data_trbc, codes, code_type="TRBC")
    return updated_data_trbc


def segment_data_process(data_path, config_path, company_names, years): 
    company_dict = open_file(data_path)
    config_segments = preprocess_company_data(company_dict, config_path, company_names)
    company_df = preprocess_company_segment_data_to_df(config_segments)
    final_df = calculate_all(company_df, years)
    return final_df


def pureplay_data_process(data, company_names, years): 
    #data_dict = pipeline_company_data_TRBC(data_path, codes)
    config_data = preprocess_company_data(data, company_names)
    company_df = preprocess_company_data_to_df(config_data)
    company_df = company_df.dropna()
    df_new_metrics = calculate_ebita(company_df, years)
    final_df = calculate_ebit_margin(df_new_metrics, years)
    return final_df
   










