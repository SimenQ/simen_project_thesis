import copy
#Get metrics and segment codes

#Returns list of either the time_series or meta_data metrics
def get_unique_metrics(dict, time_series = None): 
    metrics = set()
    for company_data in dict.values():
        if time_series == True: 
            for metric in company_data["time_series"].keys():
                metrics.add(metric)
        else: 
            for metric in company_data["meta_data"].keys():
                metrics.add(metric)

    return list(metrics)

#Returns company tickers found in the data 
def get_company_tickers(data_dict):
    return list(data_dict.keys())

#Returns the segment_codes for each segment of a company
def get_segment_codes(company_data):
    result = {}
    if company_data and isinstance(company_data, dict):
        for segment_name, segment_info in company_data.items():
            if isinstance(segment_info, dict) and 'Segment_code' in segment_info:
                # Retrieve segment codes
                segment_codes = segment_info.get('Segment_code', [])
                result[segment_name] = {'Segment Code': segment_codes}
    return result

#Returns the metrics found for the segment data
def get_segment_metrics(company_data):
    metrics = set()
    for segment_name, segment_info in company_data.items():
        if isinstance(segment_info, dict):
            # Update the metrics set with keys from each segment
            metrics.update(segment_info.keys())
    return list(metrics)

#Count the number of companies that exist in a dictionary 
def count_num_companies(data_dict):
    print(len(data_dict))

#Count based on 4-digit NAICS code
def count_companies_with_segment_code_NAICS(data, segment_codes):
    count = 0

    for company, company_data in data.items():
        metadata = company_data.get('meta_data', {})
        if any(str(metadata.get('TR.NAICSIndustryGroupAllCode')) == code for code in segment_codes):
            count += 1

    return count

#Count based on 6-digit NAICS code 
def count_companies_with_segment_code_NAICS2(data, segment_codes):
    count = 0

    for company, company_data in data.items():
        metadata = company_data.get('meta_data', {})
        if any(str(metadata.get('TR.NAICSNationalIndustryCode')) == code for code in segment_codes):
            count += 1

    return count

#Count based on 10-digit TRBC code
def count_companies_with_segment_code_TRBC(data, segment_code):
    count = 0

    for company, company_data in data.items():
        metadata = company_data.get('meta_data', {})
        if str(metadata.get('TR.TRBCActivityCode')) == segment_code:
            count += 1

    return count

#Count based on TRBC industry name, for instance segment_name =  "Integrated Oil & gas"    
def count_companies_by_code_name(data, segment_name):
    count = 0

    for company, company_data in data.items():
        metadata = company_data.get('meta_data', {})
        if str(metadata.get('TR.TRBCIndustry')) == segment_name:
            count += 1

    print(count)
