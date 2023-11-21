
# Return the duplicates in time-series data for segment data of a company 
def find_duplicates_segment_data(data_dict):
    seen = {}
    duplicates = {}
    
    for segment_info, financials in data_dict.items():
        for financial_statement, values in financials.items():
            # Skip if it's the Segment Code entry
            if financial_statement == 'Segment Code':
                continue
            for date, _ in values:
                # Convert Timestamp object to string for simplicity
                key = (segment_info, financial_statement, str(date))
                if key in seen:
                    if key not in duplicates:
                        duplicates[key] = [seen[key]]
                    duplicates[key].append((date, _))
                else:
                    seen[key] = (date, _)
                    
    return duplicates

#Return the duplicates in the time-series data for "pure-play" companies 
def find_time_series_duplicates(data_dict):
    duplicates = {}
    
    for company, company_data in data_dict.items():
        # Focus only on the time_series part of the data
        time_series = company_data.get('time_series', {})
        
        seen = {}
        company_duplicates = {}
        
        for statement, date_values in time_series.items():
            for date, value in date_values.items():
                key = (statement, str(date))
                if key in seen:
                    if key not in company_duplicates:
                        company_duplicates[key] = [seen[key]]
                    company_duplicates[key].append(value)
                else:
                    seen[key] = value
                    
        if company_duplicates:
            duplicates[company] = company_duplicates

    return duplicates

# If duplicates is found this function will keep the first instance and remove the other one 
def remove_duplicates_from_segment_data(data_dict): 
    duplicates = find_duplicates_segment_data(data_dict)
    for segment_info, financials in data_dict.items(): 
        for key, values in financials.items(): 
            if key == "Segment Code": 
                continue

            new_values = []
            for date, value in values: 
                keys = (segment_info, key, str(date))
                if keys in duplicates and (date, value) != duplicates[keys][0]: 
                    continue
                new_values.append((date, value))

            data_dict[segment_info][key] = new_values

    return data_dict