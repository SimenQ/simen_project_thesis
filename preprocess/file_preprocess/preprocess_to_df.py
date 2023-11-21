
import pandas as pd

#Returns a dataframe of the time_series data for "pure-play" companies based on given features and specified years
def preprocess_company_data_to_df(d):
    df_data = []
    
    for company, company_data in d.items():
        # Process time_series data
        if 'time_series' in company_data:
            for financial_statement, time_series_data in company_data['time_series'].items():
                for date, value in time_series_data:
                    column_name = financial_statement.replace(' ', '_') + '_' + str(date.year)
                    df_data.append((company, column_name, value))
                    
        # Process meta_data
        if 'meta_data' in company_data:
            for meta_key, meta_value in company_data['meta_data'].items():
                meta_col_name = meta_key.replace(' ', '_')
                df_data.append((company, meta_col_name, meta_value))
    
    df = pd.DataFrame(df_data, columns=['Company', 'Financial Metric', 'Value'])

    # Pivoting the DataFrame
    df_pivot = df.pivot_table(index='Company', 
                              columns='Financial Metric', 
                              values='Value', 
                              aggfunc='first')
    df_pivot.columns.name = None
    df_pivot.index.name = None
    return df_pivot

#Return a dataframe for the segment data 
def preprocess_company_segment_data_to_df(d, years=None):
    df_data = []
    segment_codes = []

    if years is not None:
        years = set(years)

    for company_key, company_data in d.items():
        company_name = company_data.get('company_name', 'Unknown')
        for segment, values in company_data.items():
            if segment == 'company_name':
                continue
            
            # Handle Segment_code separately
            segment_code = values.get('Segment_code', ['Unknown'])[0]  # Assuming segment_code is a list with one item
            segment_codes.append((company_key, company_name, segment, segment_code))
            
            for financial_statement, time_series_data in values.items():
                if financial_statement == 'Segment_code':
                    continue  # Skip Segment_code as it's already handled
                
                if isinstance(time_series_data, list):
                    for item in time_series_data:
                        if isinstance(item, tuple) and len(item) == 2:
                            date, value = item
                            if years is None or date.year in years:
                                column_name = financial_statement.replace(' ', '_') + '_' + str(date.year)
                                df_data.append((company_key, company_name, segment, column_name, value))

    # Create DataFrame for financial metrics
    df_metrics = pd.DataFrame(df_data, columns=['Company Key', 'Company Name', 'Segment', 'Financial Metric', 'Value'])
    df_pivot_metrics = df_metrics.pivot_table(index=['Company Key', 'Company Name', 'Segment'], 
                                              columns='Financial Metric', 
                                              values='Value', 
                                              aggfunc='first')

    # Create DataFrame for Segment_code
    df_codes = pd.DataFrame(segment_codes, columns=['Company Key', 'Company Name', 'Segment', 'Segment_code'])
    df_pivot_codes = df_codes.set_index(['Company Key', 'Company Name', 'Segment'])

    # Join the metrics and codes DataFrames
    df_pivot = df_pivot_metrics.join(df_pivot_codes, how='left')

    # Reorder the columns to put Segment_code first
    cols = list(df_pivot.columns)
    cols.insert(0, cols.pop(cols.index('Segment_code')))
    df_pivot = df_pivot[cols]

    df_pivot.reset_index(inplace=True)
    
    return df_pivot



