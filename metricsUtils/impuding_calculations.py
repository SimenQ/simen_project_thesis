
import pandas as pd
import numpy as np 

from firm import Firm
from pipelines.segment_pipeline import *

from firm import Firm
TRBC_MAPPINGS = {
    'TRBC Activity Code': 'TRBC Activity Name',
    'TRBC Industry Code': 'TRBC Industry Name',
    'TRBC Industry Group Code': 'TRBC Industry Group Name',
    'TRBC Business Sector Code': ' TRBC Business Sector Name',
    'TRBC Economic Sector Code': 'TRBC Economic Sector Name'
}

def calculate_median_industry_roe(df, TRBC_code, TRBC_mapping = TRBC_MAPPINGS): 
    df['Return On Equity - Actual'] = df['Return On Equity - Actual'].fillna(df['Return on Average Common Equity - %, TTM'])
    industry_name = TRBC_mapping.get(TRBC_code)
    median_roe_by_trbc = df.groupby([TRBC_code])['Return On Equity - Actual'].median()
    return median_roe_by_trbc.reset_index()

def industry_median(df, financial_metric, TRBC_code, TRBC_mapping = TRBC_MAPPINGS): 
    # Create a copy of the DataFrame to avoid SettingWithCopyWarning
    df_cleaned = df[df[financial_metric] != 0].copy()

    df_cleaned["Tot_Cap"] = df_cleaned["Company Market Cap"] + df_cleaned["Total Debt"] 
    mask = (df_cleaned["Tot_Cap"] != 0) & (df_cleaned[financial_metric] != 0)
    df_cleaned.loc[mask, "Ratio"] = df_cleaned["Tot_Cap"] / df_cleaned[financial_metric]

    industry_field = TRBC_mapping.get(TRBC_code, None)
    
    median_ratios = df_cleaned.groupby([TRBC_code]).agg(
        Median_ratio = ("Ratio", "median"),
        Num_Companies = ("Ratio", "size")
    ).reset_index()

    return median_ratios

def fill_nan_wacc_tax_rate(df):
    df_copy = df.copy()

    # Calculate the mode for each 'Country of Headquarters'
    mode_wacc_tax_rate = df_copy.groupby('Country of Headquarters')['WACC Tax Rate, (%)'].transform(lambda x: x.mode().max() if not x.mode().empty else np.nan)

    # Fill NaN in 'WACC Tax Rate, (%)' with the mode calculated for each country
    df_copy['WACC Tax Rate, (%)'] = df_copy['WACC Tax Rate, (%)'].fillna(mode_wacc_tax_rate)

    return df_copy

def calculate_industry_median_wacc(df, industry_code, TRBC_mapping = TRBC_MAPPINGS):
    industry_name = TRBC_mapping.get(industry_code, None)
    median_wacc = df.groupby([industry_code, industry_name])['WACC'].median().reset_index()
    return median_wacc

def create_firm_objects(firm_df, roe_df, TRBC_code, TRBC_mapping = TRBC_MAPPINGS):
    firm_objects = {}
    for index, row in firm_df.iterrows():
        # Match the TRBC code with the industry code
        industry_code = row[TRBC_code]
        industry_row = roe_df[roe_df[TRBC_code] == industry_code]

        if not industry_row.empty:
            # Extract the ROE from the industry dataframe
            industry_roe = industry_row.iloc[0]['Return On Equity - Actual']

            # Prepare parameters for Firm object, handle missing columns with default None
            forecasted_eps_3 = row['Earnings Per Share - SmartEstimate®.2'] if 'Earnings Per Share - SmartEstimate®.2' in row and not pd.isna(row['Earnings Per Share - SmartEstimate®.2']) else None
            ltg = row['Long Term Growth - Mean'] if 'Long Term Growth - Mean' in row and not np.isnan(row['Long Term Growth - Mean']) else None

            # Create a Firm object
            firm_object = Firm(
                name=row['Instrument'],
                industry=industry_code,
                stock_price=row['Price Close'],
                dividend_payout_ratio=row['Dividend Payout Ratio - %'],
                total_assets=row['Total Assets'],
                shares_outstanding=row['Common Shares - Outstanding - Total'],
                industry_roe=industry_roe,
                interest_bearing_debt=row["Interest Bearing Liabilities - Total"],
                total_capital=row["Total Capital"],
                total_debt=row['Total Debt'],
                total_equity=row['Shareholders Equity - Common'],
                interest_expense=row["Interest Expense"],
                forecasted_eps_1=row['Earnings Per Share - SmartEstimate®'],
                forecasted_eps_2=row['Earnings Per Share - SmartEstimate®.1'],
                forecasted_eps_3=forecasted_eps_3,
                ltg=ltg,
                tax_rate=row["WACC Tax Rate, (%)"]
            )

            # Add the Firm object to the list
            firm_objects[row['Instrument']] = firm_object
    
    return firm_objects


def imputed_segment_coc(segment_df, industry_df, TRBC_codes, financial_metric, year):
    # Ensure the DataFrame has the Segment_code as a column
    # Slice the Segment_code based on the TRBC level
    segment_df["Segment_code"] = segment_df["Segment_code"].str.slice(0, {
        'TRBC Industry Code': 8,
        'TRBC Industry Group Code': 6,
        'TRBC Business Sector Code': 4,
        'TRBC Economic Sector Code': 2
    }[TRBC_codes])

    # Create an industry DataFrame indexed by TRBC codes for fast lookup
    industry_df[TRBC_codes] = industry_df[TRBC_codes].astype(str)
    industry_df_copy = industry_df.set_index(TRBC_codes)

    # Initialize a dictionary to store the company dataframes and WACC for each company
    company_dfs = {}
    imputed_wacc_data = []

    for (company_name, company_key), group in segment_df.groupby(['Company Name', 'Company Key']):
        output_data = []

        for index, row in group.iterrows():
            code = row["Segment_code"]
            financial_metric_col = f'{financial_metric}_{year}'

            # Get the median ratio if the code exists in the industry df
            median_ratio = industry_df_copy.at[code, "Median_ratio"] if code in industry_df_copy.index else np.nan

            # Calculate the imputed value
            imputed_val = row[financial_metric_col] * median_ratio if pd.notna(median_ratio) else np.nan

            output_data.append({
                "Segment": row['Segment'],
                "Segment_code": code,
                financial_metric_col: row[financial_metric_col],
                "Median_ratio": median_ratio,
                "Imputed_value": imputed_val
            })

        # Create a DataFrame for the output data
        company_df = pd.DataFrame(output_data)
        company_df = company_df.merge(industry_df_copy[['WACC']], left_on='Segment_code', right_index=True, how='left')

        # Store the company_df in the dictionary
        company_dfs[company_name] = company_df

        # Calculate the total imputed value and the imputed weighted average WACC for the company
        total_imputed_value = company_df['Imputed_value'].sum()
        imputed_weighted_average_wacc = ((company_df['Imputed_value'] * company_df['WACC']).sum() / total_imputed_value
                                         if total_imputed_value != 0 else np.nan)
        
        # Append the result to the list
        imputed_wacc_data.append({
            "Company Key": company_key,
            "Company Name": company_name,
            "Imputed_Weighted_Average_WACC": imputed_weighted_average_wacc
        })

    # Convert the list to a DataFrame
    imputed_wacc_df = pd.DataFrame(imputed_wacc_data)

    return imputed_wacc_df, company_dfs


def return_final_dfs(data_path, config_path, company_names, impuded_df, TRBC_code, year, years, financial_metric, roe_estimates, segment_estimates): 
    segment_data = segment_data_process(data_path, config_path, company_names, years)
    impuded_wacc, company_dfs = imputed_segment_coc(segment_data, impuded_df, TRBC_code, financial_metric, year)
    firm_objects = create_firm_objects(segment_estimates, roe_estimates, TRBC_code)

    # Initialize the column to NaN so that all values are set up
    impuded_wacc["Impuded_wacc_firm_whole"] = np.nan

    for firm in company_names: 
        obj = firm_objects[firm]
        wacc = obj.calculate_gls_cost_of_capital()

        # Use .loc to find the row(s) where the 'Company Name' matches 'firm' and set the 'Impuded_wacc_firm_whole'
        impuded_wacc.loc[impuded_wacc['Company Key'] == firm, "Impuded_wacc_firm_whole"] = wacc

    return impuded_wacc, company_dfs

    
