import pandas as pd
import numpy as np

def calculate_revenue(df, years):
    for year in years:
        revenue_col = f'Revenue_{year}'
        external_revenue_col = f'External_Revenue_{year}'
        intersegment_revenue_col = f'Intersegment_Revenue_{year}'

        # Ensure the column exists, if not create it filled with NaN
        if revenue_col not in df.columns:
            df[revenue_col] = np.nan

        # Calculate Revenue for each row where it is NaN and we have both external and intersegment revenues
        both_revenues_condition = df[external_revenue_col].notna() & df[intersegment_revenue_col].notna()
        df.loc[both_revenues_condition, revenue_col] = df.loc[both_revenues_condition, external_revenue_col] + df.loc[both_revenues_condition, intersegment_revenue_col]

        # If Revenue is still NaN and we have external revenue, use that as the Revenue
        only_external_condition = df[revenue_col].isna() & df[external_revenue_col].notna()
        df.loc[only_external_condition, revenue_col] = df.loc[only_external_condition, external_revenue_col]

    return df

def calculate_ebit_margin(df, years): 
    for year in years: 
        revenue_col = f"Revenue_{year}"
        ebit_col = f"EBIT_{year}"

        # Check if both columns exist
        if revenue_col in df.columns and ebit_col in df.columns:
            # Loop through each row to perform the calculation
            for i in df.index:
                revenue = df.loc[i, revenue_col]
                ebit = df.loc[i, ebit_col]

                # Calculate EBIT margin if revenue is not zero
                if revenue != 0:
                    df.loc[i, f"EBIT_margin_{year}"] = ebit / revenue
                else:
                    # Set EBIT margin to 0 if revenue is zero
                    df.loc[i, f"EBIT_margin_{year}"] = 0

    return df

def calculate_ebita(df, years): 
    for year in years:
        ebit_col = f'EBIT_{year}'
        da_col = f"D&A_{year}"

        # Check if both columns exist
        if ebit_col in df.columns and da_col in df.columns: 
            df[f"EBITDA_{year}"] = df[ebit_col] + df[da_col]
            
    return df 

#If capex is not given one way is to calculate it in this way 
def calculate_capex(df, years):
    for i in range(len(years)-1):
        year = years[i]
        next_year = years[i + 1]
        total_assets_col_current = f"Total_Non-Current_Assets_{year}"
        total_assets_col_next = f"Total_Non-Current_Assets_{next_year}"
        da_col_next = f"D&A_{next_year}"

        # Check if necessary columns exist
        if total_assets_col_current in df.columns and total_assets_col_next in df.columns and da_col_next in df.columns:
            df[f"CapEx_{next_year}"] = (df[total_assets_col_next] - df[total_assets_col_current]) + df[da_col_next]

    # Drop Total_Non_Current_Assets columns
    for year in years:
        total_assets_col = f"Total_Non-Current_Assets_{year}"
        if total_assets_col in df.columns:
            df.drop(total_assets_col, axis=1, inplace=True)

    return df

def calculate_all(df, years):
    df = calculate_revenue(df, years)
    df = calculate_ebit_margin(df, years)
    df = calculate_ebita(df, years)
    df = calculate_capex(df, years)
    return df