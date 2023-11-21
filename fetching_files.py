import pandas as pd
import eikon as ek
import time

def fetch_data_in_batches(tickers, batch_size, file_output_name, year=None):
    if year is None:
        year = "FY0"
    else:
        year = "FY" + str(year)

    def get_data_batch(batch):
        try:
            data, _ = ek.get_data(
                batch,
                fields=[
                    f"TR.Revenue(SDate={year},Frq=FY,Curn=USD)",
                    f"TR.PriceClose(SDate={year},Frq=FY,Curn=USD)",
                    f'TR.EpsSmartEst(SDate={year},Period=FY1,Curn=USD)',
                     f"TR.EpsSmartEst(SDate={year},Period=FY2,Curn=USD)",
                    f"TR.EpsSmartEst(SDate={year},Period=FY3,Curn=USD)",
                    f"TR.ROEActValue(SDate={year},Period=FY0)",
                    f"TR.CompanyMarketCap(SDate={year},Curn=USD)",
                    f'TR.F.ComShrOutsTot(Frq=FY,SDate={year})',
                    f'TR.F.TotAssets(SDate={year},Frq=FY,Curn=USD)',
                    f'TR.F.IntrExpn(SDate={year},Frq=FY,Curn=USD)',
                    f'TR.F.TotLiab(SDate={year},Frq=FY,Curn=USD)',
                    f'TR.F.DivPayoutRatioPct(SDate={year},Period=FY0)',
                    f'TR.F.IntrBearLiabTot(Frq=FY,SDate={year},Curn=USD)',
                    f'TR.F.ShHoldEqCom(SDate={year},Frq=FY,Curn=USD)',
                    f'TR.F.TotCap(SDate={year},Frq=FY,Curn=USD)',
                    f'TR.TotalDebtOutstanding(SDate={year},Frq=FY,Curn=USD)',
                    f'TR.LTGMean(SDate={year},Frq=FY)',
                    f'TR.F.ReturnAvgComEqPctTTM(SDate={year},Period=FI0)',
                    f"TR.TRBCEconomicSector",
                    f"TR.TRBCBusinessSector",
                    f"TR.TRBCIndustryGroup",
                    f"TR.TRBCIndustry",
                    f"TR.TRBCActivity",
                    f"TR.TRBCEconSectorCode",
                    f"TR.TRBCBusinessSectorCode",
                    f"TR.TRBCIndustryGroupCode",
                    f"TR.TRBCIndustryCode",
                    f"TR.TRBCActivityCode",
                    f"TR.F.DeprDeplAmortTot(SDate={year},Period=FY0,Segcode=TRBC,Curn=USD)",
                    f"TR.WACCTaxRate",
                    f"TR.F.EBIT(SDate={year},Frq=FY,Curn=USD)",
                    f"TR.HeadquartersCountry",
                    f"TR.F.OpProfBefNonRecurIncExpn(SDate={year},Frq=FY,Curn=USD)",
                    f"TR.F.CAPEXTot(SDate={year},Period=FY0,Curn=USD)"
                ]
            )
            #add a column with the year
            data["year"] = year
            return data  # Return only the DataFrame
        except Exception as e:
            print(f"Failed to fetch data for batch: {batch}. Error: {e}")
            return None  # Return None in case of failure

    all_data_frames = []
    failed = []

    # Split tickers into batches and process them
    for i in range(0, len(tickers), batch_size):
        time.sleep(0.2)
        print("Getting data for batch number:", i // batch_size, "of", len(tickers) // batch_size)
        batch = tickers[i:i + batch_size]
        batch_data = get_data_batch(batch)
        
        if batch_data is not None:
            all_data_frames.append(batch_data)
        else:
            failed.append(batch)

    # Concatenate all the individual batch data frames into one
    final_data_frame = pd.concat(all_data_frames, ignore_index=True) if all_data_frames else pd.DataFrame()

    # Save the data to a file (adjust the path and file name as needed)
    final_data_frame.to_csv(file_output_name, index=False)
    
    return final_data_frame, failed


import pandas as pd
import eikon as ek
import time

def fetch_data_in_batches(tickers, batch_size, file_output_name, year=None):
    if year is None:
        year = "FY0"
    else:
        year = "FY" + str(year)

    def get_data_batch(batch):
        try:
            data, _ = ek.get_data(
                batch,
                fields=[
                    f"TR.Revenue(SDate={year},Frq=FY,Curn=USD)",
                    f"TR.PriceClose(SDate={year},Frq=FY,Curn=USD)",
                    f'TR.EpsSmartEst(SDate={year},Period=FY1,Curn=USD)',
                     f"TR.EpsSmartEst(SDate={year},Period=FY2,Curn=USD)",
                    f"TR.EpsSmartEst(SDate={year},Period=FY3,Curn=USD)",
                    f"TR.ROEActValue(SDate={year},Period=FY0)",
                    f"TR.CompanyMarketCap(SDate={year},Curn=USD)",
                    f'TR.F.ComShrOutsTot(Frq=FY,SDate={year})',
                    f'TR.F.TotAssets(SDate={year},Frq=FY,Curn=USD)',
                    f'TR.F.IntrExpn(SDate={year},Frq=FY,Curn=USD)',
                    f'TR.F.TotLiab(SDate={year},Frq=FY,Curn=USD)',
                    f'TR.F.DivPayoutRatioPct(SDate={year},Period=FY0)',
                    f'TR.F.IntrBearLiabTot(Frq=FY,SDate={year},Curn=USD)',
                    f'TR.F.ShHoldEqCom(SDate={year},Frq=FY,Curn=USD)',
                    f'TR.F.TotCap(SDate={year},Frq=FY,Curn=USD)',
                    f'TR.TotalDebtOutstanding(SDate={year},Frq=FY,Curn=USD)',
                    f'TR.LTGMean(SDate={year},Frq=FY)',
                    f'TR.F.ReturnAvgComEqPctTTM(SDate={year},Period=FI0)',
                    f"TR.TRBCEconomicSector",
                    f"TR.TRBCBusinessSector",
                    f"TR.TRBCIndustryGroup",
                    f"TR.TRBCIndustry",
                    f"TR.TRBCActivity",
                    f"TR.TRBCEconSectorCode",
                    f"TR.TRBCBusinessSectorCode",
                    f"TR.TRBCIndustryGroupCode",
                    f"TR.TRBCIndustryCode",
                    f"TR.TRBCActivityCode",
                    f"TR.F.DeprDeplAmortTot(SDate={year},Period=FY0,Segcode=TRBC,Curn=USD)",
                    f"TR.WACCTaxRate",
                    f"TR.F.EBIT(SDate={year},Frq=FY,Curn=USD)",
                    f"TR.HeadquartersCountry",
                    f"TR.F.OpProfBefNonRecurIncExpn(SDate={year},Frq=FY,Curn=USD)",
                    f"TR.F.CAPEXTot(SDate={year},Period=FY0,Curn=USD)",
                    f"TR.TRESGCScoreGrade(SDate={year},Period=FY0)"
                ]
            )
            #add a column with the year
            data["year"] = year
            return data  # Return only the DataFrame
        except Exception as e:
            print(f"Failed to fetch data for batch: {batch}. Error: {e}")
            return None  # Return None in case of failure
        
    all_data_frames = []
    batches_to_process = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]
    failed = []

    while batches_to_process:
        print("Number of batches to process:", len(batches_to_process)+1)
        new_failed = []
        for batch in batches_to_process:
            print("Getting data for batch number:", batches_to_process.index(batch)+1, "of", len(batches_to_process)+1)
            time.sleep(0.2)
            print("Processing batch")
            batch_data = get_data_batch(batch)
            
            if batch_data is not None:
                print("batch successful")
                all_data_frames.append(batch_data)
            else:
                print("batch failed")
                new_failed.append(batch)

        # Update the list of batches to process
        batches_to_process = new_failed

        # Remember the failed batches
        failed.extend(new_failed)

    # Concatenate all the individual batch data frames into one
    final_data_frame = pd.concat(all_data_frames, ignore_index=True) if all_data_frames else pd.DataFrame()

    # Save the data to a file (adjust the path and file name as needed)
    final_data_frame.to_csv(file_output_name, index=False)
    
    return final_data_frame, failed
