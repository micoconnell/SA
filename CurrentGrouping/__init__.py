# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt
from datetime import datetime, timedelta
import logging
import requests
import pandas as pd
import re
from io import StringIO
from typing import BinaryIO
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import pytz
import numpy as np



def main(name: str) -> str:
    
    
    blob_connection_Pulls = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_client = blob_connection_Pulls.get_container_client("supplycushionmetrics")
    blob_client1 = container_client.get_blob_client("Current1.csv")
    blob_client2 = container_client.get_blob_client("Current2.csv")
    blob_client3 = container_client.get_blob_client("Current3.csv")
    blob_client4 = container_client.get_blob_client("Current4.csv")
    blob_client5 = container_client.get_blob_client("Current5.csv")
    blob_client6 = container_client.get_blob_client("Current6.csv")
    blob_client7 = container_client.get_blob_client("Current7.csv")
    blob_client8 = container_client.get_blob_client("Current8.csv")
    blob_client9 = container_client.get_blob_client("Current9.csv")

    dfName1  = blob_client1.download_blob()  
    dfName1 = pd.read_csv(dfName1)


    dfName2  = blob_client2.download_blob()  
    dfName2 = pd.read_csv(dfName2)

    dfName3  = blob_client3.download_blob()  
    dfName3 = pd.read_csv(dfName3)

    
    dfName4  = blob_client4.download_blob()  
    dfName4 = pd.read_csv(dfName4)

    dfName5  = blob_client5.download_blob()  
    dfName5 = pd.read_csv(dfName5)

    
    
    dfName6  = blob_client6.download_blob()  
    dfName6 = pd.read_csv(dfName6)

    
    
    dfName7  = blob_client7.download_blob()  
    dfName7 = pd.read_csv(dfName7)

    
    dfName8  = blob_client8.download_blob()  
    dfName8 = pd.read_csv(dfName8)
    
    
    dfName9  = blob_client9.download_blob()  
    dfName9 = pd.read_csv(dfName9)
  
    
    column_names = [
        ["Date","CoalDCR","HydroDCR","GasDCR","EnergyDCR","SolarDCR"],
        ["Date","WindDCR","DualFuelDCR","OtherDCR","Dispatched Contingency ReserveGen"],
        ["Date",'AlbertaTotalNetGeneration','SolarGen','BCMATLIMPORTATC','BCMATLEXPORTATC','SASKIMPORTATC'],
        ["Date",'SASKEXPORTATC','SASKEXPORTGROSSOFFERS','SASKIMPORTGROSSOFFERS'],
        ["Date",'BCMATLEXPORTGROSSOFFERS','BCMATLIMPORTGROSSOFFERS'],
        ["Date",'ContingencyRequired','AIL','CoalGen','GasGen','DualGen'],
        ["Date",'HydroGen','OtherGen','StorageGen','WindGen'],
        ["Date",'NetInterchange','NetCogen','NetCombinedCycle','NetGasFiredSteam','NetSimpleCycle'],
        ["Date",'LSSiArmed','LSSiOffered'] ]
    
    
    dfName1.columns = column_names[0]
    dfName2.columns = column_names[1]
    dfName3.columns = column_names[2]
    dfName4.columns = column_names[3]
    dfName5.columns = column_names[4]
    dfName6.columns = column_names[5]
    dfName7.columns = column_names[6]
    dfName8.columns = column_names[7]
    dfName9.columns = column_names[8]
    dfName1 = dfName1.set_index(['Date'],drop=True)
    dfName2 = dfName2.set_index(['Date'],drop=True)
    dfName3 = dfName3.set_index(['Date'],drop=True)
    dfName4 = dfName4.set_index(['Date'],drop=True)
    dfName5 = dfName5.set_index(['Date'],drop=True)
    dfName6 = dfName6.set_index(['Date'],drop=True)
    dfName7 = dfName7.set_index(['Date'],drop=True)
    dfName8 = dfName8.set_index(['Date'],drop=True)
    dfName9 = dfName9.set_index(['Date'],drop=True)

    def process_dataframe(df):
        def get_this_hour_date_str():
            calgary_tz = pytz.timezone('America/Edmonton')
            this_hour = datetime.now(calgary_tz)
            this_hour = this_hour.replace(second=0, microsecond=0)  # Round down to the nearest hour
            return this_hour.strftime("%m/%d/%Y %H:%M:%S")
        last_hour = get_this_hour_date_str()
        
        df = df
        # Ensure the index is a datetime index
        #df = df.set_index(['Date'],drop=True)
        df.index = pd.to_datetime(df.index)
        df.replace('', np.nan, inplace=True)
        # Forward fill NaN values in the columns
        df_filled = df.fillna(method='ffill')

        # If the first row still has NaN values, use backward fill (bfill) to fill them
        if df_filled.iloc[0].isnull().any():
            df_filled = df_filled.bfill()

        # Resample to hourly frequency and compute the average
        df_hourly = df_filled.resample('5Min').mean()
        df_hourly = df_hourly[df_hourly.index <= pd.to_datetime(last_hour)]
        df_hourly.replace('', np.nan, inplace=True)
        df_hourly = df_hourly.fillna(method='ffill')
        print(df_hourly)
        return df_hourly

    
    
    
    dfName1 = process_dataframe(dfName1)
    dfName2 = process_dataframe(dfName2)
    dfName3 = process_dataframe(dfName3)
    dfName4 = process_dataframe(dfName4)
    dfName5 = process_dataframe(dfName5) 
    dfName6 = process_dataframe(dfName6) 
    dfName7 = process_dataframe(dfName7)  
    dfName8 = process_dataframe(dfName8)
    dfName9 = process_dataframe(dfName9)  

    merged_df = pd.concat([dfName1, dfName2, dfName3,dfName4,dfName5,dfName6,dfName7,dfName8,dfName9], axis=1)
    merged_df = merged_df.to_csv()
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_client = blob_service_client.get_container_client("supplycushionmetrics")
    blob_client = container_client.get_blob_client("supplycurrent.csv")
    container_client = blob_client.upload_blob(merged_df,overwrite=True)
    return f"Hello {name}!"
