from datetime import datetime, timedelta
import http.client
import numpy as np
import ssl
import certifi
import azure.functions as func
from io import StringIO
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
import pandas as pd
import io
import datetime


def main(historicaldata):
    last_item = historicaldata.pop()
    # print(historicaldata)
    # print(last_item)
    SERVER = 'api.nrgstream.com'
    USERNAME = 'suncor2'
    PASSWORD = 'anisoTropical308'
    SEVENDAYPREMIUM_STORAGE="DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net"
    class HTTPSConnection(http.client.HTTPSConnection):
        def __enter__(self):
            return self
        
        def __exit__(self, exc_type, exc_value, traceback):
            self.close()

    def get_last_hour_date_str():
        last_hour = datetime.datetime.now() - datetime.timedelta(hours=1)
        last_hour = last_hour.replace(minute=0, second=0, microsecond=0)  # Round down to the nearest hour
        return last_hour.strftime("%m/%d/%Y %H:%M:%S")

    def get_this_hour_date_str():
        this_hour = datetime.datetime.now()
        this_hour = this_hour.replace(minute=0, second=0, microsecond=0)  # Round down to the nearest hour
        return this_hour.strftime("%m/%d/%Y %H:%M:%S")

    def get_stream_data(server, token, stream_ids, from_date, to_date, data_format='csv'):
        dataframes = []
        for stream_id in stream_ids:
            path = f'/api/StreamData/{stream_id}?fromDate={from_date.replace(" ", "%20")}&toDate={to_date.replace(" ", "%20")}'
            headers = {'Accept': 'text/csv' if data_format == 'csv' else 'Application/json', 'Authorization': f'Bearer {token}'}
            context = ssl.create_default_context(cafile=certifi.where())
            with HTTPSConnection(server, context=context) as conn:
                conn.request('GET', path, None, headers)
                res = conn.getresponse()
                res_code = res.code
                #print(f"Response code for stream ID {stream_id}: {res_code}")  # Print the response code
                if res_code == 200:
                    stream_data = res.read().decode('utf-8').replace('\r\n', '\n')
                    #print(f"Data for stream ID {stream_id}: {stream_data}")  # Print the stream data
                    dataframes.append(pd.read_csv(io.StringIO(stream_data), comment='#'))
                else:
                    print(f"Error for stream ID {stream_id}: {res.read().decode('utf-8')}")  # Print the error message
        return dataframes

    token_blob_service_client = BlobServiceClient.from_connection_string(SEVENDAYPREMIUM_STORAGE)

    token = None
    blob_name = "token.txt"
    path = "token"
    try:
        container_client = token_blob_service_client.get_container_client(path)
        blob_client = container_client.get_blob_client(blob_name)
        blob_data = blob_client.download_blob().content_as_text()
        token = blob_data
    except ResourceNotFoundError:
        print(f"Blob Not Found : {blob_name}")
    STREAM_IDS = historicaldata 
    eventname= "supplyhistorical.csv"
    dataframes = get_stream_data(SERVER, token, STREAM_IDS, get_last_hour_date_str(), get_this_hour_date_str())
    df = pd.concat(dataframes, axis=1)
    first_column = df.iloc[:, 0]

    def is_datetime_column(column_name):
        return column_name.startswith("Date/Time")

    # Find duplicate 'Date/Time' columns
    duplicate_datetime_columns = [col for i, col in enumerate(df.columns) if is_datetime_column(col) and col in df.columns[:i]]

    # Drop duplicate 'Date/Time' columns
    df = df.drop(columns=duplicate_datetime_columns)
    df['date'] = first_column
    df = df.set_index('date')
    df = df.to_csv()

    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
    container_client = blob_service_client.get_container_client("supplycushionmetrics")
    blob_client = container_client.get_blob_client(last_item)
    container_client = blob_client.upload_blob(df,overwrite=True)
    return f"Hello!"
