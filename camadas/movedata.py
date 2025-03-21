from azure.storage.blob import BlobServiceClient
import pandas as pd
from datetime import datetime, timedelta
import os
from io import BytesIO



def read(connect_str,container,nome):
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container)

    
    data_hoje = datetime.today()
    data_ontem = data_hoje - timedelta(days=1)
    data_filtro = data_ontem.strftime('%Y-%m-%d')    
    
    blob_name = f"voos_{data_filtro}_{nome}.parquet"

    blob_client = container_client.get_blob_client(blob_name)
    stream = blob_client.download_blob()
    data = stream.readall()

    parquet_buffer = BytesIO(data)
    df = pd.read_parquet(parquet_buffer)

    return df


def save(df,connect_str,container,nome):
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container)
    
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_data = parquet_buffer.getvalue()    
    
    data_hoje = datetime.today()
    data_ontem = data_hoje - timedelta(days=1)
    data_filtro = data_ontem.strftime('%Y-%m-%d')
    

    blob_name = f"voos_{data_filtro}_{nome}.parquet"
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(parquet_data, overwrite=True)
