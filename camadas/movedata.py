# movedata.py

from azure.storage.blob import BlobServiceClient
import pandas as pd
from datetime import datetime
from io import BytesIO


def read(connect_str, container, nome):

    blob_service_client = (
        BlobServiceClient
        .from_connection_string(connect_str)
    )

    container_client = (
        blob_service_client
        .get_container_client(container)
    )

    data_filtro = (
    datetime.today() - timedelta(days=1)
).strftime("%Y-%m-%d")

    blob_name = (
        f"voos_{data_filtro}_{nome}.parquet"
    )

    blob_client = (
        container_client
        .get_blob_client(blob_name)
    )

    stream = blob_client.download_blob()

    data = stream.readall()

    buffer = BytesIO(data)

    return pd.read_parquet(buffer)


def save(df, connect_str, container, nome):

    blob_service_client = (
        BlobServiceClient
        .from_connection_string(connect_str)
    )

    container_client = (
        blob_service_client
        .get_container_client(container)
    )

    buffer = BytesIO()

    df.to_parquet(
        buffer,
        index=False
    )

    data_filtro = (
    datetime.today() - timedelta(days=1)
).strftime("%Y-%m-%d")

    blob_name = (
        f"voos_{data_filtro}_{nome}.parquet"
    )

    blob_client = (
        container_client
        .get_blob_client(blob_name)
    )

    blob_client.upload_blob(
        buffer.getvalue(),
        overwrite=True
    )
