import os
import logging
import pandas as pd

from io import BytesIO
from datetime import datetime, timezone

from azure.storage.blob import BlobServiceClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

CONNECT_STR = os.environ["CONNECT_STR"]

CONTAINER_BRONZE = os.environ["CONTAINER_NAME"]

CONTAINER_PRATA = os.environ["CONTAINER_PRATA"]

blob_service_client = (
    BlobServiceClient
    .from_connection_string(CONNECT_STR)
)

bronze_container = (
    blob_service_client
    .get_container_client(CONTAINER_BRONZE)
)

silver_container = (
    blob_service_client
    .get_container_client(CONTAINER_PRATA)
)

today_utc = datetime.now(
    timezone.utc
).strftime("%Y-%m-%d")

blob_name = (
    f"voos_{today_utc}_bronze.parquet"
)

blob_client = bronze_container.get_blob_client(
    blob_name
)

download_stream = blob_client.download_blob()

buffer = BytesIO()

buffer.write(download_stream.readall())

buffer.seek(0)

df = pd.read_parquet(buffer)

logging.info(
    f"Registros bronze: {len(df)}"
)

df = df.drop_duplicates()

df = df.dropna(
    subset=[
        "flight_iata",
        "date_flight"
    ]
)

df["status"] = (
    df["status"]
    .fillna("unknown")
    .str.lower()
)

delay_keywords = [
    "delayed",
    "late"
]

df["is_delayed"] = (
    df["status"]
    .str.contains(
        "|".join(delay_keywords),
        case=False,
        na=False
    )
)

df["companhia"] = (
    df["airline_name"]
    .fillna("Unknown")
)

df["rota"] = (
    df["dep_iata"]
    .fillna("UNK")
    + " -> " +
    df["arr_iata"]
    .fillna("UNK")
)

silver_buffer = BytesIO()

df.to_parquet(
    silver_buffer,
    index=False
)

silver_blob_name = (
    f"voos_{today_utc}_silver.parquet"
)

silver_blob_client = (
    silver_container
    .get_blob_client(silver_blob_name)
)

silver_blob_client.upload_blob(
    silver_buffer.getvalue(),
    overwrite=True
)

logging.info(
    f"Silver upload concluído: "
    f"{silver_blob_name}"
)
