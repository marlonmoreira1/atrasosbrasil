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

CONTAINER_PRATA = os.environ["CONTAINER_PRATA"]

CONTAINER_OURO = os.environ["CONTAINER_OURO"]

blob_service_client = (
    BlobServiceClient
    .from_connection_string(CONNECT_STR)
)

silver_container = (
    blob_service_client
    .get_container_client(CONTAINER_PRATA)
)

gold_container = (
    blob_service_client
    .get_container_client(CONTAINER_OURO)
)

today_utc = datetime.now(
    timezone.utc
).strftime("%Y-%m-%d")

blob_name = (
    f"voos_{today_utc}_silver.parquet"
)

blob_client = silver_container.get_blob_client(
    blob_name
)

download_stream = blob_client.download_blob()

buffer = BytesIO()

buffer.write(download_stream.readall())

buffer.seek(0)

df = pd.read_parquet(buffer)

logging.info(
    f"Registros silver: {len(df)}"
)

# ================================
# KPI POR AEROPORTO
# ================================

gold_airport = (
    df.groupby("airport_name")
    .agg({
        "flight_iata": "count",
        "is_delayed": "sum"
    })
    .reset_index()
)

gold_airport.columns = [
    "airport_name",
    "total_voos",
    "voos_atrasados"
]

gold_airport["pct_atraso"] = (
    (
        gold_airport["voos_atrasados"]
        /
        gold_airport["total_voos"]
    ) * 100
).round(2)

# ================================
# KPI POR COMPANHIA
# ================================

gold_airline = (
    df.groupby("companhia")
    .agg({
        "flight_iata": "count",
        "is_delayed": "sum"
    })
    .reset_index()
)

gold_airline.columns = [
    "companhia",
    "total_voos",
    "voos_atrasados"
]

gold_airline["pct_atraso"] = (
    (
        gold_airline["voos_atrasados"]
        /
        gold_airline["total_voos"]
    ) * 100
).round(2)

# ================================
# UPLOAD
# ================================

gold_buffer_airport = BytesIO()

gold_airport.to_parquet(
    gold_buffer_airport,
    index=False
)

blob_airport = (
    gold_container.get_blob_client(
        f"kpi_aeroportos_{today_utc}.parquet"
    )
)

blob_airport.upload_blob(
    gold_buffer_airport.getvalue(),
    overwrite=True
)

gold_buffer_airline = BytesIO()

gold_airline.to_parquet(
    gold_buffer_airline,
    index=False
)

blob_airline = (
    gold_container.get_blob_client(
        f"kpi_companhias_{today_utc}.parquet"
    )
)

blob_airline.upload_blob(
    gold_buffer_airline.getvalue(),
    overwrite=True
)

logging.info(
    "Camada ouro concluída"
)
