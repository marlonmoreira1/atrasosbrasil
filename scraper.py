import os
import time
import logging
import requests
import pandas as pd

from io import BytesIO
from datetime import datetime, timezone

from azure.storage.blob import BlobServiceClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

API_KEY = os.environ["AIRLABS_API_KEY"]

CONNECT_STR = os.environ["CONNECT_STR"]
CONTAINER_NAME = os.environ["CONTAINER_NAME"]

BRAZIL_AIRPORTS = {
    'SSA': 'Salvador',
    'GRU': 'Guarulhos',
    'CGH': 'Congonhas',
    'BSB': 'Brasília',
    'SDU': 'Santos Dumont',
    'GIG': 'Galeão',
    'CNF': 'Confins',
    'FOR': 'Fortaleza',
    'REC': 'Recife',
    'CWB': 'Curitiba',
    'BEL': 'Belém',
    'MAO': 'Manaus',
    'GYN': 'Goiânia',
    'MCZ': 'Maceió',
    'POA': 'Porto Alegre',
    'VCP': 'Viracopos'
}

BASE_URL = "https://airlabs.co/api/v9/schedules"


def request_with_retry(url, max_retries=5):

    for attempt in range(max_retries):

        try:

            response = requests.get(
                url,
                timeout=30
            )

            response.raise_for_status()

            return response.json()

        except Exception as e:

            logging.warning(
                f"Tentativa {attempt + 1} falhou: {e}"
            )

            time.sleep(2 * (attempt + 1))

    return None


def parse_datetime(dt_str):

    if not dt_str:
        return None

    try:

        dt = datetime.fromisoformat(
            dt_str.replace("Z", "+00:00")
        )

        return dt

    except:
        return None


def coletar_voos(iata, tipo):

    if tipo == "arrivals":

        url = (
            f"{BASE_URL}"
            f"?arr_iata={iata}"
            f"&api_key={API_KEY}"
        )

    else:

        url = (
            f"{BASE_URL}"
            f"?dep_iata={iata}"
            f"&api_key={API_KEY}"
        )

    data = request_with_retry(url)

    if not data:

        return pd.DataFrame()

    flights = data.get("response", [])

    registros = []

    for flight in flights:

        try:

            dep_time = parse_datetime(
                flight.get("dep_time")
            )

            arr_time = parse_datetime(
                flight.get("arr_time")
            )

            reference_time = (
                arr_time
                if tipo == "arrivals"
                else dep_time
            )

            if not reference_time:
                continue

            registro = {

                "flight_iata":
                    flight.get("flight_iata"),

                "flight_icao":
                    flight.get("flight_icao"),

                "airline_name":
                    flight.get("airline_name"),

                "status":
                    flight.get("status"),

                "dep_iata":
                    flight.get("dep_iata"),

                "arr_iata":
                    flight.get("arr_iata"),

                "dep_time":
                    dep_time.strftime("%Y-%m-%d %H:%M:%S")
                    if dep_time else None,

                "arr_time":
                    arr_time.strftime("%Y-%m-%d %H:%M:%S")
                    if arr_time else None,

                "aircraft_icao":
                    flight.get("aircraft_icao"),

                "reg_number":
                    flight.get("reg_number"),

                "hex":
                    flight.get("hex"),

                "tipo":
                    tipo,

                "airport":
                    iata,

                "airport_name":
                    BRAZIL_AIRPORTS.get(iata),

                "date_flight":
                    reference_time.strftime("%Y-%m-%d"),

                "created_at_utc":
                    datetime.now(timezone.utc)
                    .strftime("%Y-%m-%d %H:%M:%S")
            }

            registros.append(registro)

        except Exception as e:

            logging.warning(
                f"Erro parsing voo: {e}"
            )

    df = pd.DataFrame(registros)

    if df.empty:
        return df

    return df.drop_duplicates()


all_data = []

for airport in BRAZIL_AIRPORTS.keys():

    logging.info(
        f"Coletando aeroporto {airport}"
    )

    arrivals = coletar_voos(
        airport,
        "arrivals"
    )

    departures = coletar_voos(
        airport,
        "departures"
    )

    all_data.append(arrivals)
    all_data.append(departures)

    time.sleep(2)

df_final = pd.concat(
    all_data,
    ignore_index=True
)

if df_final.empty:

    raise Exception(
        "Nenhum dado retornado"
    )

today_utc = datetime.now(
    timezone.utc
).strftime("%Y-%m-%d")

df_final = df_final[
    df_final["date_flight"] == today_utc
]

df_final = df_final.drop_duplicates()

logging.info(
    f"Total voos: {len(df_final)}"
)

blob_service_client = (
    BlobServiceClient
    .from_connection_string(CONNECT_STR)
)

container_client = (
    blob_service_client
    .get_container_client(CONTAINER_NAME)
)

buffer = BytesIO()

df_final.to_parquet(
    buffer,
    index=False
)

blob_name = (
    f"voos_{today_utc}_bronze.parquet"
)

blob_client = (
    container_client.get_blob_client(blob_name)
)

blob_client.upload_blob(
    buffer.getvalue(),
    overwrite=True
)

logging.info(
    f"Upload concluído: {blob_name}"
)
