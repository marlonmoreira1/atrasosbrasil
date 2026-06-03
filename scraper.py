# scraper.py

import os
import time
import logging
import requests
import pandas as pd

from io import BytesIO
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

API_KEY = os.environ["AIRLABS_API_KEY"]

CONNECT_STR = os.environ["CONNECT_STR"]
CONTAINER_NAME = os.environ["CONTAINER_NAME"]

STATUS_ICON_MAP = {
    "scheduled": "gray",
    "active": "blue",
    "landed": "green",
    "cancelled": "red",
    "incident": "red",
    "diverted": "yellow",
    "delayed": "yellow",
    "unknown": "gray"
}

brazil_airports = {
    'SSA': 'Salvador - Aeroporto Internacional de Salvador',
    'GRU': 'São Paulo - Aeroporto Internacional de Guarulhos',
    'CGH': 'São Paulo - Aeroporto de Congonhas',
    'BSB': 'Brasília - Aeroporto Internacional de Brasília',
    'SDU': 'Rio de Janeiro - Aeroporto Internacional de Santos Dumont',
    'GIG': 'Rio de Janeiro - Aeroporto Internacional do Galeão',
    'CNF': 'Belo Horizonte - Aeroporto Internacional de Confins',
    'FOR': 'Fortaleza - Aeroporto Internacional Pinto Martins',
    'REC': 'Recife - Aeroporto Internacional dos Guararapes',
    'CWB': 'Curitiba - Aeroporto Internacional Afonso Pena',    
    'FLN': 'Florianópolis - Aeroporto Internacional Hercílio Luz',
    'GYN': 'Goiânia - Aeroporto Internacional Santa Genoveva',       
    'POA': 'Porto Alegre - Aeroporto Internacional Salgado Filho',    
    'VCP': 'Campinas - Aeroporto Internacional de Viracopos'    
}


def format_status(status, real_time):

    status = str(status).title()

    if real_time:
        return f"{status} {real_time}"

    return status


def coletar_voos(iata, tipo):

    if tipo == "arrivals":
        url = (
            f"https://airlabs.co/api/v9/schedules?"
            f"arr_iata={iata}&api_key={API_KEY}"
        )
    else:
        url = (
            f"https://airlabs.co/api/v9/schedules?"
            f"dep_iata={iata}&api_key={API_KEY}"
        )

    response = requests.get(url, timeout=60)

    logging.info(
    f"{iata} {tipo} | Status HTTP: {response.status_code}"
)

    try:
        json_response = response.json()
    
        logging.info(
            f"{iata} {tipo} | Chaves retorno: {list(json_response.keys())}"
        )
    
        logging.info(
            f"{iata} {tipo} | Retorno: {str(json_response)[:500]}"
        )
    
    except Exception as e:
    
        logging.error(
            f"{iata} {tipo} erro lendo JSON: {e}"
        )

    data = response.json().get("response", [])

    registros = []

    for flight in data:

        try:

            flight_number = (
                flight.get("flight_iata")
                or flight.get("flight_icao")
            )

            airline_name = (
                flight.get("airline_name")
                or "Unknown"
            )

            aircraft_code = (
                flight.get("aircraft_icao")
                or "Unknown"
            )

            status_raw = (
                flight.get("status")
                or "unknown"
            ).lower()

            delay_status = STATUS_ICON_MAP.get(
                status_raw,
                "gray"
            )

            if tipo == "arrivals":

                airport_iata = flight.get("dep_iata")

                city = (
                    brazil_airports.get(airport_iata, "")
                    .split(" - ")[0]
                )

                scheduled = flight.get("arr_time")
                real = flight.get("arr_actual")

            else:

                airport_iata = flight.get("arr_iata")

                city = (
                    brazil_airports.get(airport_iata, "")
                    .split(" - ")[0]
                )

                scheduled = flight.get("dep_time")
                real = flight.get("dep_actual")

            from_location = None

            if city and airport_iata:
                from_location = f"{city}({airport_iata})-"

            dt_scheduled = None
            dt_real = None

            if scheduled:
                dt_scheduled = datetime.fromisoformat(
                    scheduled.replace("Z", "+00:00")
                )

            if real:
                dt_real = datetime.fromisoformat(
                    real.replace("Z", "+00:00")
                )

            flight_date = None
            time_fmt = None
            real_fmt = None

            if dt_scheduled:
                flight_date = dt_scheduled.strftime("%Y-%m-%d")
                time_fmt = dt_scheduled.strftime("%I:%M %p")

            if dt_real:
                real_fmt = dt_real.strftime("%I:%M %p")

            status_final = format_status(
                status_raw,
                real_fmt
            )

            registro = {
                "Time": time_fmt,
                "Flight": flight_number,
                "From": from_location,
                "Airline": airline_name,
                "Aircraft": aircraft_code,
                "Status": status_final,
                "Delay_status": delay_status,
                "date_flight": flight_date
            }

            registros.append(registro)

        except Exception as e:
            logging.error(f"Erro parsing voo: {e}")

    df = pd.DataFrame(registros)

    if df.empty:
        return df

    return df.drop_duplicates()


def collect_data_from_airports(airports):

    all_data = []

    for airport, nome in airports.items():

        logging.info(f"Coletando {airport}")

        try:

            arrivals_df = coletar_voos(
                airport,
                "arrivals"
            )

            arrivals_df["Tipo"] = "arrivals"
            arrivals_df["Aeroporto"] = nome

            time.sleep(1)

            departures_df = coletar_voos(
                airport,
                "departures"
            )

            departures_df["Tipo"] = "departures"
            departures_df["Aeroporto"] = nome

            all_data.append(arrivals_df)
            all_data.append(departures_df)

        except Exception as e:

            logging.error(f"{airport} erro: {e}")

    return pd.concat(
        all_data,
        ignore_index=True
    )


df_final = collect_data_from_airports(
    brazil_airports
)

if df_final.empty:
    raise Exception("Nenhum dado coletado")

data_hoje = datetime.today()
data_ontem = data_hoje - timedelta(days=1)

data_filtro = data_ontem.strftime("%Y-%m-%d")

voos = df_final[
    df_final["date_flight"] == data_filtro
]

logging.info(f"Total voos: {len(voos)}")
logging.info(f"Colunas: {list(voos.columns)}")

blob_service_client = (
    BlobServiceClient
    .from_connection_string(CONNECT_STR)
)

container_client = (
    blob_service_client
    .get_container_client(CONTAINER_NAME)
)

buffer = BytesIO()

voos.to_parquet(
    buffer,
    index=False
)

blob_name = (
    f"voos_{data_filtro}_bronze.parquet"
)

blob_client = (
    container_client
    .get_blob_client(blob_name)
)

blob_client.upload_blob(
    buffer.getvalue(),
    overwrite=True
)

logging.info(
    f"Bronze upload concluído: {blob_name}"
)
