import os
import time
import logging
from io import BytesIO
from datetime import datetime, timedelta

import pandas as pd
import requests
from azure.storage.blob import BlobServiceClient
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# =========================================================
# ENV VARIABLES
# =========================================================

CLIENT_ID = os.environ["OPENSKY_CLIENT_ID"]
CLIENT_SECRET = os.environ["OPENSKY_CLIENT_SECRET"]

CONNECT_STR = os.environ["CONNECT_STR"]
CONTAINER_NAME = os.environ["CONTAINER_NAME"]

# =========================================================
# TOKEN URL
# =========================================================

TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/"
    "opensky-network/protocol/openid-connect/token"
)

# =========================================================
# AIRPORTS (ICAO)
# =========================================================

BRAZIL_AIRPORTS = {
    "SBSV": "Salvador - Aeroporto Internacional de Salvador",
    "SBGR": "São Paulo - Aeroporto Internacional de Guarulhos",
    "SBSP": "São Paulo - Aeroporto de Congonhas",
    "SBBR": "Brasília - Aeroporto Internacional de Brasília",
    "SBRJ": "Rio de Janeiro - Aeroporto Santos Dumont",
    "SBGL": "Rio de Janeiro - Aeroporto Internacional do Galeão",
    "SBCF": "Belo Horizonte - Aeroporto Internacional de Confins",
    "SBFZ": "Fortaleza - Aeroporto Internacional Pinto Martins",
    "SBRF": "Recife - Aeroporto Internacional dos Guararapes",
    "SBCT": "Curitiba - Aeroporto Internacional Afonso Pena",
    "SBBE": "Belém - Aeroporto Internacional de Belém",
    "SBEG": "Manaus - Aeroporto Internacional Eduardo Gomes",
    "SBVT": "Vitória - Aeroporto de Vitória",
    "SBFL": "Florianópolis - Aeroporto Internacional Hercílio Luz",
    "SBGO": "Goiânia - Aeroporto Internacional Santa Genoveva",
    "SBSG": "Natal - Aeroporto Internacional Aluízio Alves",
    "SBMO": "Maceió - Aeroporto Internacional Zumbi dos Palmares",
    "SBCG": "Campo Grande - Aeroporto Internacional de Campo Grande",
    "SBSL": "São Luís - Aeroporto Internacional de São Luís",
    "SBCY": "Cuiabá - Aeroporto Internacional Marechal Rondon",
    "SBTE": "Teresina - Aeroporto de Teresina",
    "SBAR": "Aracaju - Aeroporto de Aracaju",
    "SBPV": "Porto Velho - Aeroporto Internacional de Porto Velho",
    "SBBV": "Boa Vista - Aeroporto Internacional de Boa Vista",
    "SBRB": "Rio Branco - Aeroporto Internacional de Rio Branco",
    "SBPJ": "Palmas - Aeroporto de Palmas",
    "SBJP": "João Pessoa - Aeroporto Internacional Presidente Castro Pinto",
    "SBPA": "Porto Alegre - Aeroporto Internacional Salgado Filho",
    "SBKP": "Campinas - Aeroporto Internacional de Viracopos",
    "SBPS": "Porto Seguro - Aeroporto de Porto Seguro",
    "SBNF": "Navegantes - Aeroporto Internacional de Navegantes",
    "SBFI": "Foz do Iguaçu - Aeroporto Internacional de Foz do Iguaçu",
    "SBCX": "Caxias do Sul - Aeroporto Regional Hugo Cantergiani",
    "SBLO": "Londrina - Aeroporto de Londrina",
    "SBJV": "Joinville - Aeroporto de Joinville",
    "SBUL": "Uberlândia - Aeroporto de Uberlândia",
    "SBRP": "Ribeirão Preto - Aeroporto Leite Lopes",
    "SBMG": "Maringá - Aeroporto de Maringá",
}

# =========================================================
# TOKEN
# =========================================================

def get_access_token():

    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }

    response = requests.post(
        TOKEN_URL,
        data=payload,
        timeout=60
    )

    response.raise_for_status()

    token = response.json()["access_token"]

    return token

# =========================================================
# TIME
# =========================================================

def unix_timestamp(dt):
    return int(dt.timestamp())

# =========================================================
# GET FLIGHTS
# =========================================================

def get_flights(
    airport,
    begin,
    end,
    flight_type,
    token
):

    url = (
        f"https://api.opensky-network.org/"
        f"api/flights/{flight_type}"
    )

    headers = {
        "Authorization": f"Bearer {token}"
    }

    params = {
        "airport": airport,
        "begin": begin,
        "end": end
    }

    try:

        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=120,
            verify=False
        )

        if response.status_code != 200:

            logging.warning(
                f"{airport} {flight_type} erro "
                f"{response.status_code}"
            )

            logging.warning(response.text)

            return []

        data = response.json()

        logging.info(
            f"{airport} {flight_type}: "
            f"{len(data)} voos"
        )

        return data

    except Exception as e:

        logging.error(
            f"{airport} {flight_type} exception: {e}"
        )

        return []

# =========================================================
# PROCESS FLIGHTS
# =========================================================

def process_flights(
    flights,
    airport_name,
    tipo
):

    rows = []

    for f in flights:

        callsign = str(
            f.get("callsign", "")
        ).strip()

        departure_airport = f.get(
            "estDepartureAirport"
        )

        arrival_airport = f.get(
            "estArrivalAirport"
        )

        first_seen = f.get("firstSeen")

        last_seen = f.get("lastSeen")

        first_seen_dt = (
            datetime.utcfromtimestamp(first_seen)
            if first_seen
            else None
        )

        last_seen_dt = (
            datetime.utcfromtimestamp(last_seen)
            if last_seen
            else None
        )

        if tipo == "Chegada":

            from_field = departure_airport

        else:

            from_field = arrival_airport

        rows.append({

            "Time":
                first_seen_dt.strftime("%H:%M")
                if first_seen_dt
                else None,

            "Flight":
                callsign,

            "From":
                from_field,

            "Airline":
                callsign[:3]
                if callsign
                else None,

            "Aircraft":
                f.get("icao24"),

            "Status":
                "Landed"
                if tipo == "Chegada"
                else "Departed",

            "Delay_status":
                None,

            "date_flight":
                last_seen_dt.strftime("%Y-%m-%d")
                if last_seen_dt
                else None,

            "Tipo":
                tipo,

            "Aeroporto":
                airport_name
        })

    return rows

# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    logging.info("Iniciando pipeline OpenSky")

    token = get_access_token()

    all_rows = []

    today = datetime.utcnow()

    yesterday = today - timedelta(days=1)

    begin = unix_timestamp(
        datetime(
            yesterday.year,
            yesterday.month,
            yesterday.day,
            0,
            0,
            0
        )
    )

    end = unix_timestamp(
        datetime(
            yesterday.year,
            yesterday.month,
            yesterday.day,
            23,
            59,
            59
        )
    )

    for airport, airport_name in BRAZIL_AIRPORTS.items():

        logging.info(
            f"Coletando aeroporto {airport}"
        )

        arrivals = get_flights(
            airport=airport,
            begin=begin,
            end=end,
            flight_type="arrival",
            token=token
        )

        departures = get_flights(
            airport=airport,
            begin=begin,
            end=end,
            flight_type="departure",
            token=token
        )

        arrival_rows = process_flights(
            arrivals,
            airport_name,
            "Chegada"
        )

        departure_rows = process_flights(
            departures,
            airport_name,
            "Partida"
        )

        all_rows.extend(arrival_rows)
        all_rows.extend(departure_rows)

        time.sleep(2)

    df = pd.DataFrame(all_rows)

    logging.info(
        f"Total de voos coletados: {len(df)}"
    )

    # =====================================================
    # UPLOAD AZURE
    # =====================================================

    blob_service_client = (
        BlobServiceClient.from_connection_string(
            CONNECT_STR
        )
    )

    container_client = (
        blob_service_client.get_container_client(
            CONTAINER_NAME
        )
    )

    parquet_buffer = BytesIO()

    df.to_parquet(
        parquet_buffer,
        index=False
    )

    blob_name = (
        f"voos_{yesterday.strftime('%Y-%m-%d')}"
        f"_bronze.parquet"
    )

    blob_client = (
        container_client.get_blob_client(
            blob_name
        )
    )

    blob_client.upload_blob(
        parquet_buffer.getvalue(),
        overwrite=True
    )

    logging.info(
        f"Upload concluído: {blob_name}"
    )
