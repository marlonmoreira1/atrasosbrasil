import os
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
from azure.storage.blob import BlobServiceClient
import time

logging.basicConfig(level=logging.INFO)

CLIENT_ID = os.environ["OPENSKY_CLIENT_ID"]
CLIENT_SECRET = os.environ["OPENSKY_CLIENT_SECRET"]

CONNECT_STR = os.environ["CONNECT_STR"]
CONTAINER_NAME = os.environ["CONTAINER_NAME"]

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

BRAZIL_AIRPORTS = {
    "SSA": "Salvador",
    "GRU": "Guarulhos",
    "CGH": "Congonhas",
    "BSB": "Brasília",
    "SDU": "Santos Dumont",
    "GIG": "Galeão",
    "CNF": "Confins",
    "FOR": "Fortaleza",
    "REC": "Recife",
    "CWB": "Curitiba",
    "BEL": "Belém",
    "MAO": "Manaus",
    "VIX": "Vitória",
    "FLN": "Florianópolis",
    "GYN": "Goiânia",
    "NAT": "Natal",
    "MCZ": "Maceió",
    "CGR": "Campo Grande",
    "SLZ": "São Luís",
    "CGB": "Cuiabá",
    "THE": "Teresina",
    "AJU": "Aracaju",
    "PVH": "Porto Velho",
    "BVB": "Boa Vista",
    "RBR": "Rio Branco",
    "PMW": "Palmas",
    "JPA": "João Pessoa",
    "POA": "Porto Alegre",
    "VCP": "Viracopos",
    "BPS": "Porto Seguro",
    "NVT": "Navegantes",
    "IGU": "Foz do Iguaçu",
}

def get_access_token():

    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }

    response = requests.post(
        TOKEN_URL,
        data=payload,
        timeout=60
    )

    response.raise_for_status()

    token = response.json()["access_token"]

    return token

def unix_timestamp(dt):
    return int(dt.timestamp())

def get_flights(
    airport,
    begin,
    end,
    flight_type,
    token
):

    url = f"https://opensky-network.org/api/flights/{flight_type}"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    params = {
        "airport": airport,
        "begin": begin,
        "end": end
    }

    response = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=120
    )

    if response.status_code != 200:

        logging.warning(
            f"{airport} {flight_type} erro {response.status_code}"
        )

        return []

    return response.json()

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

        rows.append({

            "icao24": f.get("icao24"),

            "Flight": callsign,

            "DepartureAirport":
                f.get("estDepartureAirport"),

            "ArrivalAirport":
                f.get("estArrivalAirport"),

            "firstSeen":
                f.get("firstSeen"),

            "lastSeen":
                f.get("lastSeen"),

            "Tipo":
                tipo,

            "Aeroporto":
                airport_name,

            "date_flight":
                datetime.utcfromtimestamp(
                    f.get("lastSeen")
                ).strftime("%Y-%m-%d")
                if f.get("lastSeen")
                else None
        })

    return rows

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

for airport, name in BRAZIL_AIRPORTS.items():

    logging.info(f"Coletando {airport}")

    arrivals = get_flights(
        airport,
        begin,
        end,
        "arrival",
        token
    )

    departures = get_flights(
        airport,
        begin,
        end,
        "departure",
        token
    )

    all_rows.extend(
        process_flights(
            arrivals,
            name,
            "Chegada"
        )
    )

    all_rows.extend(
        process_flights(
            departures,
            name,
            "Partida"
        )
    )

    time.sleep(3)

df = pd.DataFrame(all_rows)

blob_service_client = BlobServiceClient.from_connection_string(
    CONNECT_STR
)

container_client = blob_service_client.get_container_client(
    CONTAINER_NAME
)

buffer = BytesIO()

df.to_parquet(buffer, index=False)

blob_name = (
    f"voos_{yesterday.strftime('%Y-%m-%d')}_bronze.parquet"
)

blob_client = container_client.get_blob_client(
    blob_name
)

blob_client.upload_blob(
    buffer.getvalue(),
    overwrite=True
)

logging.info(
    f"Upload bronze concluído: {blob_name}"
)
