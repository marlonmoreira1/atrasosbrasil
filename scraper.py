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

logging.info("Iniciando pipeline OpenSky")

CLIENT_ID = os.environ["OPENSKY_CLIENT_ID"]
CLIENT_SECRET = os.environ["OPENSKY_CLIENT_SECRET"]

CONNECT_STR = os.environ["CONNECT_STR"]
CONTAINER_NAME = os.environ["CONTAINER_NAME"]

TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/"
    "opensky-network/protocol/openid-connect/token"
)

BASE_URL = "https://opensky-network.org/api"

# ICAO AIRPORTS
BRAZIL_AIRPORTS = {
    "SBSV": "Salvador",
    "SBGR": "Guarulhos",
    "SBSP": "Congonhas",
    "SBBR": "Brasília",
    "SBRJ": "Santos Dumont",
    "SBGL": "Galeão",
    "SBCF": "Confins",
    "SBFZ": "Fortaleza",
    "SBRF": "Recife",
    "SBCT": "Curitiba",
    "SBBE": "Belém",
    "SBEG": "Manaus",
    "SBVT": "Vitória",
    "SBFL": "Florianópolis",
    "SBGO": "Goiânia",
    "SBSG": "Natal",
    "SBMO": "Maceió",
    "SBCG": "Campo Grande",
    "SBSL": "São Luís",
    "SBCY": "Cuiabá",
    "SBTE": "Teresina",
    "SBAR": "Aracaju",
    "SBPV": "Porto Velho",
    "SBBV": "Boa Vista",
    "SBRB": "Rio Branco",
    "SBPJ": "Palmas",
    "SBJP": "João Pessoa",
    "SBPA": "Porto Alegre",
    "SBKP": "Viracopos",
    "SBPS": "Porto Seguro",
    "SBNF": "Navegantes",
    "SBFI": "Foz do Iguaçu"
}


class TokenManager:

    def __init__(self):

        self.token = None
        self.expires_at = None

    def get_token(self):

        now = datetime.utcnow()

        if (
            self.token
            and self.expires_at
            and now < self.expires_at
        ):
            return self.token

        return self.refresh()

    def refresh(self):

        logging.info("Gerando novo token")

        response = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET
            },
            timeout=30
        )

        response.raise_for_status()

        data = response.json()

        self.token = data["access_token"]

        expires_in = data.get("expires_in", 1800)

        self.expires_at = (
            datetime.utcnow()
            + timedelta(seconds=expires_in - 60)
        )

        return self.token

    def headers(self):

        return {
            "Authorization": f"Bearer {self.get_token()}"
        }


tokens = TokenManager()


def unix_timestamp(dt):

    return int(dt.timestamp())


def get_flights(
    airport,
    begin,
    end,
    flight_type
):

    url = f"{BASE_URL}/flights/{flight_type}"

    params = {
        "airport": airport,
        "begin": begin,
        "end": end
    }

    try:

        response = requests.get(
            url,
            headers=tokens.headers(),
            params=params,
            timeout=60
        )

        if response.status_code == 404:

            logging.warning(
                f"{airport} {flight_type} sem dados"
            )

            return []

        response.raise_for_status()

        return response.json()

    except Exception as e:

        logging.error(
            f"{airport} {flight_type} exception: {e}"
        )

        return []


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


# IMPORTANTE:
# flights endpoint é histórico
# então pega D-2 para garantir processamento

target_day = (
    datetime.utcnow() - timedelta(days=2)
)

begin = unix_timestamp(
    datetime(
        target_day.year,
        target_day.month,
        target_day.day,
        0,
        0,
        0
    )
)

end = unix_timestamp(
    datetime(
        target_day.year,
        target_day.month,
        target_day.day,
        23,
        59,
        59
    )
)

all_rows = []

for airport, airport_name in BRAZIL_AIRPORTS.items():

    logging.info(
        f"Coletando aeroporto {airport}"
    )

    arrivals = get_flights(
        airport,
        begin,
        end,
        "arrival"
    )

    departures = get_flights(
        airport,
        begin,
        end,
        "departure"
    )

    all_rows.extend(
        process_flights(
            arrivals,
            airport_name,
            "Chegada"
        )
    )

    all_rows.extend(
        process_flights(
            departures,
            airport_name,
            "Partida"
        )
    )

    time.sleep(2)

df = pd.DataFrame(all_rows)

logging.info(
    f"Total registros coletados: {len(df)}"
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

df.to_parquet(
    buffer,
    index=False
)

blob_name = (
    f"voos_{target_day.strftime('%Y-%m-%d')}_bronze.parquet"
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
