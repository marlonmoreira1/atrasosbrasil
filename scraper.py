import os
import time
import logging
import requests
import pandas as pd

from io import BytesIO
from datetime import datetime, timedelta

from azure.storage.blob import BlobServiceClient

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==========================================================
# LOGGING
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Iniciando pipeline OpenSky")

# ==========================================================
# ENV
# ==========================================================

CLIENT_ID = os.environ["OPENSKY_CLIENT_ID"]
CLIENT_SECRET = os.environ["OPENSKY_CLIENT_SECRET"]

CONNECT_STR = os.environ["CONNECT_STR"]
CONTAINER_NAME = os.environ["CONTAINER_NAME"]

# ==========================================================
# URLS
# ==========================================================

TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/"
    "opensky-network/protocol/openid-connect/token"
)

BASE_URL = "https://opensky-network.org/api"

# ==========================================================
# SESSION COM RETRY
# ==========================================================

session = requests.Session()

retry_strategy = Retry(
    total=5,
    backoff_factor=2,
    status_forcelist=[
        429,
        500,
        502,
        503,
        504
    ],
    allowed_methods=["GET", "POST"]
)

adapter = HTTPAdapter(
    max_retries=retry_strategy
)

session.mount("https://", adapter)
session.mount("http://", adapter)

# ==========================================================
# AIRPORTS
# ==========================================================

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

# ==========================================================
# TOKEN MANAGER
# ==========================================================

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

        max_retries = 5

        for attempt in range(max_retries):

            try:

                logging.info(
                    f"Gerando token "
                    f"(tentativa {attempt + 1})"
                )

                response = session.post(
                    TOKEN_URL,
                    data={
                        "grant_type": "client_credentials",
                        "client_id": CLIENT_ID,
                        "client_secret": CLIENT_SECRET
                    },
                    timeout=(30, 120)
                )

                response.raise_for_status()

                data = response.json()

                self.token = data["access_token"]

                expires_in = data.get(
                    "expires_in",
                    1800
                )

                self.expires_at = (
                    datetime.utcnow()
                    + timedelta(
                        seconds=expires_in - 60
                    )
                )

                logging.info(
                    "Token gerado com sucesso"
                )

                return self.token

            except Exception as e:

                wait_time = 2 ** attempt

                logging.error(
                    f"Erro token: {e}"
                )

                logging.info(
                    f"Retry em {wait_time}s"
                )

                time.sleep(wait_time)

        raise Exception(
            "Falha ao autenticar no OpenSky"
        )

    def headers(self):

        return {
            "Authorization":
                f"Bearer {self.get_token()}"
        }

tokens = TokenManager()

# ==========================================================
# API
# ==========================================================

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

    max_retries = 3

    for attempt in range(max_retries):

        try:

            response = session.get(
                url,
                headers=tokens.headers(),
                params=params,
                timeout=(30, 180)
            )

            if response.status_code == 404:

                logging.warning(
                    f"{airport} "
                    f"{flight_type} "
                    f"sem dados"
                )

                return []

            if response.status_code == 429:

                retry_after = int(
                    response.headers.get(
                        "X-Rate-Limit-Retry-After-Seconds",
                        60
                    )
                )

                logging.warning(
                    f"429 Rate Limit. "
                    f"Aguardando {retry_after}s"
                )

                time.sleep(retry_after)

                continue

            response.raise_for_status()

            data = response.json()

            logging.info(
                f"{airport} "
                f"{flight_type}: "
                f"{len(data)} voos"
            )

            return data

        except Exception as e:

            wait_time = 2 ** attempt

            logging.error(
                f"{airport} "
                f"{flight_type} "
                f"erro: {e}"
            )

            logging.info(
                f"Retry em {wait_time}s"
            )

            time.sleep(wait_time)

    return []

# ==========================================================
# PROCESSAMENTO
# ==========================================================

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

            "icao24":
                f.get("icao24"),

            "Flight":
                callsign,

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

# ==========================================================
# TARGET DAY
# ==========================================================

target_day = (
    datetime.utcnow() - timedelta(days=5)
)

target_day = datetime(
    target_day.year,
    target_day.month,
    target_day.day
)

begin = int(target_day.timestamp())

end = int(
    (
        target_day
        + timedelta(days=1)
    ).timestamp()
)

logging.info(
    f"Data UTC coletada: "
    f"{target_day.strftime('%Y-%m-%d')}"
)

# ==========================================================
# LOOP
# ==========================================================

all_rows = []

for airport, airport_name in BRAZIL_AIRPORTS.items():

    logging.info(
        f"Coletando aeroporto {airport}"
    )

    try:

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

    except Exception as e:

        logging.error(
            f"Erro aeroporto {airport}: {e}"
        )

# ==========================================================
# DATAFRAME
# ==========================================================

df = pd.DataFrame(all_rows)

if not df.empty:

    df = df.drop_duplicates(
        subset=[
            "icao24",
            "Flight",
            "firstSeen",
            "lastSeen",
            "Tipo"
        ]
    )

logging.info(
    f"Total registros: {len(df)}"
)

if df.empty:

    raise Exception(
        "Nenhum voo retornado"
    )

# ==========================================================
# AZURE BLOB
# ==========================================================

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
