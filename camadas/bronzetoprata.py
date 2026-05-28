import os
import pandas as pd
from movedata import read, save

CONNECT_STR = os.environ["CONNECT_STR"]

BRONZE = os.environ["CONTAINER_NAME"]
PRATA = os.environ["CONTAINER_PRATA"]

df = read(CONNECT_STR, BRONZE, "bronze")

airlines = pd.read_csv("data/airlines.csv")

def identify_airline(callsign):

    if pd.isna(callsign):
        return None

    prefix = str(callsign)[:3]

    result = airlines[
        airlines["prefix"] == prefix
    ]

    if len(result) == 0:
        return "Unknown"

    return result.iloc[0]["airline"]

df["Airline"] = df["Flight"].apply(
    identify_airline
)

save(
    df,
    CONNECT_STR,
    PRATA,
    "prata"
)

print(df.head())
