import os
import pandas as pd
from movedata import read, save

CONNECT_STR = os.environ["CONNECT_STR"]

PRATA = os.environ["CONTAINER_PRATA"]
OURO = os.environ["CONTAINER_OURO"]

df = read(CONNECT_STR, PRATA, "prata")

def calculate_status(row):

    if pd.isna(row["lastSeen"]):
        return "Unknown"

    return "Completed"

df["Status"] = df.apply(
    calculate_status,
    axis=1
)

df["Duracao_Minutos"] = (
    (df["lastSeen"] - df["firstSeen"]) / 60
)

df = df.rename(columns={
    "Flight": "Numero_Voo",
    "Airline": "Companhia_Aerea",
    "date_flight": "Data_Voo"
})

save(
    df,
    CONNECT_STR,
    OURO,
    "ouro"
)

print(df.head())
