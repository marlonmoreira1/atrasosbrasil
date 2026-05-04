import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from typing import Dict, Callable
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
from unidecode import unidecode
import time
import os
import socket
import urllib3
from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException, WebDriverException
import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from io import BytesIO


def coletar_voos(iata, tipo):
    API_KEY = os.environ.get("AIRLABS_API_KEY")

    if tipo == "arrivals":
        url = f"https://airlabs.co/api/v9/schedules?arr_iata={iata.upper()}&api_key={API_KEY}"
    else:
        url = f"https://airlabs.co/api/v9/schedules?dep_iata={iata.upper()}&api_key={API_KEY}"

    try:
        response = requests.get(url, timeout=10)
    except Exception as e:
        print(f"Erro conexão: {e}")
        return pd.DataFrame()

    if response.status_code != 200:
        print(f"Erro HTTP: {response.status_code}")
        return pd.DataFrame()

    data = response.json().get("response", [])

    registros = []

    for flight in data:
        try:
            # --- INICIALIZAÇÃO (EVITA ERRO)
            departure_time_fmt = None
            flight_date = None

            # --- FLIGHT
            flight_number = flight.get("flight_iata") or flight.get("flight_icao")

            # --- AIRLINE
            airline_name = (
                flight.get("airline_name")
                or flight.get("airline_iata")
                or "Unknown"
            )

            # --- TEMPOS
            departure_time = flight.get("dep_time")
            arrival_time = flight.get("arr_time")

            if tipo == "arrivals":
                time_raw = arrival_time
                airport_iata = flight.get("dep_iata")  # origem
            else:
                time_raw = departure_time
                airport_iata = flight.get("arr_iata")  # destino

            # --- TRATAMENTO DE DATA
            if time_raw:
                try:
                    dt = datetime.fromisoformat(time_raw.replace("Z", "+00:00"))
                    flight_date = dt.strftime("%Y-%m-%d")
                    departure_time_fmt = dt.strftime("%I:%M %p")
                except:
                    pass

            # --- FROM (COMPATÍVEL COM TEU PIPELINE)
            if airport_iata and airport_iata in brazil_airports:
                city_name = brazil_airports[airport_iata].split(" - ")[0]
                from_location = f"{city_name}({airport_iata})-"
            else:
                from_location = f"Unknown({airport_iata})-" if airport_iata else None

            # --- STATUS
            status_text = flight.get("status") or "Unknown"
            status_real = status_text

            # --- AERONAVE
            aircraft = flight.get("aircraft_icao") or ""
            registration = flight.get("reg_number") or ""
            aircraft_total = f"{aircraft}({registration})"

            # --- REGISTRO FINAL (MESMO FORMATO ANTIGO)
            registro = {
                "Time": departure_time_fmt,
                "Flight": flight_number,
                "From": from_location,
                "Airline": airline_name,
                "Aircraft": aircraft_total,
                "Status": status_real,
                "Delay_status": status_text,
                "date_flight": flight_date
            }

            registros.append(registro)

        except Exception as e:
            print(f"Erro parsing: {e}")
            continue

    df = pd.DataFrame(registros)

    if df.empty:
        return df

    return df.drop_duplicates()


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
    'BEL': 'Belém - Aeroporto Internacional de Belém',
    'MAO': 'Manaus - Aeroporto Internacional Eduardo Gomes',    
    'GYN': 'Goiânia - Aeroporto Internacional Santa Genoveva',    
    'MCZ': 'Maceió - Aeroporto Internacional Zumbi dos Palmares',    
    'POA': 'Porto Alegre - Aeroporto Internacional Salgado Filho',    
    
    # Outros aeroportos relevantes    
    'VCP': 'Campinas - Aeroporto Internacional de Viracopos'    
}            

def collect_data_from_airports(airports, collect_function):
       
    all_data = []
    
    for airport, nome in airports.items():
        print(f"Coletando dados para o aeroporto: {airport} - {nome}")

        def try_collect(iata,tipo):
            retries = 0
            max_retries = 20
            while retries < max_retries:
                try:                    
                    data_df = collect_function(iata,tipo)
                    data_df['Tipo'] = tipo
                    data_df['Aeroporto'] = nome
                    return data_df
                except (TimeoutException, socket.timeout, 
                        urllib3.exceptions.MaxRetryError, urllib3.exceptions.NewConnectionError, 
                        urllib3.exceptions.ReadTimeoutError, requests.exceptions.ConnectionError, 
                        requests.exceptions.Timeout, WebDriverException) as e:                            
                            
                            retries += 1                           
                            print(f"Falha na coleta para {tipo} no aeroporto {airport} após {retries} tentativas. Erro: {str(e)}")
                            
                            time.sleep(2)

            return pd.DataFrame()                       
        
        arrivals_df = try_collect(airport, 'arrivals')
        time.sleep(1)
        departures_df = try_collect(airport, 'departures')

        all_data.append(arrivals_df)
        all_data.append(departures_df)
        
        print(f"Dados coletados para o aeroporto: {airport} - {nome}")
        print("---")
        
   
    final_df = pd.concat(all_data, ignore_index=True)
    
    return final_df





df_final = collect_data_from_airports(brazil_airports, coletar_voos)

if df_final.empty:
    print("Nenhum dado encontrado! Finalizando o script.")         
    exit()  
else:

    data_hoje = datetime.today()
    data_ontem = data_hoje - timedelta(days=1)
    data_filtro = data_ontem.strftime('%Y-%m-%d')        
    
    voos = df_final[df_final['date_flight']==data_filtro]
    
    connect_str = os.environ['CONNECT_STR']
    container_name = os.environ['CONTAINER_NAME']
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)
    
    parquet_buffer = BytesIO()
    voos.to_parquet(parquet_buffer, index=False)
    
    parquet_data = parquet_buffer.getvalue()
    
    blob_name = f"voos_{data_filtro}_bronze.parquet"
    
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(parquet_data, overwrite=True)
