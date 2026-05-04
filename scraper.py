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

    response = requests.get(url, timeout=10)

    if response.status_code != 200:
        print(f"Erro: {response.status_code}")
        return pd.DataFrame()

    data = response.json().get("response", [])

    registros = []

    for flight in data:
        try:
            flight_number = flight.get("flight_iata")

            airline_name = flight.get("airline_name")

            departure_time = flight.get("dep_time")
            arrival_time = flight.get("arr_time")

            # Escolhe o horário certo baseado no tipo
            if tipo == "arrivals":
                time_raw = arrival_time
                city = flight.get("dep_city")
                airport_iata = flight.get("dep_iata")
            else:
                time_raw = departure_time
                city = flight.get("arr_city")
                airport_iata = flight.get("arr_iata")

            if time_raw:
                dt = datetime.fromisoformat(time_raw.replace("Z", "+00:00"))
                flight_date = dt.strftime("%Y-%m-%d")
                departure_time_fmt = dt.strftime("%I:%M %p")
            else:
                flight_date = None
                departure_time_fmt = None

            from_location = f"{city}({airport_iata})-" if city and airport_iata else None

            status_text = flight.get("status")
            status_real = status_text

            aircraft = flight.get("aircraft_icao") or ""
            registration = flight.get("reg_number") or ""
            aircraft_total = f"{aircraft}({registration})"

            registro = {
                "Time": departure_time_fmt,
                "Flight": flight_number,
                "From": from_location,
                "Airline": airline_name,
                "Aircraft": aircraft_total,
                "Status": status_real,
                "Delay_status": status_text,  # pode melhorar depois
                "date_flight": flight_date
            }

            registros.append(registro)

        except Exception as e:
            print(f"Erro parsing: {e}")
            continue

    df = pd.DataFrame(registros)
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
    'VIX': 'Vitória - Aeroporto de Vitória',
    'FLN': 'Florianópolis - Aeroporto Internacional Hercílio Luz',
    'GYN': 'Goiânia - Aeroporto Internacional Santa Genoveva',
    'NAT': 'Natal - Aeroporto Internacional Aluízio Alves',
    'MCZ': 'Maceió - Aeroporto Internacional Zumbi dos Palmares',
    'CGR': 'Campo Grande - Aeroporto Internacional de Campo Grande',
    'SLZ': 'São Luís - Aeroporto Internacional de São Luís',
    'CGB': 'Cuiabá - Aeroporto Internacional Marechal Rondon',
    'THE': 'Teresina - Aeroporto de Teresina',
    'AJU': 'Aracaju - Aeroporto de Aracaju',
    'PVH': 'Porto Velho - Aeroporto Internacional de Porto Velho',    
    'BVB': 'Boa Vista - Aeroporto Internacional de Boa Vista',
    'RBR': 'Rio Branco - Aeroporto Internacional de Rio Branco',
    'PMW': 'Palmas - Aeroporto de Palmas',
    'JPA': 'João Pessoa - Aeroporto Internacional Presidente Castro Pinto',
    'POA': 'Porto Alegre - Aeroporto Internacional Salgado Filho',
    'MCP': 'Aeroporto Internacional de Macapá - Alberto Alcolumbre',
    
    # Outros aeroportos relevantes    
    'VCP': 'Campinas - Aeroporto Internacional de Viracopos',    
    'UDI': 'Uberlândia - Aeroporto de Uberlândia'   
    
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
