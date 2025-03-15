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

def coletar_voos(iata,tipo):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "Referer": "https://www.flightradar24.com/",
        "Origin": "https://www.flightradar24.com"
    }
    
    registros = []

    now = datetime.now()

    # Converter para timestamp
    timestamp = int(now.timestamp())

    for page in range(1, -11, -1):  
        url = (f"https://api.flightradar24.com/common/v1/airport.json?"
               f"code={iata}&plugin[]=&plugin-setting[schedule][mode]={tipo}&"
               f"plugin-setting[schedule][timestamp]={timestamp}&page={page}&limit=100")

        session = requests.Session()
        response = session.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            flights = data.get("result", {}) \
                          .get("response", {}) \
                          .get("airport", {}) \
                          .get("pluginData", {}) \
                          .get("schedule", {}) \
                          .get(tipo, {}) \
                          .get("data", [])

            for flight_info in flights:
                flight = flight_info.get("flight", {})

                flight_number = flight.get("identification", {}).get("number", {}).get("default") or None
                status_text   = flight.get("status", {}).get("text") or None
                status_icon   = flight.get("status", {}).get("icon") or None
                aircraft_code = flight.get("aircraft", {}).get("model", {}).get("code") or None
                registration  = flight.get("aircraft", {}).get("registration") or None
                airline_info  = flight.get("airline") or None
                airline_name  = airline_info.get("name", None) if airline_info else None
                status_text = flight.get("status", {}).get("generic", {}).get("status", {}).get("text") or None
                utc_time = flight.get("status", {}).get("generic", {}).get("eventTime", {}).get("utc") or None
                
                
                if tipo == 'arrivals':
                    destination = flight.get("airport", {}).get("origin") or None
                    city = destination.get("position", {}).get("region", {}).get("city", None) if destination else None
                    airport_iata = destination.get("code", {}).get("iata", None) if destination else None
                    real_departure_ts = flight.get("time", {}).get("scheduled", {}).get("arrival") or None
                    
                elif tipo == 'departures':
                    destination = flight.get("airport", {}).get("destination") or None
                    city = destination.get("position", {}).get("region", {}).get("city", None) if destination else None
                    airport_iata = destination.get("code", {}).get("iata", None) if destination else None
                    real_departure_ts = flight.get("time", {}).get("scheduled", {}).get("departure") or None                    
                

                
                if city and airport_iata:
                    from_location = f"{city}({airport_iata})-"
                else:
                    from_location = None                  
                
                
                if real_departure_ts:
                    flight_date = datetime.fromtimestamp(real_departure_ts).strftime("%Y-%m-%d")
                    departure_time = datetime.fromtimestamp(real_departure_ts).strftime("%I:%M %p")
                else:
                    flight_date = None
                    departure_time = None


                if utc_time:
                    utc_time_cleaned = datetime.fromtimestamp(utc_time).strftime("%I:%M %p")
                else:
                    utc_time_cleaned = None
                    
                if status_text and utc_time_cleaned:                    
                    status_real = f"{status_text}{utc_time_cleaned}"
                else:
                    status_real = None
                    
                aircraft_total = f"{aircraft_code}({registration})"
                
                
                registro = {
                    "Time": departure_time,
                    "Flight": flight_number,
                    "From": from_location,                    
                    "Airline": airline_name,
                    "Aircraft": aircraft_total,
                    "Status": status_real,
                    "Delay_status": status_icon,
                    "date_flight": flight_date
                }
                registros.append(registro)        
        
        else:
            print(f"Erro na página {page}: {response.status_code}")

        time.sleep(1)
    
    
    df = pd.DataFrame(registros)
    df = df.drop_duplicates()
    return df


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
    'BPS': 'Porto Seguro - Aeroporto de Porto Seguro',
    'NVT': 'Navegantes - Aeroporto Internacional de Navegantes',
    'IGU': 'Foz do Iguaçu - Aeroporto Internacional de Foz do Iguaçu',
    'CXJ': 'Caxias do Sul - Aeroporto Regional Hugo Cantergiani',
    'LDB': 'Londrina - Aeroporto de Londrina',
    'JOI': 'Joinville - Aeroporto de Joinville',
    'UDI': 'Uberlândia - Aeroporto de Uberlândia',    
    'RAO': 'Ribeirão Preto - Aeroporto Leite Lopes',
    'MGF': 'Maringá - Aeroporto de Maringá'
}            

def collect_data_from_airports(airports, collect_function):
    """
    Itera sobre um dicionário de aeroportos, chama a função de coleta de dados para cada um
    e retorna um dataframe combinado com todos os dados.
    
    :param airports: Dicionário de códigos IATA dos aeroportos e seus nomes
    :param collect_function: Função que coleta dados para um aeroporto específico e retorna um DataFrame
    :param delay: Tempo de espera entre as chamadas (em segundos)
    :return: DataFrame combinado com dados de todos os aeroportos
    """    
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
        
        arrivals_df = try_collect(airport.lower(), 'arrivals')
        time.sleep(1)
        departures_df = try_collect(airport.lower(), 'departures')

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

