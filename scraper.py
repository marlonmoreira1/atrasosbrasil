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
from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException
import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from io import BytesIO

options = Options()
options.add_argument('--headless')  
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
driver.set_page_load_timeout(600)  

def fechar_overlay():
    try:        
        overlay = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "onetrust-pc-dark-filter"))
        )
        fechar_botao = driver.find_element(By.ID, "onetrust-accept-btn-handler")
        fechar_botao.click()
    except Exception as e:
        pass  # Ignorando o erro, pois o overlay pode não aparecer sempre


def obter_voos(url):
    import time
    url = url
    driver.get(url)

    fechar_overlay()

    while True:
        try:
            load_more_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[@class='btn btn-table-action btn-flights-load']")))
                    
            if load_more_button and load_more_button.is_displayed() and load_more_button.is_enabled():
                load_more_button.click()                
                time.sleep(1)  
            else:
                print("Botão 'Carregar mais' não está clicável ou não está visível.")
                break
        except:
            break
            
    time.sleep(1)
  
    element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, "//table[contains(@class, 'table-condensed') and contains(@class, 'table-hover') and contains(@class, 'data-table')]"))
    )      

    html_content = element.get_attribute('outerHTML')
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    table = soup.find('table', class_='table table-condensed table-hover data-table m-n-t-15')
    flights = []
    
    if table:
        rows = table.find('tbody').find_all('tr')
        
        for row in rows:
            columns = row.find_all('td')
            if len(columns) > 1:

                time = columns[0].get_text(strip=True)
                flight = columns[1].get_text(strip=True)
                origin = columns[2].get_text(strip=True)
                airline = columns[3].get_text(strip=True)
                aircraft = columns[4].get_text(strip=True)
                status = columns[6].get_text(strip=True)
                status_div = row.find('div', class_='state-block')
                status_color = status_div.get('class')[1] if status_div else 'unknown'
                data_date = row.get('data-date')
                first_date_obj = datetime.strptime(data_date, '%A, %b %d').replace(year=datetime.now().year)
                first_date_str = first_date_obj.strftime('%Y-%m-%d')
                
                flights.append({
                    'Time': time,
                    'Flight': flight,
                    'From': origin,
                    'Airline': airline,
                    'Aircraft': aircraft,
                    'Status': status,
                    'Delay_status': status_color,
                    'date_flight': first_date_str
                })
    voos = pd.DataFrame(flights)

    return voos


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

        def try_collect(url, tipo):
            retries = 0
            max_retries = 15
            while retries < max_retries:
                try:
                    data_df = collect_function(url)
                    data_df['Tipo'] = tipo
                    data_df['Aeroporto'] = nome
                    return data_df
                except TimeoutException:
                    retries += 1                    
                    time.sleep(1)
                    print(f"Falha na coleta para {tipo} no aeroporto {airport} após {retries} tentativas.")
        
        arrivals_df = try_collect(f"https://www.flightradar24.com/data/airports/{airport.lower()}/arrivals", 'Chegada')
        time.sleep(1)
        departures_df = try_collect(f"https://www.flightradar24.com/data/airports/{airport.lower()}/departures", 'Partida')

        all_data.append(arrivals_df)
        all_data.append(departures_df)
        
        print(f"Dados coletados para o aeroporto: {airport} - {nome}")
        print("---")
        time.sleep(1)
   
    final_df = pd.concat(all_data, ignore_index=True)
    
    return final_df

df_final = collect_data_from_airports(brazil_airports, obter_voos)

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
