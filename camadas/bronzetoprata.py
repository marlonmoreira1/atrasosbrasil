import os
import time
import zipfile
import requests
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
from azure.storage.blob import BlobServiceClient
from unidecode import unidecode
from movedata import save, read

options = Options()
options.add_argument('--headless')  
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)


def converter_data(data_str):
    """Converte uma data no formato YYYY-MM-DD para o formato 'ddd, d mmm'"""
    data_obj = datetime.strptime(data_str, "%Y-%m-%d")
    dias_semana = ["seg", "ter", "qua", "qui", "sex", "sab","dom"]
    meses = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]

    dia_semana = dias_semana[data_obj.weekday()]
    dia = data_obj.day
    mes = meses[data_obj.month - 1]

    return f"{dia_semana}, {dia} {mes}"

def buscar_horario_chegada(numero_voo, data_desejada):
    """Busca os horários de chegada dos voos com status Unknown do FlightRadar."""
    base_url = "https://br.trip.com/flights/status-"
    url = base_url + numero_voo      
    driver.get(url)
    
    time.sleep(5)

    try:        
        data_formatada = converter_data(data_desejada)        
        soup = BeautifulSoup(driver.page_source, 'html.parser')        
        tabela = soup.find('table')
        
        if tabela:
            linhas = tabela.find_all('tr')
            for linha in linhas:
                colunas = linha.find_all('td')
                if len(colunas) > 6:
                    data_linha = colunas[1].text.strip() 
                    chegada = colunas[7].text.strip()
                    status = colunas[8].text.strip()
                    if status == 'Cancelado':
                        return status
                    elif data_formatada == data_linha:
                        return chegada
        return "Tabela não encontrada"
    except Exception as e:
        print(f"Erro ao buscar dados: {e}")
        return "Erro"

def atualizar_hora(row):
    if row['Status'] == 'Unknown':             
        horario = buscar_horario_chegada(row['Flight'], row['date_flight'])
        if horario in ['Cancelado', 'Tabela não encontrada', '--:--']:
            return row['Hora_realizada']
        return horario
    return row['Hora_realizada']

def atualizar_status(row):
    if row['Status'] == 'Unknown':
        status = buscar_horario_chegada(row['Flight'], row['date_flight'])
        if status == 'Cancelado':
            return 'Canceled'
        elif status in ['Tabela não encontrada', '--:--']:
            return row['Status']
        return 'Known'
    return row['Status']

def am_pm_realizado(row):
    if row['Status'] == 'Known':
        hora_realizada = pd.to_datetime(row['Hora_realizada'])
        return 'PM' if hora_realizada.hour > 12 else 'AM'
    return row['AM-PM_Realizado']



connect_str = os.environ['CONNECT_STR']
bronze_container = os.environ['CONTAINER_NAME']
prata_container = os.environ['CONTAINER_PRATA']


voos = read(connect_str,bronze_container,"bronze")


voos[['Status', 'Hora_realizada', 'AM-PM_Realizado']] = voos['Status'].str.extract(r'([a-zA-Z\s\.]+)(\d{1,2}:\d{2})?\s?(AM|PM)?')
voos[['Time', 'AM-PM_Previsto']] = voos['Time'].str.extract(r'(\d{1,2}:\d{2})\s?(AM|PM)')

voos['Hora_realizada'] = voos.apply(atualizar_hora, axis=1)
voos['Status'] = voos.apply(atualizar_status, axis=1)
voos['AM-PM_Realizado'] = voos.apply(am_pm_realizado, axis=1)


url = "https://simplemaps.com/static/data/world-cities/basic/simplemaps_worldcities_basicv1.75.zip"
response = requests.get(url)
zip_file = zipfile.ZipFile(BytesIO(response.content))

with zip_file.open('worldcities.csv') as file:
    df_cidades = pd.read_csv(file)

df_cidades['city_normalized'] = df_cidades['city'].apply(lambda x: unidecode(str(x)))

def obter_informacoes_geograficas(cidade):
    resultado = df_cidades[df_cidades['city_normalized'].str.lower() == cidade.lower()][['city', 'admin_name', 'country']].values
    if len(resultado) > 0:
        cidade, estado, pais = resultado[0]
        return cidade, estado, pais
    else:
        return None, None, None

voos[['Cidade_Correta', 'Estado/Província', 'País']] = voos['From'].apply(lambda x: pd.Series(obter_informacoes_geograficas(x)))


save(voos,connect_str,prata_container,"prata")

