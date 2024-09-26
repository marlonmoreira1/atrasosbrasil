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

connect_str = os.environ['CONNECT_STR']
bronze_container = os.environ['CONTAINER_NAME']
prata_container = os.environ['CONTAINER_PRATA']

voos = read(connect_str,bronze_container,"bronze")

voos[['From', 'Aeroporto']] = voos['From'].str.extract(r'(.+)\((.+)\)-')

voos[['Status', 'Hora_realizada', 'AM-PM_Realizado']] = voos['Status'].str.extract(r'([a-zA-Z\s\.]+)(\d{1,2}:\d{2})?\s?(AM|PM)?')
voos[['Time', 'AM-PM_Previsto']] = voos['Time'].str.extract(r'(\d{1,2}:\d{2})\s?(AM|PM)')

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

print(voos[['Cidade_Correta', 'Estado/Província', 'País']].head())
save(voos,connect_str,prata_container,"prata")

