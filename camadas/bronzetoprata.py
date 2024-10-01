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

voos[['Cidade', 'Aeroporto_iatacode']] = voos['From'].str.extract(r'(.+)\((.+)\)-')

url = "https://simplemaps.com/static/data/world-cities/basic/simplemaps_worldcities_basicv1.75.zip"
response = requests.get(url)
zip_file = zipfile.ZipFile(BytesIO(response.content))

with zip_file.open('worldcities.csv') as file:
    df_cidades = pd.read_csv(file)

df_cidades['city_normalized'] = df_cidades['city'].apply(lambda x: unidecode(str(x)))

def obter_informacoes_geograficas(cidade):
    resultado = df_cidades[df_cidades['city_normalized'].str.lower() == cidade.lower()][['city_normalized','city', 'admin_name', 'country']].values
    if len(resultado) > 0:
        cidade_normalizada, cidade, estado, pais = resultado[0]
        return cidade_normalizada, cidade, estado, pais
    else:
        return None, None, None,None

voos[['city_normalized','city', 'admin_name', 'country']] = voos['Cidade'].apply(lambda x: pd.Series(obter_informacoes_geograficas(x)))

voos = voos.drop(columns=['From'])

save(novo_voos,connect_str,prata_container,"prata")

