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

def normalize_city_name(city_name):
    city_name = str(city_name).lower()    
    city_name = city_name.strip()    
    city_name = ' '.join(word.capitalize() for word in city_name.lower().split())
    return city_name

airports = pd.read_csv("data/airports.csv",header=0,delimiter=',')
countries = pd.read_csv("data/countries.csv",header=0,delimiter=',')
states = pd.read_csv("data/regions.csv",header=0,delimiter=',')

states['region'] = states['name']
countries['country'] = countries['name']

airports = airports.merge(states[['region','code']],
                                        left_on=['iso_region'],
                                        right_on=['code'],
                                        how='left')

airports = airports.merge(countries[['country','code']],
                                        left_on=['iso_country'],
                                        right_on=['code'],
                                        how='left')

just_airports = airports[airports['iata_code'].notna()]

just_airports['municipality'] = just_airports['municipality'].str.replace(r"\(.*\)", "", regex=True).str.strip()

just_airports['municipality'] = just_airports['municipality'].apply(normalize_city_name)

just_airports['city_normalized'] = just_airports['municipality'].apply(lambda x: unidecode(str(x)))

def obter_informacoes_geograficas(cidade,iata_code):
    cidade_str = str(cidade).lower()
    iata_code_str = str(iata_code).lower()
    resultado = just_airports[(just_airports['city_normalized'].str.lower() == cidade_str) & (just_airports['iata_code'].str.lower() == iata_code_str)][['city_normalized','municipality', 'region', 'country']].values
    if len(resultado) > 0:
        cidade_normalizada, cidade, estado, pais = resultado[0]
        return cidade_normalizada, cidade, estado, pais
    else:
        return None, None, None,None

voos[['city_normalized','city', 'admin_name', 'country']] = voos[['Cidade','Aeroporto_iatacode']].apply(lambda x: pd.Series(obter_informacoes_geograficas(x['Cidade'],x['Aeroporto_iatacode'])),axis=1)

voos = voos.drop(columns=['From'])

save(voos,connect_str,prata_container,"prata")
