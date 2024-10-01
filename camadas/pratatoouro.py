import os
import time
import zipfile
import requests
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
from azure.storage.blob import BlobServiceClient
from unidecode import unidecode
from movedata import save, read

connect_str = os.environ['CONNECT_STR']
ouro_container = os.environ['CONTAINER_OURO']
prata_container = os.environ['CONTAINER_PRATA']


voos = read(connect_str,prata_container,"prata")

voos['Airline'] = voos['Airline'].str.replace(r'\s*\(.*?\)-', '', regex=True)
voos['Airline'] = voos['Airline'].str.replace(r'\-$', '', regex=True)

voos[['Status', 'Hora_realizada', 'AM-PM_Realizado']] = voos['Status'].str.extract(r'([a-zA-Z\s\.]+)(\d{1,2}:\d{2})?\s?(AM|PM)?')
voos[['Time', 'AM-PM_Previsto']] = voos['Time'].str.extract(r'(\d{1,2}:\d{2})\s?(AM|PM)')

voos[['Aircraft', 'Aircraft_type']] = voos['Aircraft'].str.extract(r'(.+)\((.+)\)')

def obter_nacionalidade(row):
    if row != 'Brazil':
        return 'Internacional'
    return 'Nacional'

voos['Is_National'] = voos['País'].apply(obter_nacionalidade)

voos['Voo_Id'] = voos['Flight'] + voos['date_flight'] + voos['Aeroporto'] + voos['Aircraft']

colunas_traduzidas = {
    'Time': 'Hora_Prevista',
    'Flight': 'Numero_Voo',    
    'Airline': 'Companhia_Aerea',
    'Aircraft': 'Modelo_Aeronave',
    'Status': 'Status_Voo',
    'Delay_status': 'Indicador_Atraso_Cor',
    'date_flight': 'Data_Voo',
    'direcao': 'Direcao_Voo',
    'Aeroporto': 'Nome_Aeroporto',
    'Aircraft_type': 'Tipo_Aeronave',
    'Hora_realizada': 'Hora_Realizada',
    'admin_name': 'Estado',
    'country': 'Pais',
    'Is_National': 'Tipo_Voo_Nacional',
    'city_normalized': 'Cidade_Normalizada',
    'city': 'Cidade_ascii',
    'AM-PM_Previsto': 'Periodo_Previsto',
    'AM-PM_Realizado': 'Periodo_Realizado'   
    
}

voos = voos.rename(columns=colunas_traduzidas)


save(voos,connect_str,ouro_container,"ouro")
