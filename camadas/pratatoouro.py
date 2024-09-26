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

voos[['From', 'Aeroporto']] = voos['From'].str.extract(r'(.+)\((.+)\)-')

voos['Airline'] = voos['Airline'].str.replace(r'\s*\(.*?\)-', '', regex=True)
voos['Airline'] = voos['Airline'].str.replace(r'\-$', '', regex=True)

voos[['Aircraft', 'Aircraft_type']] = voos['Aircraft'].str.extract(r'(.+)\((.+)\)')

def obter_nacionalidade(row):
    if row != 'Brazil':
        return 'Internacional'
    return 'Nacional'

voos['Is_National'] = voos['País'].apply(obter_nacionalidade)


def is_null(row):
    if pd.isna(row['Time']) or pd.isna(row['Hora_realizada']): 
        return row['Hora_realizada']
    return None


def convert_to_24h(time_str, am_pm,status,tipo):
    if status == 'Known' and tipo == 'realizado':
        time_obj = datetime.strptime(time_str, '%H:%M')
        return time_obj
        
    time_obj = datetime.strptime(time_str, '%I:%M')
    
    if am_pm == 'PM' and time_obj.hour != 12:
        time_obj += timedelta(hours=12)
    elif am_pm == 'AM' and time_obj.hour == 12:
        time_obj -= timedelta(hours=12)
    return time_obj
    

def obter_atraso_flag(row):
    
    result = is_null(row)

    if result is not None:
        return result
    
    hora_prevista = convert_to_24h(row['Time'], row['AM-PM_Previsto'],row['Status'],'previsto')
    hora_realizada = convert_to_24h(row['Hora_realizada'], row['AM-PM_Realizado'],row['Status'],'realizado')

    _,flag = obter_diff(hora_prevista,hora_realizada,row['AM-PM_Previsto'],row['AM-PM_Realizado'])
    return flag
    

def obter_diff(hora_prevista,hora_realizada,am_pm_previsto,am_pm_realizado):
    
    if hora_prevista.hour == 0 and (am_pm_previsto == 'AM' and am_pm_realizado == 'PM'):
        atraso = hora_prevista - hora_realizada
        flag = 'ON-Time'
    elif hora_prevista.hour == 12 and (am_pm_previsto == 'PM' and am_pm_realizado == 'AM'):
        atraso = hora_prevista - hora_realizada
        flag = 'ON-Time'
    elif hora_prevista >= hora_realizada and (am_pm_previsto == am_pm_realizado):
        atraso = hora_prevista - hora_realizada
        flag = 'ON-Time'
    else:
        atraso = hora_realizada - hora_prevista
        flag = 'Atrasado'
    
    if atraso < timedelta(0):
        atraso += timedelta(days=1)
    return atraso, flag


def obter_atraso_tempo(row):
    
    result = is_null(row)

    if result is not None:
        return result
    
    hora_prevista = convert_to_24h(row['Time'], row['AM-PM_Previsto'],row['Status'],'previsto')
    hora_realizada = convert_to_24h(row['Hora_realizada'], row['AM-PM_Realizado'],row['Status'],'realizado')
    
    atraso,_ = obter_diff(hora_prevista,hora_realizada,row['AM-PM_Previsto'],row['AM-PM_Realizado'])
    
    horas = atraso.seconds // 3600
    minutos = (atraso.seconds % 3600) // 60
    return f"{horas:02}:{minutos:02}"
            

voos['Flag'] = voos.apply(obter_atraso_flag,axis=1)

voos['Atraso\Antecipado'] = voos.apply(obter_atraso_tempo,axis=1)

def obter_status_real(row):
    if row['Status'] == 'Canceled':
        return row['Status']
    elif 'Diverted' in row['Status']:
        return 'Diverted'
        
    elif (row['Delay_status'] == 'red' and not (row['Status'] == 'Canceled' or 'Diverted' in row['Status']))\
    or (row['Delay_status'] == 'yellow' and pd.to_datetime(row['Atraso\Antecipado']) > pd.to_datetime('00:15'))\
    or (row['Flag'] == 'Atrasado' and pd.to_datetime(row['Atraso\Antecipado']) > pd.to_datetime('00:15')):
        return 'Delayed'
        
    elif row['Delay_status'] == 'gray'and not row['Status'] == 'Known':
        return 'Unknown'
    return 'ON-TIME'


voos['Voo_Status_Real'] = voos.apply(obter_status_real,axis=1)

colunas_traduzidas = {
    'Time': 'Hora_Prevista',
    'Flight': 'Numero_Voo',
    'From': 'Cidade_Origem',
    'Airline': 'Companhia_Aerea',
    'Aircraft': 'Modelo_Aeronave',
    'Status': 'Status_Voo',
    'Delay_status': 'Indicador_Atraso',
    'date_flight': 'Data_Voo',
    'direcao': 'Direcao_Voo',
    'Aeroporto': 'Nome_Aeroporto',
    'Aircraft_type': 'Tipo_Aeronave',
    'Hora_realizada': 'Hora_Realizada',
    'Estado/Província': 'Estado_Provincia_Origem',
    'País': 'Pais_Origem',
    'Is_National': 'Tipo_Voo_Nacional',
    'Cidade_Correta': 'Cidade_Origem_Normalizada',
    'AM-PM_Previsto': 'Periodo_Previsto',
    'AM-PM_Realizado': 'Periodo_Realizado',    
    'Flag': 'Indicador_Atraso',
    'Atraso\Antecipado': 'Tempo_Atraso_Antecipacao',
    'Voo_Status_Real': 'Status_Real_Voo'
}

voos = voos.rename(columns=colunas_traduzidas)


save(voos,connect_str,ouro_container,"ouro")
