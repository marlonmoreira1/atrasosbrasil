import os
import pandas as pd
from movedata import save, read

connect_str = os.environ['CONNECT_STR']

ouro_container = os.environ['CONTAINER_OURO']

prata_container = os.environ['CONTAINER_PRATA']

voos = read(connect_str, prata_container, "prata")

voos['Companhia_Aerea'] = (
    voos['Airline']
    .astype(str)
    .str.replace(r'\s*\(.*?\)', '', regex=True)
)

voos['Companhia_Aerea'] = (
    voos['Companhia_Aerea']
    .str.replace(r'\-$', '', regex=True)
)

voos[['Aeronave', 'Modelo_Aeronave']] = (
    voos['Aircraft']
    .astype(str)
    .str.extract(r'(.+)\((.+)\)')
)

voos[['Status', 'Hora_Realizada', 'AM-PM_Realizado']] = (
    voos['Status']
    .astype(str)
    .str.extract(r'([a-zA-Z\s\.]+)(\d{1,2}:\d{2})?\s?(AM|PM)?')
)

voos[['Hora_Prevista', 'AM-PM_Previsto']] = (
    voos['Time']
    .astype(str)
    .str.extract(r'(\d{1,2}:\d{2})\s?(AM|PM)')
)

def obter_nacionalidade(row):

    if row != 'Brazil':
        return 'Internacional'

    return 'Nacional'

voos['Tipo_Voo_Nacional'] = (
    voos['country']
    .apply(obter_nacionalidade)
)

voos['Voo_Id'] = (
    voos['Flight'].astype(str)
    + voos['date_flight'].astype(str)
    + voos['Aeroporto'].astype(str)
    + voos['Aircraft'].astype(str)
    + voos['city_normalized'].astype(str)
    + voos['Status'].astype(str)
)

voos = voos.drop(
    columns=['Time','Airline','Aircraft']
)

colunas_para_renomear = {
    'Flight': 'Numero_Voo',
    'Status': 'Status_voo',
    'Delay_status': 'Indicador_Atraso_Cor',
    'date_flight': 'Data_Voo',
    'city_normalized': 'Cidade_Normalizada',
    'city': 'Cidade_ascii',
    'admin_name': 'Estado',
    'country': 'Pais'
}

voos = voos.rename(columns=colunas_para_renomear)

save(voos, connect_str, ouro_container, "ouro")
