import pandas as pd
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from datetime import datetime
import requests
from unidecode import unidecode
import zipfile
import os


connect_str = os.environ['CONNECT_STR']
bronze_container = os.environ['CONTAINER_NAME']
prata_container = os.environ['CONTAINER_PRATA']

def read_from_bronze():
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(bronze_container)

    data_hoje = datetime.today()
    data_ontem = data_hoje - timedelta(days=1)
    data_filtro = data_ontem.strftime('%Y-%m-%d')    
   
    blob_client = container_client.get_blob_client(f"voos_{data_filtro}_bronze.parquet")
    
    stream = blob_client.download_blob()
    data = stream.readall()
    
    parquet_buffer = BytesIO(data)
    df = pd.read_parquet(parquet_buffer)
    
    return df
  

def save_to_prata(df):
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(prata_container)
    
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_data = parquet_buffer.getvalue()
    
    data_hoje = datetime.today()
    data_ontem = data_hoje - timedelta(days=1)
    data_filtro = data_ontem.strftime('%Y-%m-%d')

    blob_name = f"voos_{data_filtro}_prata.parquet"
    
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(parquet_data, overwrite=True)


voos = read_from_bronze()


voos[['Status', 'Hora_realizada', 'AM-PM_Realizado']] = voos['Status'].str.extract(r'([a-zA-Z\s\.]+)(\d{1,2}:\d{2})?\s?(AM|PM)?')
voos[['Time', 'AM-PM_Previsto']] = voos['Time'].str.extract(r'(\d{1,2}:\d{2})\s?(AM|PM)')


url = "https://simplemaps.com/static/data/world-cities/basic/simplemaps_worldcities_basicv1.75.zip"
response = requests.get(url)
zip_file = zipfile.ZipFile(BytesIO(response.content))

with zip_file.open('worldcities.csv') as file:
    df = pd.read_csv(file)

df['city_normalized'] = df['city'].apply(lambda x: unidecode(str(x)))

def obter_informacoes_geograficas(cidade):
    resultado = df[df['city_normalized'].str.lower() == cidade.lower()][['city','admin_name', 'country']].values
    if len(resultado) > 0:
        cidade, estado, pais = resultado[0]
        return cidade, estado, pais
    else:
        return None, None, None

voos[['Cidade_Correta', 'Estado/Província', 'País']] = voos['From'].apply(lambda x: pd.Series(obter_informacoes_geograficas(x)))

save_to_prata(voos)

