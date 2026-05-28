```python
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from datetime import datetime, timedelta
import pandas as pd

def read(connect_str, container, camada):

    blob_service_client = BlobServiceClient.from_connection_string(
        connect_str
    )

    container_client = blob_service_client.get_container_client(
        container
    )

    yesterday = datetime.utcnow() - timedelta(days=1)

    blob_name = f"voos_{yesterday.strftime('%Y-%m-%d')}_{camada}.parquet"

    blob_client = container_client.get_blob_client(blob_name)

    data = blob_client.download_blob().readall()

    return pd.read_parquet(BytesIO(data))

def save(df, connect_str, container, camada):

    blob_service_client = BlobServiceClient.from_connection_string(
        connect_str
    )

    container_client = blob_service_client.get_container_client(
        container
    )

    yesterday = datetime.utcnow() - timedelta(days=1)

    blob_name = f"voos_{yesterday.strftime('%Y-%m-%d')}_{camada}.parquet"

    buffer = BytesIO()

    df.to_parquet(buffer, index=False)

    blob_client = container_client.get_blob_client(blob_name)

    blob_client.upload_blob(
        buffer.getvalue(),
        overwrite=True
    )
```
