import os
from azure.storage.blob import BlobServiceClient

CONNECT_STR = os.environ["CONNECT_STR"]

SOURCE_CONTAINER = os.environ["SOURCE_CONTAINER"]

DEST_CONTAINER = os.environ["DEST_CONTAINER"]

blob_service_client = (
    BlobServiceClient
    .from_connection_string(CONNECT_STR)
)

source_container = (
    blob_service_client
    .get_container_client(SOURCE_CONTAINER)
)

dest_container = (
    blob_service_client
    .get_container_client(DEST_CONTAINER)
)

blobs = source_container.list_blobs()

for blob in blobs:

    source_blob = (
        source_container
        .get_blob_client(blob.name)
    )

    data = source_blob.download_blob().readall()

    dest_blob = (
        dest_container
        .get_blob_client(blob.name)
    )

    dest_blob.upload_blob(
        data,
        overwrite=True
    )

    source_blob.delete_blob()

    print(f"{blob.name} movido")
