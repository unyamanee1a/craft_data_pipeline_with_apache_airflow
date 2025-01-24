import os
import pandas as pd
import zipfile
import requests
import json
import os
import sys

from google.cloud import storage
from google.oauth2 import service_account

def fetch_greenery_data():
    url = "https://www.kaggle.com/api/v1/datasets/download/napatsakornpianchana/greenary-data-for-airflow-class"

    response = requests.get(url, stream=True)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch data, status code: {response.status_code}")

    zip_file_path = os.path.join("./data", "data.zip")

    with open(zip_file_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=128):
            file.write(chunk)

    print(f"Saved zip file to {zip_file_path}")
    extract_zip()
    load_to_gcs()


def extract_zip():
    zip_file_path = os.path.join("./data", "data.zip")
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall("./data")


def load_to_gcs(local_path):
    bucket_name = "test-load-to-gcs"
    source_file_name = local_path
    file_name = local_path.split("/")[1]
    destination_blob_name = f"raw-data/greenary/{file_name}"
    project_id = "pelagic-berm-448713-d4"
    keyfile = "gcs-credential.json"

    # keyfile = os.environ.get("KEYFILE_PATH")
    service_account_info = json.load(open(keyfile))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)

    storage_client = storage.Client(
        project=project_id,
        credentials=credentials,
    )
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

def walk_directory():
    local_folder = "data"
    for root, dirs, files in os.walk(local_folder):
        for file in files:
            local_file = os.path.join(root, file)
            print(local_file)
            load_to_gcs(local_file)

walk_directory()
# fetch_greenery_data()