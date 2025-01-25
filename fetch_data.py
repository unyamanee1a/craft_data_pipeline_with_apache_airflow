import os
import requests
import zipfile


def fetch_greenary_data():
    url = "https://www.kaggle.com/api/v1/datasets/download/napatsakornpianchana/greenary-data-for-airflow-class"

    response = requests.get(url, stream=True)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch data, status code: {response.status_code}")

    zip_file_path = os.path.join("", "data.zip")

    with open(zip_file_path, "wb") as file: 
        for chunk in response.iter_content(chunk_size=128):
            file.write(chunk)

    print(f"Save zip file to {zip_file_path}")


def extract_zip():
    zip_file_path = os.path.join("", "data.zip")
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall("")

extract_zip()