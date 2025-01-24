from airflow.decorators import dag, task
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
import json
import os
import pandas as pd
import zipfile
import requests

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Airflow DAG for data pipeline
@dag(
    dag_id="greenery_to_bigquery",
    default_args=default_args,
    description="Fetch greenery data from API and push to BigQuery",
    schedule_interval="@daily",
    catchup=False,
    tags=["greenery", "bigquery"]
)
def greenery_to_bigquery():

    @task()
    def fetch_greenery_data():
        # Example URL from OpenWeather API (replace with a valid one)
        url = "https://www.kaggle.com/api/v1/datasets/download/napatsakornpianchana/greenary-data-for-airflow-class"

        response = requests.get(url, stream=True)

        if response.status_code != 200:
            raise Exception(f"Failed to fetch data, status code: {response.status_code}")

        zip_file_path = os.path.join("./downloads", "data.zip")

        with open(zip_file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=128):
                file.write(chunk)

        print(f"Saved zip file to {zip_file_path}")

    @task
    def extract_zip():
        zip_file_path = os.path.join("./downloads", "data.zip")
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall("./downloads")

    @task
    def load_to_gcs(local_path):

        for path in local_path:
            bucket_name = "test-load-to-gcs"
            source_file_name = path
            file_name = path.split("/")[1]
            destination_blob_name = f"raw-data/greenary/{file_name}"
            project_id = "pelagic-berm-448713-d4" # Set your project ID
            keyfile = "/opt/airflow/gcs-credential.json"
            if not os.path.exists(keyfile):
                raise Exception(f"Keyfile not found: {keyfile}") # Set your keyfile path

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

    @task
    def walk_directory():
        path = []
        local_folder = "./downloads/"  # Set your folder path
        try:
            files = os.listdir(local_folder)  # List all files and directories in the folder
            print("Files in downloads folder:")
            for file in files:
                if os.path.isfile(os.path.join(local_folder, file)):  # Check if it's a file
                    print(file)
                    path.append(os.path.join(local_folder, file))
            return path
        except FileNotFoundError:
            print(f"Directory {local_folder} not found.")

    @task
    def load_dataframe_to_bq():
        keyfile = "/opt/airflow/gcs-credential.json"
        service_account_info = json.load(open(keyfile))
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        project_id = "pelagic-berm-448713-d4" # Set your project ID
        client = bigquery.Client(
            project=project_id,
            credentials=credentials,
        )

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("user_id", bigquery.SqlTypeNames.STRING),
                bigquery.SchemaField("first_name", bigquery.SqlTypeNames.STRING),
                bigquery.SchemaField("last_name", bigquery.SqlTypeNames.STRING),
                bigquery.SchemaField("email", bigquery.SqlTypeNames.STRING),
                bigquery.SchemaField("phone_number", bigquery.SqlTypeNames.STRING),
                bigquery.SchemaField("created_at", bigquery.SqlTypeNames.TIMESTAMP),
                bigquery.SchemaField("updated_at", bigquery.SqlTypeNames.TIMESTAMP),
                bigquery.SchemaField("address_id", bigquery.SqlTypeNames.STRING),
            ],
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="created_at",
            ),
            clustering_fields=["first_name", "last_name"],
        )

        file_path = "./downloads/users.csv"
        df = pd.read_csv(file_path, parse_dates=["created_at", "updated_at"])
        df.info()

        table_id = f"{project_id}.airflow.users"
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

        table = client.get_table(table_id)

    file_path = walk_directory()
    fetch_greenery_data() >> extract_zip() >> load_to_gcs(file_path) >> load_dataframe_to_bq()


greenery_to_bigquery()