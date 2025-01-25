from datetime import datetime
from airflow.decorators import dag, task
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud import bigquery
import json

keyfile= "/opt/airflow/gcs-credential.json"
local_file_path="download/addresses.csv"
project_id="principal-lane-448907-m8"
dataset_id="greenary"
table_id="addresses"
bucket_name = "airflow-test-upload"
source_file_name = "download/addresses.csv"
destination_blob_name = "greenary-data/addresses.csv"
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)




@dag(schedule_interval=None, start_date=datetime(2024, 1, 24), catchup=False)
def greenary_etl():

    @task()
    def load_to_gcs():
        project_id = "Airflow"
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

    @task()
    def clean_greenary():
        pass

    @task()
    def load_form_gcs_to_bigquery():
        client = bigquery.Client(project=project_id, credentials=credentials)
        table_ref = client.dataset(dataset_id).table(table_id)
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.CSV,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        uri = f"gs://{bucket_name}/{destination_blob_name}"

        load_job = client.load_table_from_uri(
            uri,
            table_ref,
            job_config=job_config,
        )

        load_job.result()  # Waits for the job to complete.

        print(f"Loaded {load_job.output_rows} rows into {dataset_id}:{table_id}.")

    
    load_to_gcs() >> clean_greenary() >> load_form_gcs_to_bigquery()

greenary_etl()