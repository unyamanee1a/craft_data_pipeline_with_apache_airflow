from google.cloud import bigquery
import json
from google.oauth2 import service_account


keyfile= "/workspaces/craft_data_pipeline_with_apache_airflow/principal-lane-448907-m8-f93d527e9bd1.json"
local_file_path="download/addresses.csv"
project_id="principal-lane-448907-m8"
dataset_id="greenary"
table_id="addresses"

def load_to_bigquery():
    service_account_info = json.load(open(keyfile))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    client = bigquery.Client(
        project=project_id,
        credentials=credentials,)

    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    with open(local_file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Waits for the job to complete.

    print(f"Loaded {job.output_rows} rows into {dataset_id}:{table_id}.")

# if __name__ == "__main__":
#     local_file_path = '/path/to/local/file.csv'
#     project_id = 'your_project'
#     dataset_id = 'your_dataset'
#     table_id = 'your_table'

load_to_bigquery()