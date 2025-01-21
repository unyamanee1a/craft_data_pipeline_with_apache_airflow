from google.cloud import bigquery
import os

def load_to_bigquery(local_file_path, project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)

    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    with open(local_file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Waits for the job to complete.

    print(f"Loaded {job.output_rows} rows into {dataset_id}:{table_id}.")

if __name__ == "__main__":
    local_file_path = '/path/to/local/file.csv'
    project_id = 'your_project'
    dataset_id = 'your_dataset'
    table_id = 'your_table'

    load_to_bigquery(local_file_path, project_id, dataset_id, table_id)