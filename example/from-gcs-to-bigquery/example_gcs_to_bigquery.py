from google.cloud import bigquery

def load_data_from_gcs_to_bigquery(bucket_name, source_file_name, project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)

    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    uri = f"gs://{bucket_name}/{source_file_name}"

    load_job = client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config,
    )

    load_job.result()  # Waits for the job to complete.

    print(f"Loaded {load_job.output_rows} rows into {dataset_id}:{table_id}.")

if __name__ == "__main__":
    bucket_name = 'your-bucket-name'
    source_file_name = 'path/to/your/file.csv'
    project_id = 'your-project-id'
    dataset_id = 'your_dataset_id'
    table_id = 'your_table_id'

    load_data_from_gcs_to_bigquery(bucket_name, source_file_name, project_id, dataset_id, table_id)