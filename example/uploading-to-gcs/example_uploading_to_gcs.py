import json
import os
import sys

from google.api_core import exceptions
from google.cloud import storage
from google.oauth2 import service_account


def upload_blob():
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    bucket_name = "airflow-test-upload"
    source_file_name = "download/addresses.csv"
    destination_blob_name = "greenary-data/addresses.csv"

    keyfile = "/workspaces/craft_data_pipeline_with_apache_airflow/principal-lane-448907-m8-f93d527e9bd1.json"
    service_account_info = json.load(open(keyfile))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    project_id = "Airflow"

    storage_client = storage.Client(
        project=project_id,
        credentials=credentials,
    )
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(destination_blob_name)

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to upload is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.
    # generation_match_precondition = 0
    # blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )


upload_blob()