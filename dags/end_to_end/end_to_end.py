# from airflow import models
# from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
# from airflow.providers.http.operators.http import SimpleHttpOperator
# from airflow.decorators import dag, task
# from airflow.utils.dates import days_ago
# from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime, timedelta
import os
from airflow.decorators import dag, task
import pandas as pd
import zipfile
import json
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

    # Task 1: Fetch Data from API (Greenery Data)
    @task()
    def fetch_greenery_data():
        # Example URL from OpenWeather API (replace with a valid one)
        url = "https://www.kaggle.com/api/v1/datasets/download/saurabhshahane/green-strategy-dataset"

        # Make the API request
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
    def clean_data():
        # Load the CSV file
        df = pd.read_csv("./downloads/Green Strategy Dataset.csv")
        output_path = "./downloads/cleaned_data.csv"
        # Step 1: Clean missing values
        df['DOI'].fillna('Unknown', inplace=True)
        df['Affiliations'].fillna('No affiliations listed', inplace=True)
        df['Abstract'].fillna('No abstract available', inplace=True)

        # Drop rows where important information is missing (e.g., 'Authors', 'Title')
        df.dropna(subset=['Authors', 'Title'], how='any', inplace=True)

        # Fill missing numerical values (like 'Page count', 'Cited by') with median or 0
        df['Page count'].fillna(df['Page count'].median(), inplace=True)
        df['Cited by'].fillna(0, inplace=True)

        # Step 2: Handle duplicate rows
        df.drop_duplicates(inplace=True)

        # Step 3: Standardize column formats
        df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
        df['DOI'] = df['DOI'].str.lower()
        df['Title'] = df['Title'].str.strip().str.title()
        df['Authors'] = df['Authors'].str.strip().str.title()

        # Step 4: Convert date columns to datetime format (if applicable)
        df['Conference date'] = pd.to_datetime(df['Conference date'], errors='coerce')

        # Step 5: Clean up numeric columns
        df['ISSN'] = pd.to_numeric(df['ISSN'], errors='coerce')
        df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce')
        df['Issue'] = pd.to_numeric(df['Issue'], errors='coerce')

        # Handle outliers or invalid data (e.g., negative page numbers)
        df['Cited by'] = df['Cited by'].apply(lambda x: max(0, x))  # Set negative 'Cited by' values to 0
        # df = df[df['Page start'] > 0]  # Remove invalid page starts
        df = df[df['Page end'] > df['Page start']]  # 'Page end' should be greater than 'Page start'

        # Step 6: Save the cleaned DataFrame to a new CSV file
        df.to_csv(output_path, index=False)
        print(f"Cleaned data saved to {output_path}")
        print(df.dtypes)

    fetch_greenery_data() >> extract_zip() >> clean_data()


greenery_to_bigquery()
