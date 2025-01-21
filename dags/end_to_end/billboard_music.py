from airflow.decorators import dag, task
from datetime import datetime, timedelta
import os
import pandas as pd
import requests


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(1977, 9, 1),
    'end_date': datetime(1977, 9, 30),  # Add end date here
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id="billboard_music",
    default_args=default_args,
    description="Collect daily Billboard Hot 100 music data",
    schedule_interval="@daily",
    catchup=True,
    tags=["billboard", "music"],
)
def billboard_music():
    @task(task_id="fetch_data_from_github")
    def download_and_save_csv(**context):
        date = context['ds']
        url = f'https://raw.githubusercontent.com/mhollingshead/billboard-hot-100/main/date/{date}.json'

        # Download JSON data
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch data for date {date}, status code: {response.status_code}")

        # Load JSON data
        data = response.json()
        print("Data received:", data)
        tracks = data.get("data", [])

        if not tracks:
            raise Exception(f"No tracks data found for date {date}")

        df = pd.DataFrame(tracks)
        output_dir = './billboard'
        print(f"Creating directory: {output_dir}")
        os.makedirs(output_dir, exist_ok=True)
        # Save DataFrame to CSV
        output_file = f"{output_dir}/billboard_hot_100_{date}.csv"
        df.to_csv(output_file, index=False)

        print(f"Saved CSV for {date} to {output_file}")

    download_and_save_csv()


billboard_music()
