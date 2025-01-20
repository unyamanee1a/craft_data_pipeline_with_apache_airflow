from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json

# Step 1: must set open_meteo_api connection in Airflow UI
# Step 2: must set Latitude and longitude for the desired location (London in this case)

LATITUDE = '51.5074'
LONGITUDE = '-0.1278'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'start_date': days_ago(1)
}

# DAG
with DAG(dag_id='example-extract-data',
    default_args = default_args,
    schedule_interval = None,
    catchup = False,
) as dags:

    @task()
    def extract_weather_data():
        """Extract weather data from Open-Meteo API using Airflow Connection."""

        # Use HTTP Hook to get connection details from Airflow connection
        http_hook = HttpHook(http_conn_id=API_CONN_ID,method='GET')

        # Build the API endpoint
        # https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        # Make the request via the HTTP Hook
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            data = response.json()
            print(json.dumps(data, indent=4))
            return data
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")
        

    extract_weather_data()