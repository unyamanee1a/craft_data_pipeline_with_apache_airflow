from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import pandas as pd
import os


# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'end_date': datetime(2025, 1, 15),  # Add end date here
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    'simple_weather_collector',
    default_args=default_args,
    description='Collect daily weather data',
    schedule_interval='@daily',
    catchup=True,  # Enable backfilling
)
def simple_weather_collector():
    @task(task_id="fetch_and_save_weather")
    def fetch_and_save_weather(**context):
        execution_date = context['ds']

        # Add debug prints
        print(f"Starting execution for date: {execution_date}")

        output_dir = './weather_data'
        print(f"Creating directory: {output_dir}")
        os.makedirs(output_dir, exist_ok=True)

        url = (
            "https://api.open-meteo.com/v1/forecast?"
            "latitude=51.5074&longitude=-0.1278"
            f"&start_date={execution_date}"
            f"&end_date={execution_date}"
            "&daily=temperature_2m_max,temperature_2m_min,precipitation_sum"
        )

        try:
            print(f"Fetching data from URL: {url}")
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            # Check if data is None or if there is no daily data
            if not data or 'daily' not in data:
                print(f"ERROR: No daily weather data found for date {execution_date}")
                return  # Skip the saving step if no data is available

            print("Data received:", data)  # See what data we're getting

            daily_data = data['daily']

            max_temp = daily_data['temperature_2m_max'][0]
            if max_temp is None or max_temp == '':
                print(f"ERROR: Max temperature is missing or empty for date {execution_date}")
                return  # Skip saving if max_temp is missing or empty
            # Process the data into a DataFrame

            weather_df = pd.DataFrame({
                'date': execution_date,
                'max_temp': daily_data['temperature_2m_max'][0],
                'min_temp': daily_data['temperature_2m_min'][0],
                'precipitation': daily_data['precipitation_sum'][0]
            }, index=[0])

            output_file = f"{output_dir}/weather_{context['ds_nodash']}.csv"
            print(f"Attempting to save to: {output_file}")
            weather_df.to_csv(output_file, index=False)
            print(f"Successfully saved to {output_file}")

        except Exception as e:
            print(f"ERROR: {str(e)}")
            print(f"Error type: {type(e)}")
            raise

    fetch_and_save_weather()


simple_weather_collector()
