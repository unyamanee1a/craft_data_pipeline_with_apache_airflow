from airflow import DAG
from airflow.decorators import task
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='example-trigger',
    schedule_interval='@daily',  # Run daily
    start_date=datetime(2024, 1, 1),  # Start date
    catchup=False,  # Avoid running past executions
    tags=["example"],
) as dag:

    # Task to print "Hello"
    @task
    def print_hello():
        print("Hello")

    # Task to print "World"
    @task
    def print_world():
        print("World")

    # Define the task execution order
    print_hello() >> print_world()