from pprint import pprint
from airflow.decorators import task, dag
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

@dag(
    dag_id='ieie',
    default_args=default_args,
    description='An example DAG',
    schedule_interval='@daily',
    catchup=False,
)
def example_dag():

    @task
    def print_context(**context):
        print(context['ds'])

    start = DummyOperator(
        task_id='start',
    )

    end = DummyOperator(
        task_id='end',
    )

    start >> print_context() >> end

example_dag()