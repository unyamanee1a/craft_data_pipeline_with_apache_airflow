from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.decorators import task, dag
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['your-email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    catchup=False,
    dag_id='example_templating',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    tags=['example', 'templating'],
)
def example_templating():

    @task.bash
    def bash_task():
        """Example Bash task with templated command."""
        return 'echo "Execution date is {{ ds }} and next execution is {{ next_ds }}"'

    @task
    def print_context_vars(**context):
        """Print various templated variables."""
        print(f"Execution date: {context['ds']}")
        print(f"Logical date: {context['logical_date']}")
        print(f"DAG ID: {context['dag'].dag_id}")
        print(f"Task ID: {context['task'].task_id}")
        print(f"Previous execution date: {context['prev_ds']}")

    @task.bash
    def templated_bash_task():
        """Example Bash task with templated command."""
        return """
        {% for i in range(5) %}
            echo "{{ ds }}"
            echo "{{ macros.ds_add(ds, 7) }}"
            echo "Processing file_{{ ds_nodash }}.csv"
        {% endfor %}
        """

    bash_task() >> print_context_vars() >> templated_bash_task()


example_templating()
