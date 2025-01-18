from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Default arguments for the DAG
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

dag = DAG(
    'templating_example',
    default_args=default_args,
    description='DAG showing various templating examples',
    tags=['example', 'templating'],
    schedule_interval='@daily',
)

# Example 1: Using templates in BashOperator
bash_task = BashOperator(
    task_id='bash_template_example',
    bash_command='echo "Execution date is {{ ds }} and next execution is {{ next_ds }}"',
    dag=dag,
)


# Example 2: Using templates in Python function
def print_context_vars(**context):
    """Print various templated variables."""
    print(f"Execution date: {context['ds']}")
    print(f"Logical date: {context['logical_date']}")
    print(f"DAG ID: {context['dag'].dag_id}")
    print(f"Task ID: {context['task'].task_id}")
    print(f"Previous execution date: {context['prev_ds']}")


python_task = PythonOperator(
    task_id='python_template_example',
    python_callable=print_context_vars,
    dag=dag,
)

# Example 3: Using templates in task configuration
templated_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7) }}"
    echo "Processing file_{{ ds_nodash }}.csv"
{% endfor %}
"""

templated_bash_task = BashOperator(
    task_id='templated_bash_task',
    bash_command=templated_command,
    dag=dag,
)

# Set task dependencies
bash_task >> python_task >> templated_bash_task
