from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'try',
    default_args=default_args,
    description='test task',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)


start = EmptyOperator(
    task_id='start',
    dag=dag,
)

branching = EmptyOperator(
    task_id='branching',
    dag=dag,
)

success = EmptyOperator(
    task_id='success',
    dag=dag,
)

failure = EmptyOperator(
    task_id='failure',
    dag=dag,
)

finish = EmptyOperator(
    task_id='finish',
    dag=dag,
)
send_error = EmptyOperator(
    task_id='send_error',
    dag=dag,
)

start >> branching >>[success,failure] >> finish
failure >> send_error