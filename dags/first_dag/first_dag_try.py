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
    'first_dag_try',
    default_args=default_args,
    description='test task',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

def hello_world():
    print("hello world")

start = EmptyOperator(
    task_id='start',
    dag=dag,
)

intermediate_task = EmptyOperator(
    task_id='intermediate',
    dag=dag,
)

second_intermediate_task = EmptyOperator(
    task_id='second_intermediate',
    dag=dag,
)

send_email = EmptyOperator(
    task_id='send-email',
    dag=dag,
)

send_line_message = EmptyOperator(
    task_id='send-line',
    dag=dag,
)
send_ms_team = EmptyOperator(
    task_id='send-ms-team',
    dag=dag,
)


end = EmptyOperator(
    task_id='end',
    dag=dag,
)

start >> [intermediate_task,second_intermediate_task] >> end >> [send_line_message, send_ms_team]
second_intermediate_task >> send_email