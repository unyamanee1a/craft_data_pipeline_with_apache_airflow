from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

SQL_CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS example_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    age INT
);
"""

# Define the SQL query to insert data
SQL_INSERT_QUERY = """
INSERT INTO example_table (name, age)
VALUES ('Alice', 28),
       ('Bob', 35);
"""

# Define the DAG
with DAG(
    "insert_data_into_postgres",  # Unique DAG ID
    description="Insert data into PostgreSQL from Airflow",
    schedule_interval=None,  # No automatic scheduling
    start_date=datetime(2023, 1, 1),  # Start date for the DAG
    catchup=False,  # Don't backfill
    tags=["example", "postgres", "sql"],
) as dag:

    # Task to create the table if it doesn't exist
    create_table_task = SQLExecuteQueryOperator(
        task_id="create_table",  # Task ID
        sql=SQL_CREATE_TABLE_QUERY,  # The SQL query to create the table
        conn_id="my_postgres_conn",  # Connection ID (set this in Airflow Connections)
        autocommit=True,  # Automatically commit the changes
    )

    # Task to insert data into PostgreSQL
    insert_data_task = SQLExecuteQueryOperator(
        task_id="insert_data",  # Task ID
        sql=SQL_INSERT_QUERY,  # The SQL query to execute
        conn_id="my_postgres_conn",  # Connection ID (set this in Airflow Connections)
        autocommit=True,  # Automatically commit the changes
    )

    # Set the task dependencies
    create_table_task >> insert_data_task