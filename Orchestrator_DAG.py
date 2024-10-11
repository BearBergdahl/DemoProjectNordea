from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
from DAGS import oracledb_connect, snowflake_connect

# This can't be run in normal debugger, needs a Airflow instance on a server.
# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'multi_source_to_snowflake_etl',
    default_args=default_args,
    description='A DAG to extract data from Oracle, CSV, and load into Snowflake',
    schedule_interval=timedelta(hours=1),
)

# Oracle connection parameters possibly to another docker instance oracle/
oracle_conn_params = {
    "user": "your_oracle_username",
    "password": "your_oracle_password",
    "dsn": "your_oracle_host:your_oracle_port/your_oracle_service_name"
}

# Snowflake connection parameters
snowflake_conn_params = {
    "account": "your_snowflake_account_identifier",
    "user": "your_snowflake_username",
    "password": "your_snowflake_password",
    "warehouse": "your_snowflake_warehouse",
    "database": "your_snowflake_database",
    "schema": "your_snowflake_schema"
}

# CSV file path
csv_file_path = "./csvfile.csv"


def extract_from_oracle():
    data, columns = oracledb_connect.execute_query("SELECT * FROM your_oracle_table")
    df = pd.DataFrame(data, columns=columns)
    return df


def extract_from_csv():
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"CSV file not found: {csv_file_path}")

    df = pd.read_csv(csv_file_path)
    return df



def load_to_snowflake(df, table_name):
    snowflake_connect.insert_data(df, table_name)



def oracle_to_snowflake():
    df = extract_from_oracle()
    load_to_snowflake(df, "oracle_data_table")


def csv_to_snowflake():
    df = extract_from_csv()
    load_to_snowflake(df, "csv_data_table")


# Define the tasks
oracle_task = PythonOperator(
    task_id='oracle_to_snowflake',
    python_callable=oracle_to_snowflake,
    dag=dag,
)

csv_task = PythonOperator(
    task_id='csv_to_snowflake',
    python_callable=csv_to_snowflake,
    dag=dag,
)

# Set up task dependencies
oracle_task >> csv_task