from datetime import datetime, timedelta
import pendulum
import os

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import boto3

from scripts.data import main

# -------------------- FUNCTIONS -------------------- #

def fetch_file_from_s3(key='products/products.csv', bucket_name='data-pipeline-repo', **kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_airflow')

    csv_content = s3_hook.get_key(key, bucket_name).get()['Body'].read().decode('utf-8')

    # Push the csv_content to XCom
    kwargs['ti'].xcom_push(key='csv_content', value=csv_content)

def generate_transactions(**kwargs):
    amount = 100
    ti = kwargs['ti']
    csv_content = ti.xcom_pull(task_ids='fetch_s3_data', key='csv_content')
    main(csv_content, amount)

# def upload_to_s3(key='transactions/transactions.json', bucket_name='data-pipeline-repo', **kwargs):
#     # Upload processed data to S3 bucket
#     s3_hook = S3Hook(aws_conn_id='aws_airflow')
#     s3_hook.load_file(filename='transactions.json', key=key, bucket_name=bucket_name, replace=True)  # Set to replace=True if you want to overwrite existing file

# -------------------- DAG -------------------- #

default_args = {
    'owner': 'airflow',
    'email': ['shayne@shaynelussier.com'],
    'email_on_failure': True,
    "depends_on_past": False,
    'retries': 2,
    "retry_delay": timedelta(seconds=30)
}


with DAG(
    dag_id='DAG',
    start_date=datetime(2023, 12, 1),
    schedule_interval= '0 20 * * *', # 8 pm EST,
    catchup=False,
    default_args=default_args,
    description='Perform ELT on transaction JSON data into Snowflake for downstream analytics',
) as dag:

    fetch_s3_data = PythonOperator(
        task_id='fetch_s3_data',
        python_callable=fetch_file_from_s3,
        provide_context=True,
        dag=dag,
    )

    generate_transactions = PythonOperator(
        task_id='generate_transactions',
        python_callable=generate_transactions,
        provide_context=True,
        trigger_rule='all_success',
        dag=dag,
    )

    # upload_to_s3 = PythonOperator(
    #     task_id='upload_to_s3',
    #     python_callable=upload_to_s3,
    #     provide_context=True,
    #     dag=dag,
    # )

    # Define the task dependency
    fetch_s3_data >> generate_transactions
    # fetch_s3_data >> generate_transactions >> upload_to_s3

'''
retrieve products csv and from s3 bucket
fetch max customer_id from snowflake table, if none start at 0
create fake transaction json data
load json file into s3 bucket *****replace is task below*****
load json file from s3 to snowflake using streams??? or bucket auto detect???
transform via dbt
changes should be visible in a tableau dashboard
'''