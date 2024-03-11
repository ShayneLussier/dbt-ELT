from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from scripts.transactions import fetch_product_data, generate_fake_transaction
from scripts.database import get_max_customer_id

# -------------------- FUNCTIONS -------------------- #

def generate_transactions(**kwargs):
    amount = 100
    ti = kwargs['ti']  # Access the XCom Task Instance
    max_customer_id=ti.xcom_pull(task_ids='fetch_max_customer_id', key='return_value')
    generate_fake_transaction(amount, max_customer_id)

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
    'retries': 1,
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

    fetch_s3_product_data = PythonOperator(
        task_id='fetch_s3_product_data',
        python_callable=fetch_product_data,
        do_xcom_push=True,
        dag=dag,
    )

    fetch_max_customer_id = SnowflakeOperator(
        task_id='fetch_max_customer_id',
        sql="SELECT MAX(customer_id) AS max_customer_id FROM analytics.customers_dim",
        snowflake_conn_id='snowflake_conn',
        do_xcom_push=True,
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
    [fetch_s3_product_data, fetch_max_customer_id] >> generate_transactions

'''
retrieve products csv and from s3 bucket
fetch max customer_id from snowflake table, if none start at 0
create fake transaction json data
load json data into snowflake
transform via dbt
changes should be visible in a tableau dashboard
'''

'''
retrieve products csv and from s3 bucket
fetch max customer_id from snowflake table, if none start at 0
create fake transaction json data
load json file into s3 bucket
load json file from s3 to snowflake using streams??? or bucket auto detect???
run a transform script via snowflake, sql or python? *** is this script triggered by the bucket auto detect??
tableau dashboard
'''

'''
retrieve products csv and from s3 bucket
fetch max customer_id from snowflake table, if none start at 0
create fake transaction json data
send the json file to spark for transformation
load into snowflake
tableau
'''

'''
aws pipeline
'''