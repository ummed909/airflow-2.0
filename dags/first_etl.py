from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from include.first_etl.python_function import _check_api, _get_the_data, _store_data

REGION='asia'

default_args = {
    'owner': 'ummed_choudhary',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='first_etl',
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=['ETL', 'Ummed']
)as dag:
    
    # task 1: check the status of API
    check_status_of_api = PythonOperator(
        task_id = 'check_status_of_api',
        python_callable=_check_api,
        op_kwargs={'region':REGION},
    )
    
    # task 2: get the data from the API
    get_the_data = PythonOperator(
        task_id = 'get_the_data',
        python_callable= _get_the_data,
        op_kwargs={'region':REGION},
    )
    
    # task 3: store the raw data in minio
    store_raw_data = PythonOperator(
        task_id = 'store_raw_data',
        python_callable = _store_data,
    )
    
    

    
    check_status_of_api >> get_the_data >> store_raw_data