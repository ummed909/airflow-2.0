from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from include.first_etl.python_function import _check_api, _get_the_data, _store_data, _get_raw_data, _run_bash_data_processing

REGION='asia'

default_args = {
    'owner': 'ummed_choudhary',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def called():
    print(1)
    return 1


with DAG(
    dag_id='first_etl',
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=['ETL', 'Ummed']
)as dag:
    
    with TaskGroup("Extract_Part") as Extract_Part:
        
    
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
            op_kwargs={'raw_data':'{{ti.xcom_pull(task_ids="Extract_Part.get_the_data")}}'}
        )
        
        check_status_of_api >> get_the_data >> store_raw_data
    
    with TaskGroup("Transform") as Transform:
        
        get_raw_data = PythonOperator(
            task_id = "get_raw_data",
            python_callable=_get_raw_data,
            op_kwargs= {'obj_name':'{{ti.xcom_pull(task_ids="Extract_Part.store_raw_data")}}'}
        )
        
        process_raw_data = PythonOperator(
            task_id = "process_raw_data",
            python_callable=_run_bash_data_processing,
            op_kwargs= {'json_data':'{{ti.xcom_pull(task_ids="Transform.get_raw_data")}}'}
        )
        
        get_raw_data >> process_raw_data
        
        
    
    
    end_task = PythonOperator(
        task_id = "end_task",
        python_callable=called
    )    
    
    Extract_Part >> Transform >> end_task