from airflow.decorators import dag, task
from datetime import datetime
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from include.stock_marcket.tasks import _get_stock_data, _store_prices, _load_data_from_minio

DATA_PROCESS_SCRIPT = "/airflow-2.0/include/stock_marcket"

SYMBOL = 'NVDA'

@dag(
    start_date=datetime(2023,1,1),
    schedule='@daily',
    catchup=False,
    tags=['stock_marcket']
)
def stock_marcket():
    
    # Taks 1 : check the API
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available():
        import requests
        
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        print("_____________________ url", url)
        response = requests.get(url, headers=api.extra_dejson["headers"])
        print("___________________________response", response, ">>>", response.json())
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)
    
    # Task 2 : get the api data
    get_stock_data = PythonOperator(
        task_id = 'get_stock_data',
        python_callable = _get_stock_data,
        op_kwargs={'url':'{{ti.xcom_pull(task_ids="is_api_available")}}', 'symbol': SYMBOL}
    )
    
    # Task 3 : load the data in the minio
    store_prices = PythonOperator(
        task_id = 'store_prices',
        python_callable=_store_prices,
        op_kwargs={'stock':'{{ti.xcom_pull(task_ids="get_stock_data")}}'}
    )
    
    # Tash 4 : load the unporcess data from the minio
    load_unprocess_data = PythonOperator(
        task_id = 'load_unprocess_data',
        python_callable=_load_data_from_minio,
        op_kwargs = {'obj_name':'{{ti.xcom_pull(task_ids="store_prices")}}'}
    )
    
    # Task 5 : process the loaded json file from the minio
    process_data = BashOperator(
        task_id = 'process_data',
        bash_command="source /usr/local/airflow/include/stock_marcket/bash_task.sh && _process_data '{{ti.xcom_pull(task_ids='load_unprocess_data')}}'",
    )
    
    
    
    
    
    
        
        
    is_api_available() >> get_stock_data >> store_prices >> load_unprocess_data >> process_data
        
stock_marcket()