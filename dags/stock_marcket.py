from airflow.decorators import dag, task
from datetime import datetime
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from include.stock_marcket.tasks import _get_stock_data, _store_prices

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
    
    
        
        
    is_api_available() >> get_stock_data >> store_prices
        
stock_marcket()