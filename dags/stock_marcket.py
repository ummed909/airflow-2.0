from airflow.decorators import dag, task
from airflow.operators.python import PythonbashOperator
from datetime import datetime
from airflow.sensors.base import PokeReturnValue


@dag(
    start_date=datetime(2023,1,1),
    schedule='@daily',
    catchup=False,
    tags=['stock_marcket']
)
def stock_marcket():
    
    @task.sensor(poke_inteval=30, timeout=300, mode='poke')
    def is_api_available() ->PokeReturnValue:


stock_marcket()