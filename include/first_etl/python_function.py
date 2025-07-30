from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from minio import Minio
import json
from io import BytesIO
import subprocess
from pathlib import Path

def run_logging(head, messages):
    if head is None or messages is None:
        return False
   
    current_file = Path(__file__).resolve()
    bash_script = current_file.parent/'bash_function.sh'
    bash_path = str(bash_script).replace('\\', '/').replace('C:', '/c')
    bash_command = f"bash -c 'source {bash_path}; add_log {head} {messages}'"
    try:
        result  = result = subprocess.run(bash_command, shell=True, check=True,capture_output=True, text=True )
        return True
    except subprocess.CalledProcessError as e:
        print("something went wrong while logging the logs :", e)
        return False


def _check_api(region):
    run_logging('INFO', 'starting _check_api function')
    import requests
    api  = BaseHook.get_connection('country_data')
    url = f"{api.host}{region}"
    try:
        response = requests.get(url=url)
        if response.status_code == 200:
            run_logging("SUCESS", "API is working and active with status_code:{{response.status_code}}")
            return True
    except:
        run_logging("ERROR", "Due to somereason API is not working")
        return False
    
def _get_the_data(region):
    run_logging("INFO","_get_the_data function is simulating" )
    import requests
    api = BaseHook.get_connection('country_data')
    url = f"{api.host}{region}"
    try:
        response = requests.get(url=url)
        if response.status_code == 200:
            data = response.json()
            print("data", data)
            run_logging("SUCESS", "_get_the_data executed sucessfully, received data")
            return data
    except:
        run_logging("ERROR", "get some error while geting data in _get_the_data ")
        return False


def _store_data(raw_data):
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint = minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key = minio.login,
        secret_key=minio.password,
        secure=False,
    )
    bucket_name = 'country_data',
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    data = json.dumps(raw_data, ensure_ascii=False).encode('utf-8')
    ojb = client.put_object(
        bucket_name=bucket_name,
        object_name = f"country.json",
        data = BytesIO(data),
        length = len(data)
    )
    return "country.json"
    