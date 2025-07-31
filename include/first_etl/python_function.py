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
            run_logging("SUCESS", "_get_the_data executed sucessfully, received data")
            print(">>>>>>>>>>>>>>>>>>>>>>>>>>", type(response.json()))
            return response.json()
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
    bucket_name = 'country-data'
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)   
    print(">>>>>>>>>>>>>>>>>>>>>",raw_data)
    print(type(raw_data))
    json_data = json.dumps(raw_data, ensure_ascii=False).encode('utf-8')
    byte_stream = BytesIO(json_data)
    ojb = client.put_object(
        bucket_name=bucket_name,
        object_name = f"country.json",
        data = byte_stream,
        length = len(json_data)
    )
    return "country.json" 
    
    
def _get_raw_data(obj_name):
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint = minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key = minio.login,
        secret_key=minio.password,
        secure=False,
    )
    bucket_name = 'country-data'
    response = client.get_object(
        bucket_name=bucket_name,
        object_name = f'{obj_name}'
    )
    json_data=None
    if response:
        
        raw_bytes = response.read() 
        raw_str = raw_bytes.decode('utf-8')
        json_data = json.loads(raw_str)
        print(type(json_data))       
    json_data = list(json_data) 
    json_data = json_data[0:100]
    return json_data
     
    
    
def _run_bash_data_processing(json_data):
    if not json_data:
        return False
    current_file = Path(__file__).resolve()
    bash_script = current_file.parent/'bash_function.sh'
    bash_path = str(bash_script).replace('\\', '/').replace('C:', '/c')
    bash_command = f"bash -c 'source {bash_path}; process_data {json_data}'"
    try:
        result = subprocess.run(bash_command, shell=True, check=True,capture_output=True, text=True )
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        return True
    except subprocess.CalledProcessError as e:
        print("something went wrong while logging the logs :", e)
        return False
    
    