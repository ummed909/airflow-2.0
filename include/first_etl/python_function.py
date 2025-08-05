from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from minio import Minio
import json
from io import BytesIO
import subprocess
from pathlib import Path
import os

def run_logging(head, messages):
    if head is None or messages is None:
        return False
   
    current_file = Path(__file__).resolve()
    bash_script = current_file.parent/'bash_function.sh'
    bash_path = str(bash_script).replace('\\', '/').replace('C:', '/c')
    bash_command = f"bash -c 'source {bash_path}; add_log {head} {messages}'"
    try:
        result = subprocess.run(bash_command, shell=True, check=True,capture_output=True, text=True )
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
            print(">>>>>>>>>>>>>>>>>>>>>>>>>>", type(json.dumps(response.json())))
            return json.dumps(response.json())
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
    load_data = json.loads(raw_data)
    data = json.dumps(load_data, ensure_ascii=False).encode('utf8')
    print(type(raw_data))
    ojb = client.put_object(
        bucket_name=bucket_name,
        object_name = f"country.json",
        data = BytesIO(data),
        length = len(data)
    )
    return "country.json" 
    
    
def _get_raw_data_and_save_in_temp_file(obj_name):
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
        
    raw_bytes = response.read()
    data = raw_bytes.decode('utf-8')
    print("type", type(data))
    temp_base_path = '/shared_volume/'
    file_name = 'temp_raw_data.json'
    print("writing files ..........")
    with open(f"{temp_base_path}{file_name}", 'w') as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
        print("completed file .........")
         
    full_file_path = f"{temp_base_path}{file_name}"
    print("path", full_file_path)
    return full_file_path
     
    
    
def _run_bash_function_to_process_raw_data(file_path):
    current_file = Path(__file__).resolve()
    bash_script = current_file.parent/'bash_function.sh'
    bash_path = str(bash_script).replace('\\', '/').replace('C:', '/c')
    bash_command = f"bash -c 'source {bash_path}; process_data {file_path}'"
    try:
        result = subprocess.run(bash_command, shell=True, check=True,capture_output=True, text=True )
        if os.path.exists(result.stdout.strip()):
            return result.stdout.strip()
        else:
            return None
    except subprocess.CalledProcessError as e:
        print("something went wrong while logging the logs :", e)
        return False

def load_data_to_postgres(file_path):
    import pandas as pd
    from sqlalchemy import create_engine
    engine = create_engine('postgresql://postgres:postgres@postgres:5432/airflow')
    
    df = pd.read_csv(file_path)
    
    df.to_sql("countries",engine, if_exists='replace', index=False)
    