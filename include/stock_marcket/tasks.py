from airflow.hooks.base import BaseHook
from minio import Minio
import json
from io import BytesIO


def _get_stock_data(url, symbol):
    import requests
    url = f"{url}{symbol}?metrics=high?interval=1d&range=1d"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url=url, headers=api.extra_dejson["headers"])
    print("url>>>>>>>", url)
    print("responde >>>>>>>", response.status_code, response.json())
    return json.dumps(response.json()['chart']['result'][0])

def _store_prices(stock):
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key = minio.login,
        secret_key=minio.password,
        secure=False,
    )
    bucket_name = 'stock-marcket'
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    stocks = json.loads(stock)
    symbol = stocks['meta']['symbol']
    data = json.dumps(stocks, ensure_ascii=False).encode('utf-8')
    obj = client.put_object(
        bucket_name=bucket_name,
        object_name = f'{symbol}/prices.json',
        data = BytesIO(data),
        length=len(data)
    )
    return f'{obj.bucket_name}/{symbol}'
    
    
