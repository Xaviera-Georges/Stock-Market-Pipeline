import csv
from datetime import datetime, timezone
from io import BytesIO
import json
from minio import Minio
from airflow.hooks.base import BaseHook
import requests
from airflow.exceptions import AirflowNotFoundException
BUCKET_NAME='stock-market'

def _get_minio_client():
    minio=BaseHook.get_connection('minio')
    client=Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )

    return client




def _get_stock_prices(url):
    url=f"{url}?metrics=high?&interval=1d&range=1y"
    api=BaseHook.get_connection('stock_api')
    response=requests.get(url, headers=api.extra_dejson['headers'])
    
    return json.dumps(response.json()['chart']['result'][0])

def _store_prices(stock):
    client=_get_minio_client()
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    stock=json.loads(stock)
    symbol=stock['meta']['symbol']
    data=json.dumps(stock,ensure_ascii=False).encode('utf8')
    objw=client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f'{symbol}/prices.json',
        #we use BytesIo because we are reading the data in
        data=BytesIO(data),
        length=len(data)
    )

    return f'{objw.bucket_name}/{symbol}'

def _get_formatted_csv(path):
    client=_get_minio_client()
    prefix_name=f"{path.split('/')[1]}/formatted_prices/"
    objects=client.list_objects(BUCKET_NAME, prefix=prefix_name,recursive=True)
    for obj in objects:
        if obj.object_name.endswith('.csv'):
            return obj.object_name
        
    raise AirflowNotFoundException('The csv file does not exist')


def parse_csv(filepath):
    import csv
    with open(filepath, newline="") as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            # Assuming the timestamp is in the first column
            timestamp = int(row[0])
            # Convert Unix timestamp to ISO 8601 format
            timestamp_iso = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
            # Replace the original timestamp with the converted timestamp
            row[0] = timestamp_iso
            yield row

