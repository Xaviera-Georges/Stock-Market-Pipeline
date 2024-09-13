from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
import requests
from airflow.providers.slack.notifications.slack_notifier import SlackNotifier
from airflow.providers.amazon.aws.transfers.s3_to_sql import  S3ToSqlOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from include.stockmarket.tasks import _get_stock_prices, _store_prices,_get_formatted_csv,BUCKET_NAME,parse_csv
from include.stockmarket.stocktransform import app


default_args={
    'owner':'xaviera',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

@dag(
    dag_id='stock_market',
    default_args=default_args,
    start_date=datetime(2024,9,9),
    schedule='@daily',
    tags=['stock_market'], 
    on_success_callback=SlackNotifier(
        slack_conn_id='slack',
        text='The DAG stock_market has succeeded',
        channel='general'

    ),
    on_failure_callback=SlackNotifier(
        slack_conn_id='slack',
        text='The DAG stock_market has failed',
        channel='general'

    ),
    catchup=False
)

def stock_market():
    

    @task.sensor(poke_interval=30,timeout=300,mode='poke')
    def  is_api_available() -> PokeReturnValue:
        api=BaseHook.get_connection('stock_api')
        SYMBOL = "AAPL"
        url = f"{api.host}{api.extra_dejson['endpoint']}{SYMBOL}"
        response=requests.get(url,headers=api.extra_dejson['headers'])
        condition=response.json()['chart']['error'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url) 
    
    get_stock_prices=PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,
        op_kwargs={'url': '{{ task_instance.xcom_pull(task_ids="is_api_available") }}'}

    )

    store_prices=PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={'stock': '{{ task_instance.xcom_pull(task_ids="get_stock_prices") }}'}
    )

    format_prices=PythonOperator(
        task_id='format_prices',
        python_callable=app,
        op_kwargs={'SPARK_APPLICATION_ARGS':'{{ task_instance.xcom_pull(task_ids="store_prices") }}'}
    
    )

    get_formatted_csv=PythonOperator(
        task_id='get_formatted_csv',
        python_callable=_get_formatted_csv,
        op_kwargs={
            'path':'{{ task_instance.xcom_pull(task_ids="store_prices") }}'
    
        }
    )

    


    load_data_to_postgres = S3ToSqlOperator(
        task_id='load_data_to_postgres_task',
        s3_bucket=BUCKET_NAME,                # Your S3 bucket name
        s3_key='{{ task_instance.xcom_pull(task_ids="get_formatted_csv") }}'    ,            # Path to the file in S3
        schema='public',                           # PostgreSQL schema
        table='your_table',                   # PostgreSQL table to load data into
        sql_conn_id='postgres_localhost',       # Airflow connection ID for PostgreSQL
        aws_conn_id='minio',  
        column_list=['timestamp','close','high','low','open','volume','date'],             # Airflow connection ID for AWS
        parser=parse_csv    

    )
    
    is_api_available() >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv >> load_data_to_postgres
    
stock_market()
