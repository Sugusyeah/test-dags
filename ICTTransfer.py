from airflow import DAG
from airflow.operators.python import task
# from airflow.operators.python import PythonOperator, get_current_context, task
# from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
# from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
# from sqlalchemy import create_engine
# import pandas as pd
# import sqlalchemy as sa
# import json
import requests

default_args = {
    'owner': 'DGSI',
}
with DAG(
    'ict_transfer',
    default_args=default_args,
    description='ETL ICT Transfer',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['1.1.0']
) as dag:
    url = 'http://10.41.81.70/api/v1/'

    @task(task_id="ictTransfer")
    def ictTransferfunc():
        finalurl = url + 'ICTTransfer'
        res = requests.post(finalurl)
        return res.text
    
    ictTask = ictTransferfunc()

    ictTask