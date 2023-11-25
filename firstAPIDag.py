from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine
import pandas as pd
import sqlalchemy as sa
import json
import requests

default_args = {
    'owner': 'airflow',
}

# dag = DAG(dag_id='api_dag', default_args=default_args, schedule_interval=None, start_date=days_ago(2),)

# with dag:
#     callAPI_task = SimpleHttpOperator(
#         task_id="call_api",
#         http_conn_id="test_flask_api",
#         method="GET",
#         endpoint="/",
#         # data={"param1": "value1", "param2": "value2"},
#         headers={"Content-Type": "application/json"},
#         # dag="test_callAPI",
#     )
#     callAPI_task

with DAG(
    'test_etl_callAPI',
    default_args=default_args,
    description='ETL API',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['test']
) as dag:
    url = 'http://10.41.81.70/api/v1/'
    # url = 'http://10.41.81.70/'
    instance_id='CHANGEME'
    @task(task_id="testConnection26")
    def mappingfunc():
        # Pull the bearer token and use it to submit to REST API 
        # access_token=ti.xcom_pull(key='access_token')
        # headers = {"Authorization": "Bearer " + access_token, 
                # "Content-type": "application/json"}
        headers = {"Content-type": "application/json"}
        # finalurl = url+instance_id+'/spark_applications'
        finalurl = url + 'mapping'
        # data=json.dumps({"application_details": 
            # {"application": "/opt/ibm/spark/examples/src/main/python/wordcount.py",
            # "arguments": ["/opt/ibm/spark/examples/src/main/resources/people.txt"]}})
        res = requests.post(finalurl,headers=headers)
        # res = requests.post(url)
        # res = requests.get(url)
        
        application_id = json.loads(res.text)
        # Push the application id - to be used on a downstream task 
        # ti.xcom_push(key='application_id', value= application_id)
        return application_id
    
    @task(task_id="ictTransfer")
    def ictTransferfunc():
        finalurl = url + 'ICTTransfer'
        res = requests.post(finalurl)
        return res.text
    
    callFirstAPI = mappingfunc()
    
    callSecondAPI = ictTransferfunc()

    callFirstAPI >> callSecondAPI
#     @task(task_id="fetchData")
#     def fetch():
#         # Schema is the database, not the actual schema.
#         # mssql = MsSqlHook(mssql_conn_id="GRADING_MSSQL_PRD", schema="Infinity")
        
#         # url = get_uri(mssql)
#         # uri = mssql.get_uri()
        
#         # engine = create_engine(uri+'?charset=utf8')
#         # df = mssql.get_pandas_df("SELECT top 5 subjectNameTh FROM grdGrade", engine)
#         # df.to_sql('testwa', engine, if_exists='append', index=False)
#         df = mssql.get_pandas_df("SELECT top 5 subjectNameTh FROM grdGrade")
        
#         # engine = create_engine(url)
#         # df = pd.read_sql("SELECT top 5 subjectNameTh FROM grdGrade", con=engine)
#         # This method (get_pandas_df) does not work with the regular mssql plugin
#         # df = mssql.get_pandas_df("SELECT top 5 subjectNameTh FROM grdGrade")
#         return df
    
#     fecth_task = fetch()

#     fecth_task
