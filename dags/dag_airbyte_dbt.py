from airflow.decorators import dag, task
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook, DbtCloudJobRunStatus
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable
import json
from datetime import datetime
from time import sleep

AIRBYTE_CONNETCION_ID = Variable.get("AIRBYTE_GOOGLE_POSTGRES_CONNECTION_ID")
API_KEY = f'Bearer {Variable.get("AIRBYTE_API_TOKEN")}'

DBT_CLOUD_CONN_ID = "dbt-conn"
JOB_ID = "70403103919624"

@dag(
    start_date=datetime(2024, 4, 18),
    schedule="@daily",
    catchup=False,
)
def running_airbyte_dbt():

    start_airbyte_sync = SimpleHttpOperator(
        task_id='start_airbyte_sync',
        http_conn_id='airbyte',
        endpoint=f'/v1/jobs',  # api/v1/connections/sync Endpoint correto para disparar a sincronização
        method='POST',
        headers={"Content-Type": "application/json", 
                 "User-Agent":"fake-useragent", 
                 "Accept":"application/json",
                 "Authorization": API_KEY},
        data=json.dumps({"connectionId": AIRBYTE_CONNETCION_ID, "jobType":"sync"}),  # Assegure que o connectionId está correto
        response_check=lambda response: response.json()['status'] == 'running'
    )

    @task
    def operador_http_sensor():
        sleep(180)

    rodar_dbt = DbtCloudRunJobOperator(
        task_id = "rodar_dbt",
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id=JOB_ID,
        check_interval=60,
        timeout=360,
    )

    t1 = operador_http_sensor()

    start_airbyte_sync >> t1 >> rodar_dbt

running_airbyte_dbt()