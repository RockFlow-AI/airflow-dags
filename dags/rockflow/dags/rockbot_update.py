from datetime import datetime, timedelta
from airflow import DAG
import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator


rockbot_update_10m = DAG(
    "rockbot_update_10m",
    catchup=False,
    start_date=pendulum.datetime(2023, 9, 1),
    schedule_interval='*/30 * * * *',
    default_args={
        "owner": "caohaoxuan",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='rockbot_update_10m',
    method='POST',
    http_conn_id='rockbot',
    endpoint='/bot/api/ideas/update_10m',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=rockbot_update_10m,
)