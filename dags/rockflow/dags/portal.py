import time
from datetime import datetime, timedelta

import pendulum
from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

refresh_etf_symbol = DAG(
    "refresh_etf_symbol",
    catchup=False,
    start_date=pendulum.datetime(2024, 8, 8, tz='Asia/Shanghai'),
    schedule_interval='0 12 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
    }
)

SimpleHttpOperator(
    task_id='refresh_etf_symbol',
    method='PATCH',
    http_conn_id='flow-portal',
    endpoint='/inner/portal/symbol/refresh/etf/symbols',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=refresh_etf_symbol,
)
