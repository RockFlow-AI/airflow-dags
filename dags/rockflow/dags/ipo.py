import pendulum

from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

ipo_unlisted = DAG(
    "ipo_unlisted",
    catchup=False,
    start_date=pendulum.datetime(2026, 9, 3, tz='Asia/Shanghai'),
    schedule_interval='30 8 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 3,
    }
)

SimpleHttpOperator(
    task_id='ipo_unlisted',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/ipo/unlisted',
    headers={'accept': '*/*'},
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 30},
    dag=ipo_unlisted,
)
