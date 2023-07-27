import pendulum
from airflow.models import DAG
from datetime import timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator

# 定时任务 - 每天北京时间六点调用一次
asset = DAG(
    "asset",
    catchup=False,
    start_date=pendulum.datetime(2023, 7, 12, tz='Asia/Shanghai'),
    schedule_interval='0 18 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='asset',
    method='POST',
    http_conn_id='flow-feed-portfolio',
    endpoint='/account/inner/calculate/asset',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=asset,
)