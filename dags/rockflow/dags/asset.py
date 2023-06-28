import pendulum
from airflow.models import DAG
from datetime import timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator

# 定时任务 - 每天北京时间六点调用一次
nvl_asset = DAG(
    "nvl_asset",
    catchup=False,
    start_date=pendulum.datetime(2023, 6, 28, tz='Asia/Shanghai'),
    schedule_interval='40 18 * * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=3),
    }
)

SimpleHttpOperator(
    task_id='nvl_asset',
    method='POST',
    http_conn_id='flow-feed-portfolio',
    endpoint='/account/inner/calculate/asset',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=nvl_asset,
)
