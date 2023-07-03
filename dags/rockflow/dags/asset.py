import pendulum
from airflow.models import DAG
from datetime import timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator

# 定时任务 - 每天北京时间六点调用一次
asset = DAG(
    "asset",
    catchup=False,
    start_date=pendulum.datetime(2023, 6, 28, tz='Asia/Shanghai'),
    schedule_interval='0 18 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='asset',
    method='POST',
    http_conn_id='flow-feed-portfolio',
    endpoint='/account/inner/calculate/asset',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=asset,
)
