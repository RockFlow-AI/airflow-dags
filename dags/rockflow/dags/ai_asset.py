import pendulum
from airflow.models import DAG
from datetime import timedelta, datetime
from airflow.providers.http.operators.http import SimpleHttpOperator

# 定时任务 - 每天北京时间六点调用一次
ai_asset_refresh = DAG(
    "ai_asset_refresh",
    catchup=False,
    start_date=pendulum.datetime(2023, 6, 29, tz='Asia/Shanghai'),
    schedule_interval='0/10 * * * *',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='ai_asset_refresh',
    method='POST',
    http_conn_id='flow-social',
    endpoint='/social/inner/arenas/participant/assets',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=ai_asset_refresh,
)
