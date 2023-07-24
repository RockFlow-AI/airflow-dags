import pendulum
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator

# 盘前5分钟跟单任务扫描
social_task_before_market_check = DAG(
    "social_task_before_market_check",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 4, tz='America/New_York'),
    schedule_interval='25 21-22/5 * * *',
    default_args={
        "owner": "hujing",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='social_task_before_market_check',
    method='PATCH',
    http_conn_id='flow-social',
    endpoint='/social/inner/task/beforeMarketCheck',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=social_task_before_market_check,
)

