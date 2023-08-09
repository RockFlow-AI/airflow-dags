import pendulum
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator

# 盘前5分钟跟单任务扫描
social_task_before_market = DAG(
    "social_task_before_market",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 4, tz='America/New_York'),
    # schedule_interval='25 9-10/5 * * *',
    schedule_interval='*/5 * * * *',
    default_args={
        "owner": "hujing",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='social_task_before_market',
    method='GET',
    http_conn_id='flow-social',
    endpoint='/social/inner/task/beforeMarketCheck',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=social_task_before_market,
)

# 每半小时处理过期任务
social_task_expired = DAG(
    "social_task_expired",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 4, tz='America/New_York'),
    schedule_interval='0 0/30 * * *',
    default_args={
        "owner": "hujing",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='social_task_expired',
    method='PATCH',
    http_conn_id='flow-social',
    endpoint='/social/inner/task/expired',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=social_task_expired,
)