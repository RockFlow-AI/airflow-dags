import time
from datetime import datetime, timedelta

import pendulum
from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

check_iqiyi_award_status = DAG(
    "check_iqiyi_award_status",
    catchup=False,
    start_date=pendulum.datetime(2023, 4, 20, tz='UTC'),
    schedule_interval='0 10 * * 1-7',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='check_iqiyi_award_status',
    method='POST',
    http_conn_id='flow-promotion',
    endpoint='/promotion/inner/task/activity/iqiyi/issueStatus',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=check_iqiyi_award_status,
)

# 定时灌水，六小时一次
auto_add_landing_page_count = DAG(
    "auto_add_landing_page_count",
    catchup=False,
    start_date=pendulum.datetime(2023, 11, 20, tz='Asia/Shanghai'),
    schedule_interval='0 */6 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    }
)

SimpleHttpOperator(
    task_id='auto_add_landing_page_count',
    method='PUT',
    http_conn_id='flow-promotion',
    endpoint='/promotion/inner/fund/qualification/auto/add',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=auto_add_landing_page_count,
)

promotion_reward_claim = DAG(
    "promotion_reward_claim",
    catchup=False,
    start_date=pendulum.datetime(2024, 2, 4, tz='Asia/Shanghai'),
    schedule_interval='0 * * * *',
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    }
)

SimpleHttpOperator(
    task_id='promotion_reward_claim',
    method='POST',
    http_conn_id='flow-promotion',
    endpoint='/promotion/inner/rewards',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=promotion_reward_claim,
)
