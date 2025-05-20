import pendulum
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import SimpleHttpOperator

# 盘前5分钟跟单任务扫描
social_task_before_market_check = DAG(
    "social_task_before_market_check",
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
    task_id='social_task_before_market_check',
    method='GET',
    http_conn_id='flow-social',
    endpoint='/social/inner/task/beforeMarketCheck',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=social_task_before_market_check,
)

# 每半小时处理过期任务
social_task_expired = DAG(
    "social_task_expired",
    catchup=False,
    start_date=pendulum.datetime(2022, 11, 4, tz='America/New_York'),
    schedule_interval='*/30 * * * *',
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

earning_yield_filter_refresh = DAG(
    "earning_yield_filter_refresh",
    catchup=False,
    start_date=pendulum.datetime(2023, 9, 20, tz='America/New_York'),
    schedule_interval='*/5 * * * *',
    default_args={
        "owner": "maoboxuan",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='earning_yield_filter_refresh',
    method='PUT',
    http_conn_id='flow-social',
    endpoint='/social/inner/earningYield/filters/cache',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=earning_yield_filter_refresh,
)

# 定时任务 - 开盘后 40 分钟检查日榜第一发送 push
push_high_day_yield = DAG(
    "push_high_day_yield",
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 7, tz='America/New_York'),
    schedule_interval='10 10 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
    }
)

SimpleHttpOperator(
    task_id='push_high_day_yield',
    method='PUT',
    http_conn_id='flow-social',
    endpoint='/social/inner/earningYield/push/high/day/yield',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=push_high_day_yield,
)

#每天美东9:35 股单市价单批量下单
copy_watchlist_task = DAG(
    "copy_watchlist_task",
    catchup=False,
    start_date=pendulum.datetime(2024, 8, 8, tz='America/New_York'),
    schedule_interval='35 09 * * *',
    default_args={
        "owner": "sunfulin",
        "depends_on_past": False,
    }
)

SimpleHttpOperator(
    task_id='copy_watchlist_task',
    method='PATCH',
    http_conn_id='flow-social',
    endpoint='/social/inner/task/opening',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=copy_watchlist_task,
)

refresh_copyTrading_profit = DAG(
    "refresh_copyTrading_profit",
    catchup=False,
    start_date=pendulum.datetime(2025, 5, 15, tz='America/New_York'),
    schedule_interval='*/10 * * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
    }
)

SimpleHttpOperator(
    task_id='refresh_copyTrading_profit',
    method='PATCH',
    http_conn_id='flow-social',
    endpoint='/social/inner/copyTrading/refresh/profit',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=refresh_copyTrading_profit,
)

social_batch_create_virtual_account = DAG(
    "social_batch_create_virtual_account",
    catchup=False,
    start_date=pendulum.datetime(2025, 5, 20, tz='America/New_York'),
    schedule_interval='*/5 * * * *',
    default_args={
        "owner": "maomao",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='social_batch_create_virtual_account',
    method='POST',
    http_conn_id='flow-social',
    endpoint='/social/inner/strategies/users',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=social_batch_create_virtual_account,
)