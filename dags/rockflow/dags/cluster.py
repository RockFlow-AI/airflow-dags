import time
from datetime import datetime, timedelta

import pendulum
from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

# 汇率更新
currencies_refresh = DAG(
    "currencies_refresh_by_hour",
    catchup=False,
    start_date=pendulum.datetime(2022, 3, 22, tz='America/New_York'),
    schedule_interval='30 0 * * *',
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='currencies_refresh',
    method='POST',
    http_conn_id='flow-portfolio-service',
    endpoint='/portfolio/inner/currencies/refresh',
    headers={'appId': '1'},
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=currencies_refresh,
)

# 节假日更新
holiday_refresh = DAG(
    "holiday_refresh_yearly",
    catchup=False,
    start_date=pendulum.datetime(2024, 12, 29, tz='America/New_York'),
    schedule_interval='0 0 29 12 *',
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='holiday_refresh',
    method='POST',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/holidays/refresh',
    headers={'appId': '1'},
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=holiday_refresh,
)

# 碎股及港股更新
contracts_refresh = DAG(
    "contracts_refresh_daily",
    catchup=False,
    start_date=datetime(2022, 1, 5, 0, 0),
    schedule_interval=timedelta(days=1),
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='contracts_refresh',
    method='POST',
    http_conn_id='flow-portfolio-service',
    endpoint='/portfolio/inner/contracts/refresh',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60 * 3},
    dag=contracts_refresh,
)

clean_expiry_option_osus = DAG(
    "clean_expiry_option_osus",
    catchup=False,
    start_date=pendulum.datetime(2022, 2, 28, tz='America/New_York'),
    schedule_interval='0 0 * * 2-6',
    default_args={
        "owner": "jiemin",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='options',
    method='PUT',
    http_conn_id='flow-ticker-service',
    endpoint='/ticker/inner/ticks/options/expiry',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=clean_expiry_option_osus,
)

# 日结单
daily_statement = DAG(
    "daily_statement",
    catchup=False,
    start_date=pendulum.datetime(2022, 3, 10, tz='Asia/Hong_Kong'),
    schedule_interval='0 16 * * *',
    default_args={
        "owner": "maoboxuan",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='daily_statement',
    method='GET',
    http_conn_id='flow-statement-service',
    endpoint='/inner/statements/daily',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=daily_statement,
)

daily_open_account_sync = DAG(
    "daily_open_account_sync",
    catchup=False,
    start_date=pendulum.datetime(2022, 3, 16, tz='America/New_York'),
    schedule_interval='15 4 * * SUN-FRI',
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='daily_open_account_sync',
    method='POST',
    http_conn_id='flow-portfolio-service',
    endpoint='/portfolio/inner/accounts/refresh?status=1&limit=20&firstDepositReceived=true',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=daily_open_account_sync,
)

daily_pending_account_sync = DAG(
    "daily_pending_account_sync",
    catchup=False,
    start_date=pendulum.datetime(2022, 3, 16, tz='America/New_York'),
    schedule_interval='30 4 * * SUN-FRI',
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='daily_pending_account_sync',
    method='POST',
    http_conn_id='flow-portfolio-service',
    endpoint='/portfolio/inner/accounts/refresh?status=1&limit=20&firstDepositReceived=false',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=daily_pending_account_sync,
)

weekly_pending_order_sync = DAG(
    "weekly_pending_order_sync",
    catchup=False,
    start_date=pendulum.datetime(2022, 4, 11, tz='America/New_York'),
    schedule_interval='00 08 * * SUN',
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='weekly_pending_order_sync',
    method='PATCH',
    http_conn_id='flow-portfolio-service',
    endpoint='/portfolio/inner/orders',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=weekly_pending_order_sync,
)

# 月结单
monthly_statement = DAG(
    "monthly_statement",
    catchup=False,
    start_date=pendulum.datetime(2022, 4, 13, tz='Asia/Hong_Kong'),
    schedule_interval='0 16 2 * *',
    default_args={
        "owner": "maoboxuan",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='monthly_statement',
    method='GET',
    http_conn_id='flow-statement-service',
    endpoint='/inner/statements/monthly',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 600},
    dag=monthly_statement,
)

option_follow_task = DAG(
    "option_follow_task",
    catchup=False,
    start_date=pendulum.datetime(2023, 4, 20, tz='America/New_York'),
    schedule_interval='30/10 9 * * 1-5',
    default_args={
        "owner": "chengwei",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='option_follow_task',
    method='POST',
    http_conn_id='flow-portfolio-service',
    endpoint='/portfolio/inner/follow/task',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=option_follow_task,
)

top_movers = DAG(
    "top_movers",
    catchup=False,
    start_date=pendulum.datetime(2025, 1, 26, tz='America/New_York'),
    schedule_interval='10-50/10 4-20 * * 1-5',
    default_args={
        "owner": "yinxiang",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='top_movers',
    method='PATCH',
    http_conn_id='flow-feed-portfolio',
    endpoint='/alert/inner/topMovers',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=top_movers,
)
