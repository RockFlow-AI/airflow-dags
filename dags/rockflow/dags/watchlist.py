import pendulum
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator


WATCHLIST_US_DAY = DAG(
    "WATCHLIST_US_DAY",
    catchup=False,
    start_date=pendulum.datetime(2022, 9, 8, tz='America/New_York'),
    schedule_interval='0 17 * * 1-5',
    default_args={
        "owner": "chenborui",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='WATCHLIST_US_DAY',
    method='PUT',
    http_conn_id='flow-mr-option',
    endpoint='/watchlist/inner/tasks',
    data={"period": "DAY"},
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=WATCHLIST_US_DAY,
)

WATCHLIST_US_MINUTE = DAG(
    "WATCHLIST_US_MINUTE",
    catchup=False,
    start_date=pendulum.datetime(2022, 9, 8, tz='America/New_York'),
    schedule_interval='*/10 9-17 * * 1-5',
    default_args={
        "owner": "chenborui",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='WATCHLIST_US_MINUTE',
    method='PUT',
    http_conn_id='flow-mr-option',
    endpoint='/watchlist/inner/tasks',
    data={"period": "MINUTE"},
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=WATCHLIST_US_MINUTE,
)

# 定时更新精选股单
UPSERT_ES_WATCHLIST = DAG(
    "UPSERT_ES_WATCHLIST",
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 12, tz='Asia/Shanghai'),
    schedule_interval='0 12 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='UPSERT_ES_WATCHLIST',
    method='PUT',
    http_conn_id='flow-watchlist',
    endpoint='/watchlist/inner/sync/es/watchlist',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=UPSERT_ES_WATCHLIST,
)

# 重载热门股单
RELOAD_HOT_WATCHLIST = DAG(
    "RELOAD_HOT_WATCHLIST",
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 12, tz='Asia/Shanghai'),
    schedule_interval='0 10 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='RELOAD_HOT_WATCHLIST',
    method='PUT',
    http_conn_id='flow-watchlist',
    endpoint='/watchlist/inner/reload/hot/watchlist',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 60},
    dag=RELOAD_HOT_WATCHLIST,
)


FLUSH_HOT_WATCHLIST = DAG(
    "FLUSH_HOT_WATCHLIST",
    catchup=False,
    start_date=pendulum.datetime(2024, 7, 23, tz='Asia/Shanghai'),
    schedule_interval='00 18 * * *',
    default_args={
        "owner": "sunfulin",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='FLUSH_HOT_WATCHLIST',
    method='GET',
    http_conn_id='flow-watchlist',
    endpoint='/inner/watchlist/popular/cache',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=FLUSH_HOT_WATCHLIST,
)

RELOAD_WATCHLIST_SYMBOL_CHART_CACHE_1DAY = DAG(
    "RELOAD_WATCHLIST_SYMBOL_CHART_CACHE_1DAY",
    catchup=False,
    start_date=pendulum.datetime(2025, 7, 15, tz='Asia/Shanghai'),
    schedule_interval='*/5 * * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='RELOAD_WATCHLIST_SYMBOL_CHART_CACHE_1DAY',
    method='POST',
    http_conn_id='flow-watchlist',
    endpoint='inner/watchlist/reload/symbols/chart/cache/1day',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=RELOAD_WATCHLIST_SYMBOL_CHART_CACHE_1DAY,
)

RELOAD_WATCHLIST_SYMBOL_CHART_CACHE_1WEEK = DAG(
    "RELOAD_WATCHLIST_SYMBOL_CHART_CACHE_1WEEK",
    catchup=False,
    start_date=pendulum.datetime(2025, 7, 15, tz='Asia/Shanghai'),
    schedule_interval='*/17 * * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='RELOAD_WATCHLIST_SYMBOL_CHART_CACHE_1WEEK',
    method='POST',
    http_conn_id='flow-watchlist',
    endpoint='inner/watchlist/reload/symbols/chart/cache/1week',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=RELOAD_WATCHLIST_SYMBOL_CHART_CACHE_1WEEK,
)

RELOAD_WATCHLIST_SYMBOL_CHART_CACHE_1MONTH = DAG(
    "RELOAD_WATCHLIST_SYMBOL_CHART_CACHE_1MONTH",
    catchup=False,
    start_date=pendulum.datetime(2025, 7, 15, tz='Asia/Shanghai'),
    schedule_interval='0 0 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='RELOAD_WATCHLIST_SYMBOL_CHART_CACHE_1MONTH',
    method='POST',
    http_conn_id='flow-watchlist',
    endpoint='inner/watchlist/reload/symbols/chart/cache/1month',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=RELOAD_WATCHLIST_SYMBOL_CHART_CACHE_1MONTH,
)

RELOAD_WATCHLIST_SYMBOL_CHART_CACHE_1YEAR = DAG(
    "RELOAD_WATCHLIST_SYMBOL_CHART_CACHE_1YEAR",
    catchup=False,
    start_date=pendulum.datetime(2025, 7, 15, tz='Asia/Shanghai'),
    schedule_interval='0 1 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='RELOAD_WATCHLIST_SYMBOL_CHART_CACHE_1YEAR',
    method='POST',
    http_conn_id='flow-watchlist',
    endpoint='inner/watchlist/reload/symbols/chart/cache/1year',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=RELOAD_WATCHLIST_SYMBOL_CHART_CACHE_1YEAR,
)

RELOAD_WATCHLIST_SYMBOL_CHART_CACHE_5YEAR = DAG(
    "RELOAD_WATCHLIST_SYMBOL_CHART_CACHE_5YEAR",
    catchup=False,
    start_date=pendulum.datetime(2025, 7, 15, tz='Asia/Shanghai'),
    schedule_interval='0 2 * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
    }
)

SimpleHttpOperator(
    task_id='RELOAD_WATCHLIST_SYMBOL_CHART_CACHE_5YEAR',
    method='POST',
    http_conn_id='flow-watchlist',
    endpoint='inner/watchlist/reload/symbols/chart/cache/5year',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=RELOAD_WATCHLIST_SYMBOL_CHART_CACHE_5YEAR,
)

RELOAD_WATCHLIST_SYMBOL_FEED_CACHE = DAG(
    "RELOAD_WATCHLIST_SYMBOL_FEED_CACHE",
    catchup=False,
    start_date=pendulum.datetime(2025, 7, 15, tz='Asia/Shanghai'),
    schedule_interval='*/2 * * * *',
    default_args={
        "owner": "yuzhiqiang",
        "depends_on_past": False,
        "retries": 0,
    }
)

SimpleHttpOperator(
    task_id='RELOAD_WATCHLIST_SYMBOL_FEED_CACHE',
    method='POST',
    http_conn_id='flow-watchlist',
    endpoint='inner/watchlist/reload/symbols/feed/cache',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=RELOAD_WATCHLIST_SYMBOL_FEED_CACHE,
)