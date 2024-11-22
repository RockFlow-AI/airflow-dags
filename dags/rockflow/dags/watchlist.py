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

RELOAD_HOT_WATCHLIST_POOL_RELATIONS = DAG(
    "RELOAD_HOT_WATCHLIST_POOL_RELATIONS",
    catchup=False,
    start_date=pendulum.datetime(2024, 11, 20, tz='Asia/Shanghai'),
    schedule_interval='00 19 * * *',
    default_args={
        "owner": "yuzhiqaing",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    }
)

SimpleHttpOperator(
    task_id='RELOAD_HOT_WATCHLIST_POOL_RELATIONS',
    method='GET',
    http_conn_id='flow-watchlist',
    endpoint='/inner/portal/watchlist/pool/relations',
    response_check=lambda response: response.json()['code'] == 200,
    extra_options={"timeout": 3600},
    dag=RELOAD_HOT_WATCHLIST_POOL_RELATIONS,
)